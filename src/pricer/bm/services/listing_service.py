# src/pricer/bm/services/listing_service.py
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from pricer.bm.requester.client import Requester
from pricer.utils.logging import log_json
from pricer.core.exceptions import BackMarketAPIError
from pricer.bm.endpoints.listings_get_all import snapshot_index_by_id


def _price_str(child: Dict[str, Any]) -> str:
    v = child.get("max_price")
    if isinstance(v, (int, float)):
        return f"{v:.2f}"
    if isinstance(v, str) and v.strip():
        return v.strip()
    return "2000.00"


def _payload_for_update(child: Dict[str, Any], *, activate: bool) -> Dict[str, Any]:
    return {
        "min_price": None,
        "price": _price_str(child),
        "currency": "GBP",
        "quantity": 1 if activate else 0,
    }


@dataclass
class UpdateResult:
    ok: bool
    listing_id: str
    requested_quantity: int
    verified_quantity: Optional[int]
    error: Optional[str] = None


@dataclass
class BulkSummary:
    total: int
    ok: int
    suspect: int
    failed: int


@dataclass
class BulkResult:
    summary: BulkSummary
    results: List[UpdateResult]


@dataclass
class CycleReport:
    baseline_count: int
    activated_count: int
    deactivated_count: int
    reconcile_ok: bool
    anomalies: Dict[str, Any]
    durations_ms: Dict[str, int]
    details: Dict[str, Any]


class ListingService:
    def __init__(self, requester: Requester) -> None:
        self.req = requester

    # ---------------- low-level update ----------------

    async def _update_one(self, listing_id: str, child: Dict[str, Any], *, activate: bool) -> UpdateResult:
        payload = _payload_for_update(child, activate=activate)
        requested_qty = payload["quantity"]

        log_json(
            "listing_update_attempt",
            listing_id=listing_id,
            action=("activate" if activate else "deactivate"),
            payload={"price": payload["price"], "quantity": requested_qty, "currency": payload["currency"]},
        )

        try:
            await self.req.send(
                "POST",
                f"/ws/listings/{listing_id}",
                json_body=payload,
                endpoint_tag="listing_update",
                category="seller_mutations",
                timeout_ms=None,
                idempotency_key=None,
            )
            log_json(
                "listing_update_ok",
                listing_id=listing_id,
                action=("activate" if activate else "deactivate"),
                payload={"price": payload["price"], "quantity": requested_qty},
            )
            return UpdateResult(ok=True, listing_id=listing_id, requested_quantity=requested_qty, verified_quantity=None)
        except Exception as exc:
            log_json(
                "listing_update_failed",
                listing_id=listing_id,
                action=("activate" if activate else "deactivate"),
                error=str(exc),
            )
            return UpdateResult(ok=False, listing_id=listing_id, requested_quantity=requested_qty, verified_quantity=None, error=str(exc))

    async def bulk_update(self, items: Sequence[Dict[str, Any]], *, activate_flag: bool, workers: int = 24) -> BulkResult:
        sem = asyncio.Semaphore(max(1, workers))
        results: List[UpdateResult] = []

        async def one(it: Dict[str, Any]):
            lid = str(it["listing_id"])
            child = dict(it.get("child") or {})
            async with sem:
                res = await self._update_one(lid, child, activate=activate_flag)
                results.append(res)

        tasks = [asyncio.create_task(one(it)) for it in items]
        await asyncio.gather(*tasks)

        ok = sum(1 for r in results if r.ok)
        failed = sum(1 for r in results if not r.ok)
        summary = BulkSummary(total=len(results), ok=ok, suspect=0, failed=failed)
        return BulkResult(summary=summary, results=results)

    async def _bulk_update_chunked(
        self,
        items: Sequence[Dict[str, Any]],
        *,
        activate_flag: bool,
        workers: int,
        chunk_size: int = 1000,
        phase: str = "activate",
    ) -> BulkResult:
        """
        Run updates in manageable chunks to avoid spawning too many tasks at once.
        """
        total_results: List[UpdateResult] = []
        totals = {"total": 0, "ok": 0, "failed": 0}

        for i in range(0, len(items), chunk_size):
            chunk = items[i : i + chunk_size]
            log_json(f"{phase}_chunk_start", idx=i // chunk_size + 1, size=len(chunk))
            bulk = await self.bulk_update(chunk, activate_flag=activate_flag, workers=workers)
            total_results.extend(bulk.results)
            totals["total"] += bulk.summary.total
            totals["ok"] += bulk.summary.ok
            totals["failed"] += bulk.summary.failed
            log_json(f"{phase}_chunk_done", idx=i // chunk_size + 1, size=len(chunk), ok=bulk.summary.ok, failed=bulk.summary.failed)

        summary = BulkSummary(total=totals["total"], ok=totals["ok"], suspect=0, failed=totals["failed"])
        return BulkResult(summary=summary, results=total_results)

    # ---------------- cycle orchestration ----------------

    async def full_cycle(
        self,
        items: Sequence[Dict[str, Any]],
        *,
        workers: int = 24,
        abort_on_first_failure: bool = False,
    ) -> CycleReport:
        t0 = time.perf_counter()

        # 1) Baseline scan (fast, paginated)
        log_json("cycle_baseline_start")
        baseline_idx = await snapshot_index_by_id(self.req, page_size=100)
        t1 = time.perf_counter()
        log_json("cycle_baseline_done", count=len(baseline_idx), elapsed_ms=int((t1 - t0) * 1000))

        # Determine which to activate: those with quantity==0 or missing in baseline
        to_activate: List[Dict[str, Any]] = []
        for it in items:
            lid = str(it["listing_id"])
            snap = baseline_idx.get(lid)
            qty = snap["quantity"] if snap else 0
            if qty <= 0:
                to_activate.append(it)

        log_json("cycle_activate_start", candidates=len(items), to_activate=len(to_activate), workers=workers)

        # 2) Activate (subset), chunked
        act_bulk = BulkResult(summary=BulkSummary(total=0, ok=0, suspect=0, failed=0), results=[])
        if to_activate:
            act_bulk = await self._bulk_update_chunked(
                to_activate, activate_flag=True, workers=workers, chunk_size=1000, phase="activate"
            )
            if abort_on_first_failure and act_bulk.summary.failed > 0:
                raise BackMarketAPIError(f"Activation failures: {act_bulk.summary.failed}")
        t2 = time.perf_counter()
        log_json("cycle_activate_done", activated=len(to_activate), elapsed_ms=int((t2 - t1) * 1000))

        # 2.5) Explicit settle wait so BM catches up (configurable; default 120s)
        settle_seconds = int(getattr(self.req.settings, "cycle_settle_seconds", 120))
        if settle_seconds > 0:
            log_json("cycle_settle_wait", seconds=settle_seconds)
            await asyncio.sleep(settle_seconds)

        # 3) Pricing placeholder
        log_json("pricing_phase_start", note="placeholder_noop")
        # TODO: integrate pricing/services when ready
        log_json("pricing_phase_done", priced_count=len(to_activate))

        # 4) Deactivate the ones we just activated (chunked)
        log_json("cycle_deactivate_start", count=len(to_activate), workers=workers)
        deact_bulk = await self._bulk_update_chunked(
            to_activate, activate_flag=False, workers=workers, chunk_size=1000, phase="deactivate"
        )
        t3 = time.perf_counter()
        log_json("cycle_deactivate_done", deactivated=len(to_activate), elapsed_ms=int((t3 - t2) * 1000))

        # 5) Rescan (fast, paginated) and reconcile
        log_json("cycle_rescan_start")
        final_idx = await snapshot_index_by_id(self.req, page_size=100)
        t4 = time.perf_counter()
        log_json("cycle_rescan_done", count=len(final_idx), elapsed_ms=int((t4 - t3) * 1000))

        initial_active_ids = {lid for (lid, snap) in baseline_idx.items() if (snap.get("quantity") or 0) > 0}
        final_active_ids = {lid for (lid, snap) in final_idx.items() if (snap.get("quantity") or 0) > 0}
        activated_ids = {str(it["listing_id"]) for it in to_activate}

        anomalies: Dict[str, Any] = {}
        still_active = sorted(list(activated_ids & final_active_ids))
        if still_active:
            anomalies["activated_still_active"] = still_active
        regressed = sorted(list(initial_active_ids - final_active_ids))
        if regressed:
            anomalies["initial_active_became_inactive"] = regressed

        details = {
            "activated_ids": sorted(list(activated_ids)),
            "initial_active_count": len(initial_active_ids),
            "final_active_count": len(final_active_ids),
        }
        reconcile_ok = not anomalies

        t5 = time.perf_counter()
        durations_ms = {
            "baseline_ms": int((t1 - t0) * 1000),
            "activate_ms": int((t2 - t1) * 1000),
            "settle_ms": settle_seconds * 1000,
            "deactivate_ms": int((t3 - t2) * 1000),
            "rescan_ms": int((t4 - t3) * 1000),
            "total_ms": int((t5 - t0) * 1000),
        }

        log_json(
            "cycle_complete",
            baseline_count=len(baseline_idx),
            activated_count=len(to_activate),
            deactivated_count=len(to_activate),
            reconcile_ok=reconcile_ok,
            anomalies=anomalies,
            durations_ms=durations_ms,
        )

        return CycleReport(
            baseline_count=len(baseline_idx),
            activated_count=len(to_activate),
            deactivated_count=len(to_activate),
            reconcile_ok=reconcile_ok,
            anomalies=anomalies,
            durations_ms=durations_ms,
            details=details,
        )

    # Convenience wrappers to keep /activate /deactivate endpoints working
    async def activate(self, listing_id: str, child: Dict[str, Any]) -> UpdateResult:
        return await self._update_one(listing_id, child, activate=True)

    async def deactivate(self, listing_id: str, child: Dict[str, Any]) -> UpdateResult:
        return await self._update_one(listing_id, child, activate=False)



