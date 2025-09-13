# src/pricer/web/routers/activation.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from pricer.bm.services.listing_service import ListingService
from pricer.core.settings import Settings
from pricer.utils.logging import log_json
from pricer.web.deps import get_requester, get_settings, get_db
from pricer.bm.requester.client import Requester

router = APIRouter(prefix="/bm/listings", tags=["activation"])


# ---------- Schemas kept for manual tooling ----------

class ItemInput(BaseModel):
    listing_id: str = Field(..., description="BackMarket listing ID (UUID)")
    child: Dict[str, Any] = Field(default_factory=dict, description="Child listing doc with at least max_price")


class BulkInput(BaseModel):
    items: List[ItemInput]
    workers: int = Field(24, ge=1, le=256)


class CycleManualInput(BaseModel):
    items: List[ItemInput] = Field(..., description="Explicit list of items to run through full_cycle")
    workers: int = Field(24, ge=1, le=256)
    abort_on_first_failure: bool = Field(False, description="Abort activation phase on the first failed/suspect update")


# ---------- Helpers (DB extraction, streaming) ----------

def _top_or_source(d: Dict[str, Any], key: str) -> Any:
    if key in d:
        return d[key]
    src = d.get("source")
    if isinstance(src, dict):
        return src.get(key)
    return None


async def _extract_children_from_groups_stream(
    request: Request,
    settings: Settings,
    *,
    limit: Optional[int],                # None or <=0 => unlimited
    require_max_price: bool,
    only_inactive_hint: bool,
    dedupe_by_listing_id: bool,
) -> List[Dict[str, Any]]:
    """
    Streams from {bm_tradein_groups} and returns a flat list:
      [{'listing_id': <uuid>, 'child': {...with max_price...}}, ...]

    We prefer TOP-LEVEL child fields and only fall back to child.source if missing.
    """
    db = get_db(request, settings)
    groups_coll_name = getattr(settings, "mongo_coll_tradein_groups", None) or "bm_tradein_groups"
    coll = db.get_collection(groups_coll_name)

    pipeline = [
        {"$project": {"key": 1, "children": 1}},
        {"$unwind": "$children"},
        {"$project": {"_id": 0, "child": "$children"}},
    ]

    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    stats = {
        "scanned": 0, "produced": 0, "kept": 0,
        "skipped_no_id": 0, "skipped_no_price": 0,
        "skipped_active_hint": 0, "skipped_dupe": 0
    }

    cursor = coll.aggregate(pipeline, allowDiskUse=True)
    async for d in cursor:
        stats["scanned"] += 1
        child = d.get("child") or {}

        listing_id = _top_or_source(child, "id")
        if not isinstance(listing_id, (str, int)):
            stats["skipped_no_id"] += 1
            continue
        listing_id = str(listing_id)

        if only_inactive_hint:
            q = child.get("quantity")
            if q is None:
                q = _top_or_source(child, "quantity")
            if isinstance(q, int) and q > 0:
                stats["skipped_active_hint"] += 1
                continue

        if dedupe_by_listing_id and listing_id in seen:
            stats["skipped_dupe"] += 1
            continue

        max_price = child.get("max_price")
        if max_price in (None, ""):
            max_price = _top_or_source(child, "max_price")

        if require_max_price and (max_price in (None, "")):
            stats["skipped_no_price"] += 1
            continue

        norm_child = dict(child)
        if max_price not in (None, ""):
            norm_child["max_price"] = max_price

        out.append({"listing_id": listing_id, "child": norm_child})
        seen.add(listing_id)
        stats["kept"] += 1

        if isinstance(limit, int) and limit > 0 and len(out) >= limit:
            break

    stats["produced"] = len(out)
    log_json(
        "cycle_db_extraction_stats",
        **stats,
        limit=(limit if limit and limit > 0 else "ALL"),
        require_max_price=require_max_price,
        only_inactive_hint=only_inactive_hint,
        dedupe_by_listing_id=dedupe_by_listing_id,
    )
    return out


# ---------- Manual/utility endpoints ----------

@router.post("/activate")
async def activate_listing(
    payload: ItemInput,
    req: Requester = Depends(get_requester),
):
    svc = ListingService(req)
    res = await svc.activate(payload.listing_id, payload.child)
    return {
        "ok": res.ok,
        "listing_id": res.listing_id,
        "requested_quantity": res.requested_quantity,
        "verified_quantity": res.verified_quantity,
        "error": res.error,
    }


@router.post("/deactivate")
async def deactivate_listing(
    payload: ItemInput,
    req: Requester = Depends(get_requester),
):
    svc = ListingService(req)
    res = await svc.deactivate(payload.listing_id, payload.child)
    return {
        "ok": res.ok,
        "listing_id": res.listing_id,
        "requested_quantity": res.requested_quantity,
        "verified_quantity": res.verified_quantity,
        "error": res.error,
    }


@router.post("/cycle")
async def cycle_bulk(
    payload: BulkInput,
    req: Requester = Depends(get_requester),
):
    svc = ListingService(req)
    items = [i.model_dump() for i in payload.items]
    bulk = await svc.bulk_update(items, activate_flag=True, workers=payload.workers)
    return {
        "summary": {
            "total": bulk.summary.total,
            "ok": bulk.summary.ok,
            "suspect": bulk.summary.suspect,
            "failed": bulk.summary.failed,
        },
        "results": [r.__dict__ for r in bulk.results],
    }


@router.post("/full_cycle_manual")
async def full_cycle_manual(
    payload: CycleManualInput,
    req: Requester = Depends(get_requester),
):
    svc = ListingService(req)
    items = [i.model_dump() for i in payload.items]
    log_json("cycle_started_manual", count=len(items))
    report = await svc.full_cycle(items, workers=payload.workers, abort_on_first_failure=payload.abort_on_first_failure)
    return {
        "baseline_count": report.baseline_count,
        "activated_count": report.activated_count,
        "deactivated_count": report.deactivated_count,
        "reconcile_ok": report.reconcile_ok,
        "anomalies": report.anomalies,
        "durations_ms": report.durations_ms,
        "details": report.details,
    }


# ---------- DB-driven endpoint (NO BODY) ----------

@router.post("/full_cycle")
async def full_cycle_from_db(
    request: Request,
    settings: Settings = Depends(get_settings),
    req: Requester = Depends(get_requester),
):
    """
    DB-driven full cycle (NO BODY):
      - Pull ALL children from {bm_tradein_groups} (deduped by listing_id)
      - Baseline scan remote listings (paginated)
      - Activate all that are inactive
      - Wait 'cycle_settle_seconds' to let BM catch up
      - [Pricing placeholder]
      - Deactivate the ones we activated
      - Rescan & reconcile
    """
    # Defaults from Settings (env) with safe fallbacks
    # cycle_limit <= 0 => process ALL
    limit = getattr(settings, "cycle_limit", 0)
    if isinstance(limit, int) and limit <= 0:
        limit = None  # unlimited

    workers = int(getattr(settings, "cycle_workers", 24))
    require_max_price = bool(getattr(settings, "cycle_require_max_price", True))
    only_inactive_hint = bool(getattr(settings, "cycle_only_inactive_hint", True))
    dedupe_by_listing_id = bool(getattr(settings, "cycle_dedupe_by_listing_id", True))
    abort_on_first_failure = bool(getattr(settings, "cycle_abort_on_first_failure", False))

    # Extract ALL (or limited) items from DB
    items = await _extract_children_from_groups_stream(
        request,
        settings,
        limit=limit,
        require_max_price=require_max_price,
        only_inactive_hint=only_inactive_hint,
        dedupe_by_listing_id=dedupe_by_listing_id,
    )
    if not items:
        raise HTTPException(status_code=400, detail="No candidate children found from trade-in groups.")

    log_json(
        "cycle_started_db",
        count=len(items),
        limit=(limit if limit else "ALL"),
        require_max_price=require_max_price,
        only_inactive_hint=only_inactive_hint,
        dedupe_by_listing_id=dedupe_by_listing_id,
        workers=workers,
    )

    svc = ListingService(req)
    report = await svc.full_cycle(items, workers=workers, abort_on_first_failure=abort_on_first_failure)

    return {
        "source": "db",
        "baseline_count": report.baseline_count,
        "activated_count": report.activated_count,
        "deactivated_count": report.deactivated_count,
        "reconcile_ok": report.reconcile_ok,
        "anomalies": report.anomalies,
        "durations_ms": report.durations_ms,
        "details": report.details,
        "defaults_used": {
            "limit": (limit if limit else "ALL"),
            "workers": workers,
            "require_max_price": require_max_price,
            "only_inactive_hint": only_inactive_hint,
            "dedupe_by_listing_id": dedupe_by_listing_id,
            "abort_on_first_failure": abort_on_first_failure,
        },
    }













