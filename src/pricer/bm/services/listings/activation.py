# src/pricer/bm/services/listings/activation.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from pricer.bm.requester.client import Requester
from pricer.utils.logging import log_json


def build_activation_payload(child: dict[str, Any], activate: bool) -> dict[str, Any]:
    """
    Always conform to BM payload contract.
    Price comes from child.max_price (stringified) or "2000.00" fallback.
    Activation == quantity 1; Deactivation == quantity 0.
    """
    price = str(child.get("max_price") or "2000.00")
    return {
        "min_price": None,
        "price": price,
        "currency": "GBP",
        "quantity": 1 if activate else 0,
    }


async def _fetch_listing(requester: Requester, listing_id: str) -> Optional[dict[str, Any]]:
    """
    Best-effort verification fetch. If BM doesn't expose GET for this resource
    in your account/region, we'll return None and treat verification as best-effort.
    """
    try:
        data = await requester.send(
            "GET",
            f"/ws/listings/{listing_id}",
            endpoint_tag="listing_get",
            category="seller_generic",
            retry_404=True,
        )
        return data if isinstance(data, dict) else None
    except Exception:
        return None


@dataclass
class ActivationResult:
    listing_id: str
    requested_quantity: int
    verified_quantity: Optional[int]
    ok: bool
    error: Optional[str] = None


async def update_listing_quantity(
    requester: Requester,
    *,
    listing_id: str,
    child: dict[str, Any],
    activate: bool,
) -> ActivationResult:
    """
    Idempotent POST update with best-effort verification read.
    Emits listing_update_ok / listing_update_suspect / listing_update_failed.
    """
    payload = build_activation_payload(child, activate)
    desired_q = payload["quantity"]

    # Idempotency: tie to run, listing, desired quantity and price so repeats are harmless
    idem = f"{requester.run_id}:{listing_id}:{desired_q}:{payload['price']}"
    error: Optional[str] = None
    ok = False
    verified_q: Optional[int] = None

    try:
        await requester.send(
            "POST",
            f"/ws/listings/{listing_id}",
            json_body=payload,
            endpoint_tag="listing_update",
            category="seller_mutations",
            idempotency_key=idem,
            retry_404=True,  # BM edge consistency
        )
        # Verify (best-effort): read back and compare quantity
        doc = await _fetch_listing(requester, listing_id)
        if doc:
            if isinstance(doc.get("quantity"), int):
                verified_q = doc["quantity"]
            else:
                quantities = doc.get("quantities") or {}
                v = quantities.get("available")
                verified_q = int(v) if isinstance(v, int) else None

        ok = (verified_q is None) or (verified_q == desired_q)

        log_json(
            "listing_update_ok" if ok else "listing_update_suspect",
            listing_id=listing_id,
            desired_quantity=desired_q,
            verified_quantity=verified_q,
            payload={"price": payload["price"], "currency": "GBP"},
        )
    except Exception as exc:
        error = str(exc)
        log_json(
            "listing_update_failed",
            listing_id=listing_id,
            desired_quantity=desired_q,
            payload={"price": payload["price"], "currency": "GBP"},
            error=error[:500],
        )

    return ActivationResult(
        listing_id=listing_id,
        requested_quantity=desired_q,
        verified_quantity=verified_q,
        ok=ok,
        error=error,
    )


class ActivationJournal:
    """
    Tracks which listings were temporarily activated so we can roll them back.
    Use in a try/finally block in your cycle flow.
    """
    def __init__(self) -> None:
        self._activated_ids: set[str] = set()

    def record_activation(self, listing_id: str) -> None:
        self._activated_ids.add(listing_id)

    async def rollback(self, requester: Requester, child_by_id: dict[str, dict[str, Any]]) -> None:
        failures: list[str] = []
        for lid in sorted(self._activated_ids):
            res = await update_listing_quantity(requester, listing_id=lid, child=child_by_id.get(lid, {}), activate=False)
            if not res.ok:
                failures.append(lid)
        log_json("activation_rollback_summary", count=len(self._activated_ids), failures=failures)
