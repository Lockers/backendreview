# src/pricer/bm/endpoints/listings_get_all.py
from __future__ import annotations

from typing import Any, Dict, List

from pricer.bm.requester.client import Requester
from pricer.core.exceptions import BackMarketDataError
from pricer.utils.logging import log_json


def _read_quantity(obj: Dict[str, Any]) -> int:
    """
    Robustly extract a quantity-like value from a listing payload.
    Prefer top-level 'quantity', else 'quantities.available', else 0.
    """
    q = obj.get("quantity")
    if isinstance(q, int):
        return q
    quantities = obj.get("quantities")
    if isinstance(quantities, dict):
        q2 = quantities.get("available")
        if isinstance(q2, int):
            return q2
    return 0


def _read_price_like(obj: Dict[str, Any], key: str) -> str | None:
    """
    Return a price-like field as a string if present.
    """
    v = obj.get(key)
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return f"{v:.2f}"
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


async def fetch_all_pages(
    req: Requester,
    *,
    page_size: int = 100,
    endpoint_tag: str = "listings_get_all",
) -> List[Dict[str, Any]]:
    """
    Fetch all seller listings using Back Market's paginated endpoint.
    Relies on Requester.paginate() which follows 'next' links with per-page retries.
    """
    pages = await req.paginate(
        "/ws/listings",
        page_param="page",
        size_param="page-size",
        page_size=page_size,
        params={},
        endpoint_tag=endpoint_tag,
        category="seller_generic",
    )

    results: List[Dict[str, Any]] = []
    for i, page in enumerate(pages, start=1):
        # Common list shape is {"count": N, "next": url|null, "previous": url|null, "results": [...]}
        items = page.get("results")
        if not isinstance(items, list):
            raise BackMarketDataError("Unexpected listings page shape (missing 'results')")
        results.extend(items)
        log_json("listings_scan_page", page=i, page_count=len(items))

    log_json("listings_scan_complete", total=len(results), pages=len(pages))

    # ---- completeness check (count vs aggregated results) ----
    expected_total = None
    if pages and isinstance(pages[0].get("count"), int):
        expected_total = pages[0]["count"]

    if expected_total is not None and len(results) != expected_total:
        log_json(
            "listings_count_mismatch",
            expected=expected_total,
            got=len(results),
            delta=expected_total - len(results),
            run_id=req.run_id,
        )
        raise BackMarketDataError(
            f"Listings count mismatch: expected {expected_total}, got {len(results)}"
        )

    return results


async def snapshot_index_by_id(
    req: Requester,
    *,
    page_size: int = 100,
) -> Dict[str, Dict[str, Any]]:
    """
    Build an index keyed by listing UUID string:
      idx[id] = {
        "id": <uuid>,
        "quantity": <int>,
        "max_price": <str|None>,
        "price": <str|None>,
        "active": <bool>,
        "raw": <original object>,
      }
    """
    items = await fetch_all_pages(req, page_size=page_size)

    idx: Dict[str, Dict[str, Any]] = {}
    for obj in items:
        # Prefer 'id' (uuid). Some payloads may also have 'listing_id' (int); we use 'id' consistently.
        lid = obj.get("id")
        if not isinstance(lid, str):
            # Skip objects without a UUID id; defensive
            continue

        qty = _read_quantity(obj)
        price = _read_price_like(obj, "price")
        max_price = _read_price_like(obj, "max_price")
        active = qty > 0

        idx[lid] = {
            "id": lid,
            "quantity": qty,
            "price": price,
            "max_price": max_price,
            "active": active,
            "raw": obj,
        }
    log_json("listings_snapshot_built", count=len(idx))
    return idx


