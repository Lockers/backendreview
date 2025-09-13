from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any, Dict, List, Optional, Set

from fastapi import APIRouter, Query, Request

from pricer.core.settings import Settings
from pricer.bm.requester.client import Requester
from pricer.utils.logging import log_json

router = APIRouter(tags=["listings"])


async def scan_listings_core(
    *,
    settings: Settings,
    mongo: Any,
    page_size: int,
    include_raw: bool,
    save: bool,
    baseline_run_id: Optional[str] = None,
    endpoint_rates_repo: Any = None,
) -> Dict[str, Any]:
    """
    Fetch all SELL listings with page-number pagination.
    - If save=True: upsert to `bm_listings` with timestamps.
    - If baseline_run_id is provided: stamp {baseline_run_id, was_active}.
    """
    rid = f"scan-{int(time.time() * 1000)}"
    pages = 0
    total_count: Optional[int] = None
    samples: List[Dict[str, Any]] = []
    written_total = 0

    async with Requester(settings, rid, endpoint_rates_repo=endpoint_rates_repo) as req:
        all_pages = await req.paginate(
            "/ws/listings",
            page_param="page",
            size_param="page-size",
            page_size=page_size,
            params={},
            endpoint_tag="listings_get_all",
            category="seller_generic",
            cursor_param=None,
        )

    coll = mongo.listings(getattr(settings, "mongo_coll_listings", "bm_listings"))

    now = datetime.now(timezone.utc)
    for p in all_pages:
        pages += 1
        if total_count is None and isinstance(p.get("count"), int):
            total_count = p["count"]

        items = p.get("results") or p.get("listings") or []
        if not isinstance(items, list):
            items = []

        if save and items:
            ops = []
            from pymongo import UpdateOne  # local import to avoid global dependency at import time
            for it in items:
                lid = it.get("id") or it.get("listing_id")
                if not lid:
                    continue

                qty = it.get("quantity") or 0
                try:
                    qty = int(qty)
                except Exception:
                    qty = 0

                doc = dict(it)
                doc["last_seen_at"] = now

                update_doc: Dict[str, Any] = {
                    "$set": doc,
                    "$setOnInsert": {"created_at": now},
                    "$currentDate": {"updated_at": True},
                }
                if baseline_run_id:
                    update_doc["$set"]["baseline_run_id"] = baseline_run_id
                    update_doc["$set"]["was_active"] = (qty > 0)

                ops.append(UpdateOne({"id": str(lid)}, update_doc, upsert=True))

            if ops:
                res = await coll.bulk_write(ops, ordered=False)
                written_total += int(res.upserted_count + res.modified_count)

        if not samples and items:
            for it in items[:3]:
                samples.append(
                    {
                        "id": it.get("id") or it.get("listing_id"),
                        "active": (int(it.get("quantity") or 0) > 0),
                        "price": it.get("price"),
                        "max_price": it.get("max_price"),
                        "min_price": it.get("min_price"),
                        "quantity": it.get("quantity"),
                        "publication_state": it.get("publication_state"),
                        "sku": it.get("sku"),
                    }
                )

    return {
        "count": total_count if total_count is not None else 0,
        "pages": pages,
        "persist": {"written": written_total, "collection": getattr(settings, "mongo_coll_listings", "bm_listings")} if save else None,
        "sample": samples if not include_raw else all_pages[:1],
    }


@router.get("/bm/listings/scan")
async def scan_listings(
    request: Request,
    page_size: int = Query(50, ge=1, le=100),
    include_raw: bool = Query(False),
    save: bool = Query(True),
) -> Dict[str, Any]:
    settings: Settings = request.app.state.settings
    mongo = request.app.state.mongo
    endpoint_rates_repo = getattr(request.app.state, "endpoint_rates_repo", None)

    return await scan_listings_core(
        settings=settings,
        mongo=mongo,
        page_size=page_size,
        include_raw=include_raw,
        save=save,
        baseline_run_id=None,
        endpoint_rates_repo=endpoint_rates_repo,
    )









