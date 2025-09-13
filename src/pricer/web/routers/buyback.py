from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from pricer.core.settings import Settings
from pricer.bm.requester.client import Requester
from pricer.db.repositories.buyback_repo import BuybackRepo
from pricer.utils.logging import log_json

router = APIRouter(tags=["buyback"])


@router.get("/bm/buyback/scan")
async def scan_buyback_listings(
    request: Request,
    page_size: int = Query(100, ge=1, le=100),
    include_raw: bool = Query(False),
    save: bool = Query(True),
) -> Dict[str, Any]:
    """
    Fetch all trade-in (buyback) listings via cursor pagination.
    - Persists docs with timestamps when `save=True`.
    - Emits per-endpoint learning snapshots (buyback category).
    - Returns a summary (and an optional sample for sanity).
    """
    settings: Settings = request.app.state.settings
    mongo = request.app.state.mongo
    endpoint_rates_repo = getattr(request.app.state, "endpoint_rates_repo", None)

    rid = f"run-{int(time.time() * 1000)}"
    log_json("buyback_scan_start", run_id=rid, page_size=page_size, save=save)

    written_total = 0
    pages = 0
    samples: List[Dict[str, Any]] = []

    try:
        async with Requester(settings, rid, endpoint_rates_repo=endpoint_rates_repo) as req:
            all_pages = await req.paginate(
                "/ws/buyback/v1/listings",
                size_param="pageSize",
                page_size=page_size,
                params={},
                endpoint_tag="buyback_listings",
                category="buyback",
                cursor_param="cursor",   # enable cursor mode
                next_field="next",
            )

        repo = BuybackRepo(mongo)

        for p in all_pages:
            pages += 1
            items = p.get("results") or []
            if not isinstance(items, list):
                items = []
            if save and items:
                res = await repo.upsert_many(items)
                written_total += int(res.get("written", 0))
            if not samples and items:
                # keep a small sample for visibility
                for it in items[:3]:
                    samples.append(
                        {
                            "id": it.get("id"),
                            "productId": it.get("productId"),
                            "sku": it.get("sku"),
                            "aestheticGradeCode": it.get("aestheticGradeCode"),
                            "prices": it.get("prices"),
                            "markets": it.get("markets"),
                        }
                    )

        summary: Dict[str, Any] = {
            "run_id": rid,
            "pages": pages,
            "persist": {"written": written_total, "collection": "bm_buyback_listings"} if save else None,
            "sample": samples if not include_raw else all_pages[:1],  # avoid massive payloads
        }
        log_json("buyback_scan_complete", run_id=rid, pages=pages, written=written_total)
        return summary

    except Exception as e:
        log_json("buyback_scan_error", run_id=rid, error=str(e)[:400])
        # Return partial summary instead of 500-ing, so the run can still proceed / debug
        return {
            "run_id": rid,
            "error": str(e),
            "pages": pages,
            "persist": {"written": written_total, "collection": "bm_buyback_listings"} if save else None,
            "sample": samples,
        }





