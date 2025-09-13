from __future__ import annotations

import time
from typing import Any, Dict, Set

from fastapi import APIRouter, Query, Request

from pricer.core.settings import Settings
from pricer.bm.requester.client import Requester
from pricer.web.routers.listings import scan_listings_core
from pricer.utils.logging import log_json

router = APIRouter(tags=["pricer"])


@router.post("/bm/pricer/run")
async def run_pricer_flow(
    request: Request,
    page_size: int = Query(50, ge=1, le=100),
) -> Dict[str, Any]:
    """
    Single-entry orchestrator for the pricing run.

      1) Baseline scan (persist = True) → stamps { baseline_run_id, was_active }
      2) [Your steps] activate → read bi_listings → deactivate back to baseline
      3) Final verification scan (persist = False) → compare to baseline
    """
    settings: Settings = request.app.state.settings
    mongo = request.app.state.mongo
    endpoint_rates_repo = getattr(request.app.state, "endpoint_rates_repo", None)

    run_id = f"pricer-{int(time.time())}"

    # Step 1: baseline
    baseline = await scan_listings_core(
        settings=settings,
        mongo=mongo,
        page_size=page_size,
        include_raw=False,
        save=True,
        baseline_run_id=run_id,
        endpoint_rates_repo=endpoint_rates_repo,
    )

    # Step 2: your existing pipeline (placeholders)
    log_json("pricer_step", step="activate_non_active_to_measure_prices", run_id=run_id)
    log_json("pricer_step", step="fetch_bi_listings_prices_and_match", run_id=run_id)
    log_json("pricer_step", step="deactivate_back_to_baseline", run_id=run_id)

    # Step 3: verify (no writes)
    verify = await scan_listings_core(
        settings=settings,
        mongo=mongo,
        page_size=page_size,
        include_raw=False,
        save=False,
        baseline_run_id=None,
        endpoint_rates_repo=endpoint_rates_repo,
    )

    # Build current active set in-memory (mini collector)
    current_active_ids: Set[str] = set()
    async with Requester(settings, run_id=run_id, endpoint_rates_repo=endpoint_rates_repo) as req:
        pages = await req.paginate(
            "/ws/listings",
            page_param="page",
            size_param="page-size",
            page_size=page_size,
            params={},
            endpoint_tag="listings_get_all",
            category="seller_generic",
        )
    for page in pages:
        items = page.get("results") or page.get("listings") or []
        if not isinstance(items, list):
            continue
        for it in items:
            lid = it.get("id") or it.get("listing_id")
            if not lid:
                continue
            qty = it.get("quantity") or 0
            try:
                qty = int(qty)
            except Exception:
                qty = 0
            if qty > 0:
                current_active_ids.add(str(lid))

    # Baseline sets from Mongo
    coll = mongo.listings(getattr(settings, "mongo_coll_listings", "bm_listings"))
    cursor = coll.find({"baseline_run_id": run_id}, projection={"id": 1, "was_active": 1})
    baseline_ids_active: Set[str] = set()
    for doc in await cursor.to_list(length=None):
        lid = str(doc.get("id", ""))
        if not lid:
            continue
        if bool(doc.get("was_active", False)):
            baseline_ids_active.add(lid)

    unexpected_active = sorted(list(current_active_ids - baseline_ids_active))
    unexpected_inactive = sorted(list(baseline_ids_active - current_active_ids))

    summary = {
        "run_id": run_id,
        "baseline": {
            "pages": baseline["pages"],
            "count": baseline["count"],
            "persist": baseline["persist"],
        },
        "verify": {
            "pages": verify["pages"],
            "count": verify["count"],
        },
        "checks": {
            "expected_active_count": len(baseline_ids_active),
            "current_active_count": len(current_active_ids),
            "unexpected_active_count": len(unexpected_active),
            "unexpected_inactive_count": len(unexpected_inactive),
            "unexpected_active_sample": unexpected_active[:10],
            "unexpected_inactive_sample": unexpected_inactive[:10],
        },
    }

    log_json("pricer_run_complete", run_id=run_id, summary=summary["checks"])
    return summary

