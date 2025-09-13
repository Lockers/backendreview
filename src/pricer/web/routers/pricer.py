from __future__ import annotations

import time
from typing import Any, Dict, List, Set

from fastapi import APIRouter, Query, Request

from pricer.core.settings import Settings
from pricer.utils.logging import log_json
from .listings import scan_listings_core  # reuse the core scan function

router = APIRouter(tags=["pricer"])


@router.post("/bm/pricer/run")
async def run_pricer_flow(
    request: Request,
    page_size: int = Query(50, ge=1, le=100),
) -> Dict[str, Any]:
    """
    Single-entry orchestrator for the pricing run.

    Steps:
      1) Baseline scan (persist = True) → stamps { baseline_run_id, was_active }
      2) [Your steps] activate missing → price collection (bi_listings) → deactivate back to baseline
      3) Final verification scan (persist = False) → compare to baseline

    Returns a summary with deltas showing any leaked actives or lost actives.
    """
    settings: Settings = request.app.state.settings
    mongo = request.app.state.mongo

    run_id = f"pricer-{int(time.time())}"

    # --- Step 1: Baseline (persist + baseline markers) ---
    baseline = await scan_listings_core(
        settings=settings,
        mongo=mongo,
        page_size=page_size,
        include_raw=False,
        save=True,
        baseline_run_id=run_id,
    )

    # --- Step 2: Your existing activation / pricing / deactivation pipeline ---
    # NOTE: We leave these as placeholders to plug into your existing code.
    # log_json helps keep an auditable trail in your logs.
    log_json("pricer_step", step="activate_non_active_to_measure_prices", run_id=run_id)
    # await activate_non_active_listings(...)

    log_json("pricer_step", step="fetch_bi_listings_prices_and_match", run_id=run_id)
    # await fetch_and_match_bi_listings_prices(...)

    log_json("pricer_step", step="deactivate_back_to_baseline", run_id=run_id)
    # await deactivate_back_to_baseline(...)

    # --- Step 3: Final verification (no writes) ---
    verify = await scan_listings_core(
        settings=settings,
        mongo=mongo,
        page_size=page_size,
        include_raw=False,
        save=False,             # verification only; DO NOT update DB
        baseline_run_id=None,   # not stamping on verify
    )

    # --- Compare current active set vs baseline was_active ---
    # Current actives from the verification scan (in-memory; no writes)
    current_active_ids: Set[str] = set()
    # To build current_active_ids we need the full listing ids from `verify`.
    # `scan_listings_core(save=False)` returns only a small sample by design, so
    # we reconstruct the active set by re-running page fetch quickly (in-memory).
    # If you prefer to avoid this second pass, you can refactor scan_listings_core
    # to optionally return the full collected list when save=False.
    # For clarity and simplicity, we do a mini collector here:

    # Mini collector: fetch all pages again (cheap) and gather active IDs
    from pricer.bm.requester.client import Requester
    async with Requester(settings, run_id=run_id) as req:
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
        items = None
        if isinstance(page, dict):
            if "results" in page and isinstance(page["results"], list):
                items = page["results"]
            elif "listings" in page and isinstance(page["listings"], list):
                items = page["listings"]
        if not items:
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
    coll = mongo.listings(settings.mongo_coll_listings)
    cursor = coll.find(
        {"baseline_run_id": run_id},
        projection={"id": 1, "was_active": 1},
    )
    baseline_ids_active: Set[str] = set()
    baseline_ids_inactive: Set[str] = set()
    for doc in await cursor.to_list(length=None):
        lid = str(doc.get("id", ""))
        if not lid:
            continue
        if bool(doc.get("was_active", False)):
            baseline_ids_active.add(lid)
        else:
            baseline_ids_inactive.add(lid)

    # Deltas
    # - unexpected_active: were inactive at baseline but are active now (leaked)
    # - unexpected_inactive: were active at baseline but are inactive now (lost)
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
