# src/pricer/web/routers/listing_actions.py
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request, Body

from pricer.bm.requester.client import Requester
from pricer.utils.logging import log_json

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/bm/listings", tags=["BackMarket • Listings: actions"])


async def _fetch_listing_info(mongo, listings_coll_name: str, listing_id: str) -> Optional[dict]:
    """Pull minimal fields needed to safely activate."""
    coll = mongo.listings(listings_coll_name)
    return await coll.find_one({"id": listing_id}, {"id": 1, "max_price": 1, "currency": 1})


async def _update_listing(
    requester: Requester,
    *,
    listing_id: string,
    active: bool,
    price: Optional[str],
    currency: Optional[str],
) -> Dict[str, Any]:
    """
    Activate or deactivate a single listing on Back Market.
    - activate: quantity=1, price=max_price, min_price=None
    - deactivate: quantity=0
    """
    payload: Dict[str, Any]
    if active:
        if not price or not currency:
            return {"id": listing_id, "success": False, "error": "MISSING_PRICE_OR_CURRENCY_FOR_ACTIVATION"}
        payload = {
            "min_price": None,      # ALWAYS None to avoid backpricer interference
            "price": str(price),    # BM prefers price as string
            "currency": currency,
            "quantity": 1,
        }
    else:
        payload = {"quantity": 0}

    path = f"/ws/listings/{listing_id}"

    try:
        res = await requester.send(
            "POST",                # <-- BM docs: POST for listing updates
            path,
            json_body=payload,
            endpoint_tag="listing_update",
            category="seller_mutations",
        )
        return {"id": listing_id, "success": True, "response": res}
    except Exception as exc:
        log_json("listing_update_failed", listing_id=listing_id, active=active, error=str(exc))
        return {"id": listing_id, "success": False, "error": str(exc)}


async def _update_many(
    request: Request,
    ids: List[str],
    *,
    active: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    """
    Iterates IDs, looks up max_price/currency from bm_listings, and issues POST calls.
    Adapts delay on 429s; keeps going and summarizes results.
    """
    settings = request.app.state.settings
    mongo = request.app.state.mongo

    run_id = f"act-{int(time.time())}"
    delay = 0.1  # adaptive pacing between calls

    results: List[Dict[str, Any]] = []

    async with Requester(settings, run_id) as requester:
        for listing_id in ids:
            if dry_run:
                results.append({"id": listing_id, "success": True, "dry_run": True})
                continue

            # Latest details from bm_listings (source of truth)
            doc = await _fetch_listing_info(mongo, settings.mongo_coll_listings, listing_id)
            max_price = (doc or {}).get("max_price")
            currency = (doc or {}).get("currency")

            if active and (not max_price or not currency):
                results.append({
                    "id": listing_id,
                    "success": False,
                    "error": "MISSING_DB_DETAILS_FOR_ACTIVATION",
                    "detail": {"have_doc": bool(doc), "max_price": max_price, "currency": currency},
                })
                continue

            res = await _update_listing(
                requester,
                listing_id=listing_id,
                active=active,
                price=max_price if active else None,
                currency=currency if active else None,
            )
            results.append(res)

            # Adaptive pacing (respond to 429s)
            if not res.get("success") and "429" in (res.get("error") or ""):
                delay = min(delay * 2, 30.0)  # back off up to 30s
            else:
                delay = max(0.05, delay * 0.7)  # gently speed up

            await asyncio.sleep(delay)

    ok = sum(1 for r in results if r.get("success"))
    failed = len(results) - ok
    return {
        "count": len(results),
        "ok": ok,
        "failed": failed,
        "results": results[:50],     # cap inline details
        "more": len(results) - 50 if len(results) > 50 else 0,
        "run_id": run_id,
    }


@router.post("/activate", summary="Activate listings (quantity=1, price=max_price, min_price=None)")
async def activate_listings(
    request: Request,
    ids: List[str] = Body(..., embed=True, description="Listing UUIDs to activate"),
    dry_run: bool = False,
) -> Dict[str, Any]:
    return await _update_many(request, ids, active=True, dry_run=dry_run)


@router.post("/deactivate", summary="Deactivate listings (quantity=0)")
async def deactivate_listings(
    request: Request,
    ids: List[str] = Body(..., embed=True, description="Listing UUIDs to deactivate"),
    dry_run: bool = False,
) -> Dict[str, Any]:
    return await _update_many(request, ids, active=False, dry_run=dry_run)

