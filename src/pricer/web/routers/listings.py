from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request, Response

from pricer.core.settings import Settings
from pricer.bm.requester.client import Requester
from pricer.utils.logging import log_json

router = APIRouter(tags=["listings"])

logger = logging.getLogger(__name__)


# ---------- PURE SERVICE (no FastAPI objects) ----------
async def scan_listings_core(
    *,
    settings: Settings,
    mongo,
    page_size: int,
    include_raw: bool,
    save: bool,
) -> Dict[str, Any]:
    """
    Fetch Back Market listings via /ws/listings and (optionally) persist to Mongo.

    Returns a dict with counts + basic persist summary.
    """
    run_id = f"ls-scan-{int(time.time())}"
    total_items = 0
    pages_seen = 0
    collected: List[Dict[str, Any]] = []

    # Call BM with the shared Requester
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

    pages_seen = len(pages)

    # Flatten results from each page (handle a couple of possible shapes)
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
            if not isinstance(it, dict):
                continue

            # Keep original object (include_raw toggle stays here if you want to trim later)
            doc = dict(it)

            # Normalize id
            doc["id"] = doc.get("id") or doc.get("listing_id")
            if not doc["id"]:
                continue

            # Normalize numeric fields that we rely on later
            qty = doc.get("quantity")
            try:
                qty = int(qty) if qty is not None else 0
            except Exception:
                qty = 0
            doc["quantity"] = qty

            for price_key in ("max_price", "min_price"):
                if price_key in doc and doc[price_key] is not None:
                    try:
                        doc[price_key] = float(doc[price_key])
                    except Exception:
                        # leave as-is if it truly can't be parsed
                        pass

            # Compute 'active' flag (simple rule: quantity > 0)
            pub_state = doc.get("publication_state")
            # If you want stricter, uncomment next line:
            # doc["active"] = (qty > 0) and (pub_state in (3, 4))
            doc["active"] = (qty > 0)

            collected.append(doc)

    total_items = len(collected)

    persist: Dict[str, Any] = {}
    if save and total_items:
        coll = mongo.listings(settings.mongo_coll_listings)

        # Upsert concurrently in batches
        async def upsert_many(docs: List[Dict[str, Any]], *, scan_run_id: str) -> int:
            from pymongo import UpdateOne  # type: ignore

            now = datetime.now(timezone.utc).replace(tzinfo=None)  # store as UTC naive for Mongo
            ops = []
            for d in docs:
                key = {"id": d["id"]}

                # Always refresh updated_at; create created_at only once.
                # Tag with current scan_run_id for traceability.
                update = {
                    "$set": {
                        **d,
                        "updated_at": now,
                        "scan_run_id": scan_run_id,
                    },
                    "$setOnInsert": {
                        "created_at": now,
                    },
                }
                ops.append(UpdateOne(key, update, upsert=True))

            if not ops:
                return 0

            res = await coll.bulk_write(ops, ordered=False)
            # Count all affected docs (modified + matched + upserted)
            return int(
                (res.upserted_count or 0)
                + (res.modified_count or 0)
                + (res.matched_count or 0)
            )

        # Chunk into reasonable batches
        BATCH = 1_000
        written = 0
        for i in range(0, len(collected), BATCH):
            written += await upsert_many(collected[i : i + BATCH], scan_run_id=run_id)

        persist = {
            "written": written,
            "collection": settings.mongo_coll_listings,
            "scan_run_id": run_id,
        }

    return {
        "run_id": run_id,
        "count": total_items,
        "pages": pages_seen,
        "persist": persist,
        "sample": collected[:3],
    }


# ---------- FASTAPI ENDPOINT (calls the pure service) ----------
@router.get("/bm/listings/scan")
async def scan_listings(
    request: Request,
    response: Response,
    page_size: int = Query(50, ge=1, le=100),
    include_raw: bool = Query(False),
    save: bool = Query(True),
):
    settings: Settings = request.app.state.settings
    mongo = request.app.state.mongo

    result = await scan_listings_core(
        settings=settings,
        mongo=mongo,
        page_size=page_size,
        include_raw=include_raw,
        save=save,
    )
    return result







