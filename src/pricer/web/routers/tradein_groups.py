from __future__ import annotations

import uuid
from typing import Any, Dict, List

from fastapi import APIRouter, Query, Request

from pricer.db.repositories.tradein_groups_repo import (
    clear_collection,
    ensure_indexes,
    bulk_upsert,
)
from pricer.core.grouping.tradein_groups_builder import build_groups
from pricer.utils.logging import log_json

router = APIRouter(prefix="/bm/tradein", tags=["TradeIn Groups"])


@router.post("/group")
async def build_tradein_groups(
    request: Request,
    save: bool = Query(default=True),
    fresh: bool = Query(default=True),
    limit: int = Query(default=200_000, ge=1, le=1_000_000),
):
    """Builds the unified trade-in grouping collection."""
    settings = request.app.state.settings
    mongo = request.app.state.mongo

    run_id = str(uuid.uuid4())

    buyback_coll = mongo.listings(settings.mongo_coll_buyback_skus_good)
    sell_coll = mongo.listings(settings.mongo_coll_skus_good)
    groups_coll = mongo.listings(settings.mongo_coll_tradein_groups)

    # Prepare destination
    if save:
        if fresh:
            await clear_collection(groups_coll)
        await ensure_indexes(groups_coll)

    # Load source docs
    buyback_docs = await buyback_coll.find({}, {}).limit(limit).to_list(length=None)
    sell_docs = await sell_coll.find({}, {}).limit(limit).to_list(length=None)

    # Build groups
    groups = build_groups(buyback_docs, sell_docs, run_id=run_id)

    persist = None
    if save and groups:
        persist = await bulk_upsert(groups_coll, groups)

    log_json("tradein_groups_built", run_id=run_id, count=len(groups))

    return {
        "run_id": run_id,
        "count": len(groups),
        "persisted": persist,
        "sample": groups[:2],
    }
