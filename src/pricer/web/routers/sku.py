from __future__ import annotations
from typing import List, Dict, Any
from fastapi import APIRouter, Request, Query

from pricer.db.repositories.sku_repo import (
    ensure_good_indexes,
    ensure_bad_indexes,
    clear_collection,
    bulk_upsert,
)
from pricer.sku.validate import (
    validate_sku,
    doc_for_good,
    doc_for_bad,
)

router = APIRouter(prefix="/bm/sku", tags=["bm:sell:sku"])

@router.post("/validate")
async def validate_all_skus(
    request: Request,
    limit: int = Query(default=200_000, ge=1, le=1_000_000),
    save: bool = Query(default=True),
    fresh: bool = Query(default=True, description="If true, clears good/bad collections before saving"),
):
    settings = request.app.state.settings
    mongo = request.app.state.mongo

    # Source: raw SELL listings from /ws/listings (full docs)
    src = mongo.listings(settings.mongo_coll_listings)

    # Destinations: validated SELL SKUs
    good = mongo.listings(settings.mongo_coll_skus_good)
    bad = mongo.listings(settings.mongo_coll_skus_bad)

    # Prepare destination collections
    if save:
        if fresh:
            await clear_collection(good)
            await clear_collection(bad)
        await ensure_good_indexes(good)
        await ensure_bad_indexes(bad)

    # Stream FULL source docs (we want entire listing as `source`)
    cursor = src.find({}, {}).limit(limit)

    good_docs: List[dict] = []
    bad_docs: List[dict] = []
    scanned = ok = invalid_format = invalid_value = missing = 0

    async for doc in cursor:
        listing_id = doc.get("id")
        sku = doc.get("sku")
        source = doc  # keep the entire original SELL listing

        v = validate_sku(sku)
        scanned += 1

        if v.status == "OK":
            ok += 1
            good_docs.append(doc_for_good(listing_id, sku, v, source=source))
        elif v.status == "MISSING":
            missing += 1
            bad_docs.append(doc_for_bad(listing_id, sku, v, source=source))
        elif v.status == "FORMAT_INVALID":
            invalid_format += 1
            bad_docs.append(doc_for_bad(listing_id, sku, v, source=source))
        else:  # VALUE_INVALID
            invalid_value += 1
            bad_docs.append(doc_for_bad(listing_id, sku, v, source=source))

    persist: Dict[str, Any] = {}
    if save:
        if good_docs:
            persist["good"] = await bulk_upsert(good, good_docs, batch_size=1000)
        if bad_docs:
            persist["bad"] = await bulk_upsert(bad, bad_docs, batch_size=1000)

    return {
        "scanned": scanned,
        "ok": ok,
        "invalid_format": invalid_format,
        "invalid_value": invalid_value,
        "missing": missing,
        "persist": persist,
        "samples": {
            "bad": bad_docs[:5],
        },
    }


