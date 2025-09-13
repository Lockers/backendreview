from __future__ import annotations
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Request, Query

from pricer.sku.validate_buyback import validate_buyback_sku, doc_for_good, doc_for_bad
from pricer.db.repositories.sku_repo import (
    ensure_indexes_buyback_good,
    ensure_indexes_buyback_bad,
    bulk_upsert,
    clear_collection,
    find_docs,
)

router = APIRouter(prefix="/bm/buyback/sku", tags=["bm:buyback:sku"])

@router.post("/validate")
async def validate_buyback_skus(
    request: Request,
    limit: int = Query(default=200_000, ge=1, le=1_000_000),
    save: bool = Query(default=True),
    fresh: bool = Query(default=True, description="If true, clears good/bad collections before saving"),
):
    settings = request.app.state.settings
    mongo = request.app.state.mongo

    src = mongo.listings(settings.mongo_coll_buyback_listings)
    good = mongo.listings(settings.mongo_coll_buyback_skus_good)
    bad = mongo.listings(settings.mongo_coll_buyback_skus_bad)

    if save:
        if fresh:
            await clear_collection(good)
            await clear_collection(bad)
        await ensure_indexes_buyback_good(good)
        await ensure_indexes_buyback_bad(bad)

    # Only need id + sku from buyback listings
    cursor = src.find({}, {}).limit(limit)

    good_docs, bad_docs = [], []
    scanned = ok = invalid_format = invalid_value = missing = 0

    async for doc in cursor:
        listing_id = doc.get("id")
        sku = doc.get("sku")
        source = doc                         # <— full original page item

        v = validate_buyback_sku(sku)
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
        "samples": {"bad": bad_docs[:5]},
    }

@router.get("/bad")
async def list_bad_buyback_skus(
    request: Request,
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
):
    settings = request.app.state.settings
    mongo = request.app.state.mongo
    coll = mongo.listings(settings.mongo_coll_buyback_skus_bad)
    docs = await find_docs(
        coll,
        projection={"_id": 0, "id": 1, "sku": 1, "sku_status": 1, "sku_errors": 1, "validated_at": 1},
        skip=skip,
        limit=limit,
    )
    return {"count": len(docs), "items": docs}

@router.get("/good")
async def list_good_buyback_skus(
    request: Request,
    make: Optional[str] = Query(default=None),
    model: Optional[str] = Query(default=None),
    storage: Optional[str] = Query(default=None),
    condition: Optional[str] = Query(default=None),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
):
    settings = request.app.state.settings
    mongo = request.app.state.mongo
    coll = mongo.listings(settings.mongo_coll_buyback_skus_good)

    filt: Dict[str, Any] = {}
    if make:      filt["sku_parts.make"] = make.strip().upper()
    if model:     filt["sku_parts.model"] = " ".join(model.strip().upper().split())
    if storage:   filt["sku_parts.storage"] = storage.strip().upper()
    if condition: filt["sku_parts.condition"] = condition.strip().upper()

    docs = await find_docs(
        coll,
        filter=filt,
        projection={"_id": 0, "id": 1, "sku": 1, "sku_status": 1, "sku_parts": 1, "validated_at": 1},
        skip=skip,
        limit=limit,
    )
    return {"count": len(docs), "items": docs}
