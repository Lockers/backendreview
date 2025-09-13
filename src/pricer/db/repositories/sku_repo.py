from __future__ import annotations
from typing import Iterable, Mapping, Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import UpdateOne, IndexModel, ASCENDING

async def ensure_good_indexes(coll: AsyncIOMotorCollection) -> None:
    await coll.create_indexes([
        IndexModel([("_id", ASCENDING)], name="pk_id"),  # _id index already exists; this is safe (name only)
        IndexModel([("sku_status", ASCENDING)], name="status"),
        IndexModel([("sku_parts.make", ASCENDING)], name="make"),
        IndexModel([("sku_parts.model", ASCENDING)], name="model"),
        IndexModel([("sku_parts.storage", ASCENDING)], name="storage"),
        IndexModel([("sku_parts.sim", ASCENDING)], name="sim"),
        IndexModel([("sku_parts.grade", ASCENDING)], name="grade"),
        IndexModel([("validated_at", ASCENDING)], name="validated_at"),
    ])

async def ensure_bad_indexes(coll: AsyncIOMotorCollection) -> None:
    await coll.create_indexes([
        IndexModel([("_id", ASCENDING)], name="pk_id"),
        IndexModel([("sku_status", ASCENDING)], name="status"),
        IndexModel([("validated_at", ASCENDING)], name="validated_at"),
    ])

async def bulk_upsert(coll: AsyncIOMotorCollection, docs: Iterable[Mapping], batch_size: int = 1000) -> dict:
    ops = []
    matched = modified = upserted = batches = 0
    for d in docs:
        _id = d["_id"]
        update = {k: v for k, v in d.items() if k != "_id"}
        ops.append(UpdateOne({"_id": _id}, {"$set": update, "$setOnInsert": {"created_at": d.get("validated_at")}}, upsert=True))
        if len(ops) >= batch_size:
            res = await coll.bulk_write(ops, ordered=False, bypass_document_validation=True)
            matched += res.matched_count or 0
            modified += res.modified_count or 0
            upserted += len(res.upserted_ids or {})
            batches += 1
            ops = []
    if ops:
        res = await coll.bulk_write(ops, ordered=False, bypass_document_validation=True)
        matched += res.matched_count or 0
        modified += res.modified_count or 0
        upserted += len(res.upserted_ids or {})
        batches += 1
    return {"matched": matched, "modified": modified, "upserted": upserted, "batches": batches}

async def clear_collection(coll: AsyncIOMotorCollection) -> Dict[str, Any]:
    """Fast delete-all for a small/medium collection. If you expect millions, prefer TTL/rolling."""
    res = await coll.delete_many({})
    return {"deleted": res.deleted_count}

async def find_docs(
    coll: AsyncIOMotorCollection,
    *,
    filter: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, int]] = None,
    skip: int = 0,
    limit: int = 100,
):
    cur = coll.find(filter or {}, projection or {}).skip(skip).limit(limit)
    return [d async for d in cur]

async def ensure_indexes_buyback_good(coll: AsyncIOMotorCollection) -> None:
    await coll.create_indexes([
        IndexModel([("_id", ASCENDING)], name="pk_id"),
        IndexModel([("sku_parts.make", ASCENDING)], name="make"),
        IndexModel([("sku_parts.model", ASCENDING)], name="model"),
        IndexModel([("sku_parts.storage", ASCENDING)], name="storage"),
        IndexModel([("sku_parts.condition", ASCENDING)], name="condition"),
        IndexModel([("validated_at", ASCENDING)], name="validated_at"),
    ])

async def ensure_indexes_buyback_bad(coll: AsyncIOMotorCollection) -> None:
    await coll.create_indexes([
        IndexModel([("_id", ASCENDING)], name="pk_id"),
        IndexModel([("sku_status", ASCENDING)], name="status"),
        IndexModel([("validated_at", ASCENDING)], name="validated_at"),
    ])
