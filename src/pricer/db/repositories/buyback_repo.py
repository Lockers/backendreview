from __future__ import annotations
from typing import Iterable, Mapping
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import UpdateOne, IndexModel, ASCENDING

async def ensure_indexes(coll: AsyncIOMotorCollection) -> None:
    await coll.create_indexes([
        IndexModel([("_id", ASCENDING)], name="pk_id"),
        IndexModel([("productId", ASCENDING)], name="product"),
        IndexModel([("aestheticGradeCode", ASCENDING)], name="grade_code"),
        IndexModel([("markets", ASCENDING)], name="markets"),
    ])

def _doc(item: Mapping) -> dict:
    d = dict(item)
    d["_id"] = d["id"]  # Back Market buyback listing id
    return d

async def bulk_upsert(coll: AsyncIOMotorCollection, items: Iterable[Mapping], batch_size: int = 1000) -> dict:
    ops = []
    matched = modified = upserted = batches = 0
    for it in items:
        d = _doc(it)
        _id = d["_id"]
        update = {k: v for k, v in d.items() if k != "_id"}
        ops.append(UpdateOne({"_id": _id}, {"$set": update, "$setOnInsert": {"created_at": d.get("created_at")}}, upsert=True))
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
