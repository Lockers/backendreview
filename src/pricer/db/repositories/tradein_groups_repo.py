from __future__ import annotations

from typing import Any, Dict, List
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ReplaceOne, UpdateOne  # <-- use real ops

async def ensure_indexes(coll: AsyncIOMotorCollection) -> None:
    await coll.create_index("parts.make")
    await coll.create_index("parts.model")
    await coll.create_index("parts.storage")
    await coll.create_index("parts.condition")
    await coll.create_index("children.active")

async def clear_collection(coll: AsyncIOMotorCollection) -> None:
    await coll.delete_many({})

async def bulk_upsert(
    coll: AsyncIOMotorCollection,
    docs: List[Dict[str, Any]],
    batch_size: int = 500,
) -> int:
    """
    Upsert all docs by `_id` in batches using proper BulkWrite operations.
    Returns number of upserted+modified docs.
    """
    total = 0
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in batch]
        if not ops:
            continue
        res = await coll.bulk_write(ops, ordered=False)
        total += (res.upserted_count or 0) + (res.modified_count or 0)
    return total

async def mark_child_activation(coll, group_key: str, list_id: str, run_id: str, prev_active: bool, prev_qty: int):
    return await coll.update_one(
        {"_id": group_key, "children.id": list_id},
        {"$set": {
            "children.$.activation": {
                "run_id": run_id,
                "activated_at": datetime.utcnow(),
                "prev_active": prev_active,
                "prev_quantity": prev_qty,
            }
        }}
    )

async def clear_child_activation(coll, group_key: str, list_id: str):
    return await coll.update_one(
        {"_id": group_key, "children.id": list_id},
        {"$unset": {"children.$.activation": ""}}
    )