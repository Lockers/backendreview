from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import UpdateOne, IndexModel, ASCENDING
from pymongo.errors import PyMongoError

# ~10k ops per batch is a good balance; BSON doc limit is 16MB (per doc).
DEFAULT_BATCH = 1_000

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _to_doc(item: Mapping) -> dict:
    # keep listing id unique on 'id' (UUID string)
    doc = dict(item)
    doc["_id"] = doc["id"]
    doc["updated_at"] = _now()
    return doc

async def ensure_indexes(coll):
    # Do NOT create an index on _id — it already exists and is unique by default.
    models = [
        IndexModel([("sku", ASCENDING)], name="sku"),
        IndexModel([("active", ASCENDING)], name="active"),
        IndexModel([("quantity", ASCENDING)], name="qty"),
        IndexModel([("grade", ASCENDING)], name="grade"),
        IndexModel([("publication_state", ASCENDING)], name="pub_state"),
        IndexModel([("updated_at", ASCENDING)], name="updated_at"),
    ]
    await coll.create_indexes(models)

async def bulk_upsert_listings(
    coll: AsyncIOMotorCollection,
    items: Sequence[Mapping],
    *,
    batch_size: int = DEFAULT_BATCH,
    run_id: str | None = None,
) -> dict:
    """
    Idempotent, concurrent-safe bulk upsert by _id.
    Uses ordered=False for maximum parallelism server-side.
    """
    total = len(items)
    if total == 0:
        return {"matched": 0, "modified": 0, "upserted": 0, "batches": 0}

    # Build operations in chunks to stay under memory/BSON limits.
    batches: list[list[UpdateOne]] = []
    ops: list[UpdateOne] = []
    created_at = _now()

    for it in items:
        doc = _to_doc(it)
        ops.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {
                    "$set": {k: v for k, v in doc.items() if k != "_id"},
                    "$setOnInsert": {"created_at": created_at},
                },
                upsert=True,
            )
        )
        if len(ops) >= batch_size:
            batches.append(ops)
            ops = []
    if ops:
        batches.append(ops)

    matched = modified = upserted = 0
    for i, chunk in enumerate(batches, start=1):
        try:
            res = await coll.bulk_write(chunk, ordered=False, bypass_document_validation=True)
            matched += res.matched_count or 0
            modified += res.modified_count or 0
            upserted += len(res.upserted_ids or {})
        except PyMongoError as e:
            # Return partial stats; you can also log the error here.
            return {
                "matched": matched,
                "modified": modified,
                "upserted": upserted,
                "batches": i,
                "error": str(e),
            }

    return {"matched": matched, "modified": modified, "upserted": upserted, "batches": len(batches)}
