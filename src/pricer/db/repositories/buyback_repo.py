from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

try:
    from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection
    from pymongo import UpdateOne
except Exception:  # pragma: no cover
    AsyncIOMotorDatabase = Any  # type: ignore
    AsyncIOMotorCollection = Any  # type: ignore
    UpdateOne = Any  # type: ignore


class BuybackRepo:
    """
    Simple repo to upsert buyback listings with timestamps.
    """

    def __init__(self, db_or_mongo: Any, coll_name: str = "bm_buyback_listings") -> None:
        if hasattr(db_or_mongo, "db") and getattr(db_or_mongo, "db") is not None:
            self.db: AsyncIOMotorDatabase = db_or_mongo.db  # type: ignore[assignment]
        else:
            self.db = db_or_mongo  # type: ignore[assignment]

        self.collection: AsyncIOMotorCollection = self.db[coll_name]  # type: ignore[index]

    async def upsert_many(self, docs: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        ops = []
        now = datetime.now(timezone.utc)

        for raw in docs:
            if not raw or not isinstance(raw, dict):
                continue
            doc = dict(raw)
            _id = doc.get("id")
            if not _id:
                continue

            # Always stamp last-seen server time
            doc["last_seen_at"] = now

            ops.append(
                UpdateOne(
                    {"id": _id},
                    {
                        "$set": doc,
                        "$setOnInsert": {"created_at": now},
                        "$currentDate": {"updated_at": True},
                    },
                    upsert=True,
                )
            )

        if not ops:
            return {"written": 0, "collection": self.collection.name}

        res = await self.collection.bulk_write(ops, ordered=False)
        return {"written": res.upserted_count + res.modified_count, "collection": self.collection.name}



