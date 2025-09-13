from __future__ import annotations

import time
from typing import Any, Dict, List

try:
    from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection
except Exception:  # pragma: no cover
    AsyncIOMotorDatabase = Any  # type: ignore
    AsyncIOMotorCollection = Any  # type: ignore


class EndpointRatesRepo:
    """
    Persistence for per-endpoint adaptive rate learning snapshots.

    Accepts either:
      - Your Mongo wrapper (with a `.db` attribute), or
      - A Motor `AsyncIOMotorDatabase`.

    Collection defaults to 'bm_endpoint_rates' unless a different name is provided.
    """

    def __init__(self, db_or_mongo: Any, coll_name: str = "bm_endpoint_rates") -> None:
        if hasattr(db_or_mongo, "db") and getattr(db_or_mongo, "db") is not None:
            self.db: AsyncIOMotorDatabase = db_or_mongo.db  # type: ignore[assignment]
        else:
            self.db = db_or_mongo  # type: ignore[assignment]

        self.collection: AsyncIOMotorCollection = self.db[coll_name]  # type: ignore[index]

    async def insert_snapshots(self, snapshots: List[Dict[str, Any]]) -> None:
        """Append-only insert of learning snapshots, with a server receipt timestamp."""
        if not snapshots:
            return
        now = time.time()
        for s in snapshots:
            s.setdefault("server_received_at", now)
        await self.collection.insert_many(snapshots, ordered=False)

    async def latest_for_endpoint(self, endpoint_tag: str, limit: int = 50) -> List[Dict[str, Any]]:
        cursor = (
            self.collection.find({"endpoint_tag": endpoint_tag})
            .sort([("_id", -1)])
            .limit(limit)
        )
        return await cursor.to_list(length=limit)

    async def clear(self) -> None:
        await self.collection.delete_many({})





