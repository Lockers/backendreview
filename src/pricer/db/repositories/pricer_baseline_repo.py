from __future__ import annotations

from typing import Iterable, List, Dict, Any, Set
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ASCENDING
from pymongo.errors import BulkWriteError


class PricerBaselineRepo:
    """
    Stores immutable baseline state for a pricing run.
    Unique on (run_id, id) so retries are safe.
    """

    def __init__(self, coll: AsyncIOMotorCollection) -> None:
        self.coll = coll

    async def ensure_indexes(self) -> None:
        # Unique composite index so each listing is recorded once per run
        await self.coll.create_index(
            [("run_id", ASCENDING), ("id", ASCENDING)],
            unique=True,
            name="uniq_run_id_listing_id",
        )
        # Optional helpers if you want to query by run quickly
        await self.coll.create_index([("run_id", ASCENDING)], name="run_id")

    async def bulk_insert_baseline(self, docs: Iterable[Dict[str, Any]]) -> int:
        """
        Insert baseline docs (ordered=False). Ignore duplicates (expected on retries).
        Returns number of inserted docs (best effort).
        """
        docs_list = list(docs)
        if not docs_list:
            return 0
        try:
            result = await self.coll.insert_many(docs_list, ordered=False)
            return len(result.inserted_ids)
        except BulkWriteError as bwe:
            # Count only successful inserts; dupes are fine
            details = bwe.details or {}
            write_errors = details.get("writeErrors") or []
            inserted = len(docs_list) - len(write_errors)
            return max(inserted, 0)

    async def get_ids_inactive_at_start(self, run_id: str) -> Set[str]:
        ids: Set[str] = set()
        cursor = self.coll.find({"run_id": run_id, "was_active": False}, {"id": 1})
        async for doc in cursor:
            _id = doc.get("id")
            if isinstance(_id, str):
                ids.add(_id)
        return ids

    async def get_ids_active_at_start(self, run_id: str) -> Set[str]:
        ids: Set[str] = set()
        cursor = self.coll.find({"run_id": run_id, "was_active": True}, {"id": 1})
        async for doc in cursor:
            _id = doc.get("id")
            if isinstance(_id, str):
                ids.add(_id)
        return ids
