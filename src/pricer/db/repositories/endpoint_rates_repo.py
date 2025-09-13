from typing import Any, Dict, List
from motor.motor_asyncio import AsyncIOMotorDatabase
import datetime


class EndpointRatesRepo:
    """
    Persists learning tracker snapshots into MongoDB.
    Each document in `bm_endpoint_rates` is one flush batch.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        self.collection = db["bm_endpoint_rates"]

    async def insert_snapshots(self, snapshots: List[Dict[str, Any]]) -> None:
        """
        Insert a batch of tracker snapshots into Mongo.
        Snapshots come from LearningTracker.maybe_flush().
        """
        if not snapshots:
            return

        doc = {
            "ts": datetime.datetime.utcnow(),
            "snapshots": snapshots,
        }

        await self.collection.insert_one(doc)

    async def get_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent snapshot batches for inspection.
        """
        cursor = (
            self.collection.find().sort("ts", -1).limit(limit)
        )
        return [doc async for doc in cursor]

    async def clear(self) -> None:
        """
        Delete all documents in bm_endpoint_rates.
        """
        await self.collection.delete_many({})



