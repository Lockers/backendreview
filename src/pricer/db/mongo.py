from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import WriteConcern
from pymongo.errors import PyMongoError

from pricer.core.settings import Settings


class Mongo:
    def __init__(self, client: AsyncIOMotorClient, db_name: str) -> None:
        self.client = client
        self.db = client[db_name]

    def listings(self, coll_name: str) -> AsyncIOMotorCollection:
        # Fire-and-forget write concern (low latency) is w=1 by default.
        # You can tighten this later if you want stronger guarantees.
        return self.db.get_collection(coll_name, write_concern=WriteConcern(w=1))

    def endpoint_rates(self, coll_name: str) -> AsyncIOMotorCollection:
        # For Requester learning snapshots (rate observations)
        return self.db.get_collection(coll_name, write_concern=WriteConcern(w=1))

    def pricer_baseline(self, coll_name: str) -> AsyncIOMotorCollection:
        # Immutable baseline per run_id (was_active, qty, etc.)
        return self.db.get_collection(coll_name, write_concern=WriteConcern(w=1))


async def _init_client(uri: str) -> AsyncIOMotorClient:
    cli = AsyncIOMotorClient(
        uri,
        uuidRepresentation="standard",
        serverSelectionTimeoutMS=5_000,
    )
    # sanity ping (non-blocking long term)
    try:
        await cli.admin.command("ping")
    except PyMongoError:
        # Don’t fail app startup; allow routes to error if DB is down.
        pass
    return cli


@asynccontextmanager
async def mongo_lifespan(settings: Settings) -> AsyncIterator[Mongo]:
    client = await _init_client(settings.mongo_uri)
    try:
        yield Mongo(client, settings.mongo_db)
    finally:
        client.close()


# FastAPI deps
def get_settings() -> Settings:
    return Settings()


async def get_mongo(settings: Settings = Depends(get_settings)) -> Mongo:
    # This getter is replaced by the app lifespan so routes get a singleton.
    client = await _init_client(settings.mongo_uri)
    return Mongo(client, settings.mongo_db)

