# src/pricer/web/deps.py
from __future__ import annotations

import time
from typing import Callable, Optional, Any, Mapping

from fastapi import Depends, Request

from pricer.core.settings import Settings
from pricer.bm.requester.client import Requester


# --------------------
# Settings
# --------------------
def get_settings(request: Request) -> Settings:
    """
    Settings are initialised in web/app.py and attached to app.state.settings.
    """
    settings: Settings = request.app.state.settings
    return settings


# --------------------
# Mongo DB (robust detection)
# --------------------
def _debug_type(obj: Any) -> str:
    try:
        return f"{type(obj).__module__}.{type(obj).__name__}"
    except Exception:
        return str(type(obj))


def get_db(request: Request, settings: Settings = Depends(get_settings)):
    """
    Return a Motor *Database* object from app.state.mongo, handling common shapes:

    - Database-like: has get_collection()  -> return as-is
    - Client-like:   has __getitem__       -> return client[settings.mongo_db]
    - Collection:    has find_one(), .database.get_collection() -> return collection.database
    - Wrapper dict:  {"db": Database} or {"database": Database} or {"client": Client}
    - Wrapper obj:   .db or .database attributes

    If none match, raise a descriptive error so we can fix the app initialisation.
    """
    mongo = getattr(request.app.state, "mongo", None)
    if mongo is None:
        raise RuntimeError("Mongo client/database not initialised on app.state.mongo")

    # 1) Direct Database?
    if hasattr(mongo, "get_collection"):
        return mongo

    # 2) Collection? (has common collection methods; get its parent database)
    if hasattr(mongo, "find_one") or hasattr(mongo, "aggregate"):
        db = getattr(mongo, "database", None)
        if db is not None and hasattr(db, "get_collection"):
            return db

    # 3) Dict/Mapping wrapper?
    if isinstance(mongo, Mapping):
        # Prefer explicit 'db' / 'database'
        for key in ("db", "database"):
            db = mongo.get(key)
            if db is not None and hasattr(db, "get_collection"):
                return db
        # Or a client we can index with db name
        client = mongo.get("client")
        if client is not None and hasattr(client, "__getitem__"):
            return client[settings.mongo_db]

    # 4) Wrapper object with .db or .database
    for attr in ("db", "database"):
        db = getattr(mongo, attr, None)
        if db is not None and hasattr(db, "get_collection"):
            return db

    # 5) Client-like? (__getitem__)
    if hasattr(mongo, "__getitem__"):
        try:
            return mongo[settings.mongo_db]
        except Exception:
            pass

    # If we got here, we don't recognise the shape. Give a helpful error.
    t = _debug_type(mongo)
    attrs = [a for a in dir(mongo) if a in ("get_collection", "__getitem__", "db", "database", "find_one", "aggregate")]
    raise RuntimeError(
        f"Unknown mongo object on app.state.mongo (type={t}). "
        f"Visible attrs={attrs}. Expected a Motor client/database/collection, or a wrapper with .db/.database/.client."
    )


# --------------------
# New-style Requester dependency (lifecycle-managed)
# --------------------
async def get_requester(settings: Settings = Depends(get_settings)):
    """
    Provides a Requester that is started before the endpoint runs and closed afterwards.
    Usage in routers:
        @router.get("/x")
        async def handler(req: Requester = Depends(get_requester)): ...
    """
    run_id = new_run_id()
    req = Requester(settings=settings, run_id=run_id)
    await req.start()
    try:
        yield req
    finally:
        await req.close()


# --------------------
# Backwards-compat helpers (legacy routers may import these)
# --------------------
def new_run_id() -> str:
    """Stable run-id generator used across routers/services."""
    return f"run-{int(time.time() * 1000)}"


def get_requester_factory(settings: Settings = Depends(get_settings)) -> Callable[[Optional[str]], Requester]:
    """
    Legacy pattern used by older routers:
        req_factory = Depends(get_requester_factory)
        async with req_factory() as req:
            ...
    This factory returns a Requester that supports async context manager
    (Requester implements __aenter__/__aexit__).
    """
    def factory(run_id: Optional[str] = None) -> Requester:
        rid = run_id or new_run_id()
        return Requester(settings=settings, run_id=rid)
    return factory


