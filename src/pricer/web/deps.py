from __future__ import annotations

import time
from typing import Callable

from fastapi import Depends, Request

from pricer.bm.requester.client import Requester
from pricer.core.settings import Settings
from pricer.db.mongo import Mongo

def get_settings(request: Request) -> Settings:
    # single source of truth set in app.lifespan
    return request.app.state.settings  # type: ignore[attr-defined]

def get_db(request: Request, settings: Settings = Depends(get_settings)) -> Mongo:
    return request.app.state.mongo  # type: ignore[attr-defined]

async def get_requester(request: Request, settings: Settings = Depends(get_settings)) -> Requester:
    """
    FastAPI dependency that returns a Requester wired with the learning repo.
    """
    rid = f"run-{int(time.time() * 1000)}"
    endpoint_rates_repo = getattr(request.app.state, "endpoint_rates_repo", None)
    return Requester(settings, run_id=rid, endpoint_rates_repo=endpoint_rates_repo)

def get_requester_factory(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> Callable[[str], Requester]:
    """
    Factory used by some routers (kept for backwards compat).
    It returns a callable that accepts run_id and produces a Requester.
    Also supports the old `.new(run_id)` style to avoid touching routers.
    """
    endpoint_rates_repo = getattr(request.app.state, "endpoint_rates_repo", None)

    class _Factory:
        def __init__(self) -> None:
            self._settings = settings
            self._repo = endpoint_rates_repo

        def __call__(self, run_id: str) -> Requester:
            return Requester(self._settings, run_id=run_id, endpoint_rates_repo=self._repo)

        # legacy convenience so existing code `factory.new(rid)` keeps working
        def new(self, run_id: str) -> Requester:
            return self(run_id)

    return _Factory()



