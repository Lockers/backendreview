from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from typing import cast

from pricer.core.settings import Settings
from pricer.db.mongo import mongo_lifespan, Mongo
from pricer.utils.logging import setup_logging
from pricer.web.routers import health, listings, sku, buyback, buyback_sku, tradein_groups, listing_actions, activation, run

def get_settings(app: FastAPI) -> Settings:
    return cast(Settings, app.state.settings)

def get_mongo(app: FastAPI) -> Mongo:
    return cast(Mongo, app.state.mongo)

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(logging.INFO)
    settings = Settings()
    # create & share a single Mongo instance
    async with mongo_lifespan(settings) as mongo:
        app.state.settings = settings
        app.state.mongo = mongo
        yield
    # cleanup happens in mongo_lifespan

def create_app() -> FastAPI:
    app = FastAPI(
        title="Pricer API",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(listings.router)

    app.include_router(sku.router)

    app.include_router(buyback.router)

    app.include_router(buyback_sku.router)

    app.include_router(tradein_groups.router)

    app.include_router(listing_actions.router)

    app.include_router(activation.router)

    app.include_router(run.router)

    return app

app = create_app()


