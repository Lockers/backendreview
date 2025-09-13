from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    return {"ok": True}


@router.get("/ready")
async def ready():
    return {"ready": True}
