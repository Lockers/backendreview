from __future__ import annotations
import time
from fastapi import APIRouter, HTTPException, Query, Response, Request, status

from pricer.web.deps import get_requester_factory, new_run_id
from pricer.core.exceptions import (
    BackMarketAPIError,
    BackMarketAuthError,
    BackMarketForbiddenError,
    BackMarketRateLimitError,
    BackMarketServerError,
    BackMarketWAFError,
)
from pricer.bm.endpoints import buyback_listings
from pricer.db.repositories import buyback_repo

router = APIRouter(prefix="/bm/buyback", tags=["backmarket:buyback"])

@router.get("/scan")
async def scan_buyback_listings(
    request: Request,
    response: Response,
    run_id: str | None = Query(default=None),
    page_size: int = Query(default=100, ge=1, le=100),
    cursor: str | None = Query(default=None, description="Start cursor (uuid)"),
    product_id: str | None = Query(default=None),
    include_raw: bool = Query(default=False),
    save: bool = Query(default=True),
):
    settings = request.app.state.settings
    mongo = request.app.state.mongo
    rid = run_id or new_run_id()
    started = time.perf_counter()

    try:
        async with get_requester_factory(settings).new(rid) as requester:
            result = await buyback_listings.fetch(
                requester,
                run_id=rid,
                page_size=page_size,
                cursor=cursor,
                product_id=product_id,
                include_raw=include_raw,
                accept_language=settings.bm_accept_language,
            )
    except BackMarketWAFError as e:
        response.headers["X-Run-Id"] = rid
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail={"run_id": rid, "error": "WAF_BLOCK", "detail": str(e)})
    except BackMarketAuthError as e:
        response.headers["X-Run-Id"] = rid
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,
                            detail={"run_id": rid, "error": "UPSTREAM_AUTH", "detail": str(e)})
    except BackMarketForbiddenError as e:
        response.headers["X-Run-Id"] = rid
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,
                            detail={"run_id": rid, "error": "UPSTREAM_FORBIDDEN", "detail": str(e)})
    except BackMarketRateLimitError as e:
        response.headers["X-Run-Id"] = rid
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail={"run_id": rid, "error": "RATE_LIMIT", "detail": str(e)})
    except BackMarketServerError as e:
        response.headers["X-Run-Id"] = rid
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,
                            detail={"run_id": rid, "error": "UPSTREAM_FAILURE", "detail": str(e)})
    except BackMarketAPIError as e:
        response.headers["X-Run-Id"] = rid
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,
                            detail={"run_id": rid, "error": "UPSTREAM_DATA", "detail": str(e)})

    # Optional persistence
    persist = {}
    if save:
        coll = mongo.listings(settings.mongo_coll_buyback_listings)
        await buyback_repo.ensure_indexes(coll)
        persist = await buyback_repo.bulk_upsert(coll, result["items"], batch_size=1000)

    duration_ms = int((time.perf_counter() - started) * 1000)
    result["meta"]["duration_ms"] = duration_ms  # type: ignore[index]
    result["run_id"] = rid
    result["country"] = settings.bm_accept_language

    response.headers["X-Run-Id"] = rid
    response.headers["X-Items-Count"] = str(result["meta"]["count_seen"])  # type: ignore[index]
    response.headers["X-Scan-Duration-Ms"] = str(duration_ms)

    if save:
        result["meta"]["persist"] = persist  # type: ignore[index]
    return result
