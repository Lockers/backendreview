from __future__ import annotations

import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query, Request, Response

from pricer.core.settings import Settings
from pricer.utils.logging import log_json

# We will call your existing scan_listings_core (already used in /bm/listings/scan)
from pricer.web.routers.listings import scan_listings_core

router = APIRouter(tags=["run"])


async def _maybe_build_groups(app_settings: Settings, mongo) -> Dict[str, Any]:
    """
    Best-effort call into your trade-in grouping core, if present.
    This will NOT throw if the function/module isn’t there; it just no-ops.
    """
    try:
        # Import lazily so this file doesn’t hard-couple to names while we’re aligning.
        from pricer.web.routers import tradein_groups  # type: ignore[attr-defined]

        # Try a few likely core function names you might already have:
        candidates = [
            "build_tradein_groups_core",
            "refresh_tradein_groups_core",
            "rebuild_groups_core",
            "scan_tradein_groups_core",
        ]
        for name in candidates:
            fn = getattr(tradein_groups, name, None)
            if callable(fn):
                log_json("run_groups_start", step=name)
                res = await fn(settings=app_settings, mongo=mongo)  # type: ignore[misc]
                log_json("run_groups_done", step=name)
                return {"ok": True, "used": name, "result": res}
        # Couldn’t find a known core fn
        log_json("run_groups_skipped", reason="no_core_fn_found")
        return {"ok": False, "skipped": True, "reason": "no_core_fn_found"}
    except Exception as e:
        log_json("run_groups_error", error=str(e)[:300])
        return {"ok": False, "error": str(e)[:300]}


@router.post("/run")
async def run_pipeline(
    request: Request,
    response: Response,
    page_size: int = Query(50, ge=1, le=100),
    build_groups: bool = Query(False, description="If true, try to rebuild trade-in groups first."),
    final_check: bool = Query(False, description="If true, re-fetch listings at the end without saving."),
    include_raw: bool = Query(False, description="Passed through to listing scan; usually False."),
) -> Dict[str, Any]:
    """
    Orchestrates your current 'pre + step 1' flow:
      1) (optional) trade-in grouping (best-effort, no-op if unavailable)
      2) scan listings with save=True -> updates DB to latest quantity/max_price/state
      3) (optional) final non-saving scan to verify counts

    Returns a compact summary so you can eyeball results quickly.
    """
    settings: Settings = request.app.state.settings
    mongo = request.app.state.mongo

    run_id = f"run-{int(time.time())}"
    summary: Dict[str, Any] = {"run_id": run_id, "page_size": page_size}

    # Step 0: trade-in groups (optional & best-effort)
    if build_groups:
        summary["groups"] = await _maybe_build_groups(settings, mongo)

    # Step 1: initial listings scan with save=True (this is the DB-updating one)
    log_json("run_step", step="listings_initial_save")
    initial = await scan_listings_core(
        settings=settings,
        mongo=mongo,
        page_size=page_size,
        include_raw=include_raw,
        save=True,
    )
    summary["initial"] = {
        "count": initial.get("count"),
        "pages": initial.get("pages"),
        "persist": initial.get("persist"),
        "sample": initial.get("sample", []),
    }

    # Step 2: optional final check (no DB writes)
    if final_check:
        log_json("run_step", step="listings_final_check")
        final = await scan_listings_core(
            settings=settings,
            mongo=mongo,
            page_size=page_size,
            include_raw=False,
            save=False,  # IMPORTANT: do not overwrite initial states on the check pass
        )
        summary["final_check"] = {
            "count": final.get("count"),
            "pages": final.get("pages"),
            "sample": final.get("sample", []),
        }

    return summary
