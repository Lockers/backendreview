from __future__ import annotations

import asyncio
import random
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

from pricer.bm.requester.client import Requester
from pricer.core.exceptions import BackMarketWAFError, BackMarketRateLimitError

ENDPOINT = "/ws/buyback/v1/listings"

def _next_cursor(next_url: Optional[str]) -> Optional[str]:
    if not next_url:
        return None
    try:
        q = parse_qs(urlparse(next_url).query)
        return (q.get("cursor") or [None])[0]
    except Exception:
        return None

async def fetch(
    requester: Requester,
    *,
    run_id: str,
    page_size: int = 100,
    cursor: Optional[str] = None,
    product_id: Optional[str] = None,
    include_raw: bool = False,
    accept_language: str = "en-gb",
    # optional tunables
    inter_page_jitter_ms: tuple[int, int] = (150, 350),
    waf_cooldown_seconds: int = 10,
    waf_retry_max: int = 3,
) -> Dict[str, Any]:
    """
    Fetch buyback (trade-in) listings via cursor pagination.
    - Uses the dedicated 'buyback' rate bucket to avoid CF blocks.
    - If Cloudflare returns an HTML 429 (classified as BackMarketWAFError), we wait and continue.
    """
    assert 1 <= page_size <= 100

    params: Dict[str, Any] = {"pageSize": page_size}
    if cursor:
        params["cursor"] = cursor
    if product_id:
        params["productId"] = product_id

    headers = {
        "Accept-Language": accept_language,  # same stable header set style as other endpoints
    }

    items: List[Dict[str, Any]] = []
    raw_pages: List[Dict[str, Any]] = []
    pages = 0
    count_seen = 0
    next_cur: Optional[str] = None
    consecutive_waf = 0

    while True:
        try:
            data = await requester.send(
                "GET",
                ENDPOINT,
                params=params,
                headers=headers,
                endpoint_tag="buyback_listings",
                category="buyback",  # << use the stricter bucket
            )
        except (BackMarketWAFError, BackMarketRateLimitError):
            consecutive_waf += 1
            if consecutive_waf > waf_retry_max:
                # give up after a few backoffs so we don't loop forever
                raise
            await asyncio.sleep(waf_cooldown_seconds)
            # retry same page/cursor after cooldown
            continue

        # success -> reset WAF counter
        consecutive_waf = 0
        pages += 1

        if not isinstance(data, dict):
            break

        results = data.get("results") or []
        if isinstance(results, list):
            if include_raw:
                raw_pages.append(data)
            for it in results:
                rec = dict(it)
                rec["_source"] = "buyback"
                items.append(rec)
            count_seen += len(results)

        next_cur = _next_cursor(data.get("next"))
        if not next_cur:
            break

        # gentle jitter between pages to avoid burst patterns
        low, high = inter_page_jitter_ms
        await asyncio.sleep(random.uniform(low, high) / 1000.0)

        params = {"pageSize": page_size, "cursor": next_cur}
        if product_id:
            params["productId"] = product_id

    meta = {
        "pages_fetched": pages,
        "count_seen": count_seen,
        "next_cursor": next_cur,
        "page_size": page_size,
    }
    out: Dict[str, Any] = {"meta": meta, "items": items}
    if include_raw:
        out["raw_pages"] = raw_pages
    return out


