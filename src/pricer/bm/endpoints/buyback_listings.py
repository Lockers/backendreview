from __future__ import annotations

from typing import Any, Dict, List

from pricer.bm.requester.client import Requester

# Cursor-based buyback listings endpoint:
# GET /ws/buyback/v1/listings
# Response: { "next": "<abs-url>" | null, "results": [...] }

BUYBACK_PATH = "/ws/buyback/v1/listings"
ENDPOINT_TAG = "buyback_listings"
CATEGORY = "buyback"


async def fetch_buyback_pages(
    requester: Requester,
    page_size: int = 100,
    *,
    sleep_between_pages_ms: int = 1000,  # keep it friendly: ~1s pause between cursor pages
) -> List[Dict[str, Any]]:
    """Fetch all buyback pages sequentially, respecting BackMarket cursor pagination."""
    pages = await requester.paginate(
        BUYBACK_PATH,
        page_size=page_size,
        params={},  # no productId filter for "scan everything"
        endpoint_tag=ENDPOINT_TAG,
        category=CATEGORY,
        max_attempts_per_page=12,
        cursor_param="cursor",
        next_field="next",
        sleep_between_pages_ms=sleep_between_pages_ms,
    )
    return pages





