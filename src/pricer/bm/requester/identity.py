from __future__ import annotations

from pricer.core.settings import Settings


def default_headers(settings: Settings) -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Accept-Language": settings.bm_accept_language,
        "Authorization": settings.bm_auth_token,
        "User-Agent": settings.bm_user_agent,
    }
