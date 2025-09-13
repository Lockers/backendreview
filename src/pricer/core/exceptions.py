from __future__ import annotations


class BackMarketError(RuntimeError):
    """Base error for Back Market integration."""


class BackMarketAPIError(BackMarketError):
    pass


class BackMarketAuthError(BackMarketAPIError):
    """401."""


class BackMarketForbiddenError(BackMarketAPIError):
    """403 application-level, not WAF."""


class BackMarketWAFError(BackMarketAPIError):
    """403/HTML/WAF/bot management block."""


class BackMarketNotFoundError(BackMarketAPIError):
    pass


class BackMarketBadRequestError(BackMarketAPIError):
    pass


class BackMarketRateLimitError(BackMarketAPIError):
    pass


class BackMarketNetworkError(BackMarketAPIError):
    pass


class BackMarketServerError(BackMarketAPIError):
    pass


class BackMarketDataError(BackMarketAPIError):
    pass
