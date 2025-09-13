from __future__ import annotations

import random
from typing import Iterable


RETRYABLE_STATUSES: set[int] = {
    408, 409, 423, 425, 429, 500, 502, 503, 504,
}


def backoff_delay_ms(attempt: int, base_ms: int = 500, jitter_ratio: float = 0.3, max_ms: int = 10_000) -> int:
    delay = base_ms * (2 ** (attempt - 1))
    delay = min(delay, max_ms)
    jitter = int(delay * random.uniform(1 - jitter_ratio, 1 + jitter_ratio))
    return max(jitter, 0)


def is_retryable_status(status: int, extra: Iterable[int] = ()) -> bool:
    return status in RETRYABLE_STATUSES or status in set(extra)
