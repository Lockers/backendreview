from __future__ import annotations
import asyncio
import time

class AdaptiveTokenBucket:
    """
    Token bucket with adaptive throughput:
    - penalize() reduces effective max rate
    - record_ok() counts successes; after enough OKs, it slowly ramps back
    """
    def __init__(self, max_per_window: int, window_seconds: int,
                 penalty_factor: float = 0.5, recover_after_ok: int = 10) -> None:
        self._base_max = max(1, max_per_window)
        self._window = max(1, window_seconds)
        self._penalty_factor = max(0.1, min(1.0, penalty_factor))
        self._recover_after_ok = max(1, recover_after_ok)

        self._effective_max = float(self._base_max)
        self._tokens = float(self._effective_max)
        self._last = time.monotonic()
        self._ok_streak = 0
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        self._last = now
        refill = (self._effective_max / self._window) * elapsed
        self._tokens = min(self._tokens + refill, self._effective_max)

    async def acquire(self) -> float:
        """
        Waits for a token and returns the sleep time (seconds) we had to wait before acquiring.
        """
        start = time.monotonic()
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    break
                # need to wait for next token
                deficit = 1.0 - self._tokens
                rate_per_sec = self._effective_max / self._window
                wait = max(0.01, deficit / max(rate_per_sec, 0.0001))
                await asyncio.sleep(wait)
        return max(0.0, time.monotonic() - start)

    def penalize(self) -> None:
        # cut throughput on 429
        self._effective_max = max(1.0, self._effective_max * self._penalty_factor)
        # cap tokens to new effective max
        self._tokens = min(self._tokens, self._effective_max)
        self._ok_streak = 0

    def record_ok(self) -> None:
        # after enough OKs, slowly ramp back (by +1 towards base)
        self._ok_streak += 1
        if self._ok_streak >= self._recover_after_ok and self._effective_max < self._base_max:
            self._effective_max = min(self._base_max, self._effective_max + 1.0)
            self._ok_streak = 0

    def snapshot(self) -> dict:
        return {
            "base_max": self._base_max,
            "effective_max": int(self._effective_max),
            "window": self._window,
            "ok_streak": self._ok_streak,
            "tokens": round(self._tokens, 2),
        }
