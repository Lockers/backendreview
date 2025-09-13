# add these fields & methods if not present
from __future__ import annotations
import time
import threading

class TokenBucket:
    def __init__(self, max_per_window: int, window_seconds: int) -> None:
        self.base_max = max_per_window
        self._effective_max = float(max_per_window)
        self.window_seconds = float(window_seconds)
        self.tokens = float(max_per_window)
        self.updated_at = time.monotonic()
        self.ok_streak = 0
        self._lock = threading.Lock()

    def effective_max(self) -> int:
        return max(1, int(self._effective_max))

    async def acquire(self) -> float:
        # refill
        now = time.monotonic()
        with self._lock:
            elapsed = now - self.updated_at
            self.updated_at = now
            refill = (self.effective_max() * elapsed) / self.window_seconds
            self.tokens = min(self.effective_max(), self.tokens + refill)

            sleep_time = 0.0
            if self.tokens < 1.0:
                needed = 1.0 - self.tokens
                rate_per_sec = self.effective_max() / self.window_seconds
                sleep_time = needed / max(rate_per_sec, 1e-6)
            else:
                self.tokens -= 1.0
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)
        return sleep_time

    # --- adaptive bits ---
    def penalize(self, factor: float = 0.5) -> None:
        """Shrink throughput on 429/WAF (HTML) etc."""
        with self._lock:
            self._effective_max = max(1.0, self._effective_max * factor)
            self.ok_streak = 0

    def record_ok(self, recover_after_ok: int = 10) -> None:
        """After a few OKs, nudge back toward base limit."""
        with self._lock:
            self.ok_streak += 1
            if self.ok_streak >= recover_after_ok and self._effective_max < self.base_max:
                # step back toward base (25% of gap)
                gap = self.base_max - self._effective_max
                self._effective_max = min(self.base_max, self._effective_max + max(1.0, gap * 0.25))
                self.ok_streak = 0
