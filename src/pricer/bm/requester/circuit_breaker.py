from __future__ import annotations

import time


class CircuitBreaker:
    def __init__(self, fail_threshold: int, cooldown_seconds: int) -> None:
        self.fail_threshold = fail_threshold
        self.cooldown = cooldown_seconds
        self._consecutive = 0
        self._open_until: float = 0.0

    def allow(self) -> bool:
        return time.monotonic() >= self._open_until

    def record_success(self) -> None:
        self._consecutive = 0

    def record_failure(self) -> None:
        self._consecutive += 1
        if self._consecutive >= self.fail_threshold:
            self._open_until = time.monotonic() + self.cooldown
            self._consecutive = 0  # reset after opening

    def remaining_cooldown(self) -> float:
        rem = self._open_until - time.monotonic()
        return rem if rem > 0 else 0.0
