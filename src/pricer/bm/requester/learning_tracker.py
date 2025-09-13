from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import Dict, Optional, List, Literal


Outcome = Literal["ok", "ratelimit", "waf", "server_error", "network_error", "client_error"]


@dataclass
class EndpointStats:
    ok: int = 0
    ratelimit: int = 0
    waf: int = 0
    server_error: int = 0
    network_error: int = 0
    client_error: int = 0
    last_error_at: float | None = None  # epoch seconds
    last_ok_at: float | None = None     # epoch seconds
    # soft recommendation cache (seconds)
    recommend_delay_s: float = 0.0

    def to_snapshot(self, endpoint_tag: str, run_id: str) -> Dict:
        return {
            "run_id": run_id,
            "endpoint_tag": endpoint_tag,
            "ts": int(time.time()),
            "ok": self.ok,
            "ratelimit": self.ratelimit,
            "waf": self.waf,
            "server_error": self.server_error,
            "network_error": self.network_error,
            "client_error": self.client_error,
            "last_error_at": int(self.last_error_at) if self.last_error_at else None,
            "last_ok_at": int(self.last_ok_at) if self.last_ok_at else None,
            "recommended_delay_ms": int(self.recommend_delay_s * 1000),
        }


class LearningTracker:
    """
    Lightweight per-endpoint tracker.

    - observe(endpoint_tag, outcome, ...) to record events
    - recommend_delay(endpoint_tag) returns a conservative delay hint
    - maybe_flush(run_id) returns {"snapshots": [...]} to persist (caller handles errors)

    This is intentionally simple; you can later replace the recommendation policy with
    something more sophisticated (EMA of 429s, endpoint-specific max RPS, etc.)
    """

    def __init__(self) -> None:
        self._stats: Dict[str, EndpointStats] = defaultdict(EndpointStats)
        self._last_flush_ts: float = 0.0
        self._flush_interval_s: float = 30.0  # snapshot cadence

    def observe(
        self,
        endpoint_tag: str,
        outcome: Outcome,
        *,
        elapsed_ms: Optional[int] = None,
        retry_after_s: Optional[float] = None,
    ) -> None:
        now = time.time()
        s = self._stats[endpoint_tag]

        if outcome == "ok":
            s.ok += 1
            s.last_ok_at = now
            # decay recommendation on success
            s.recommend_delay_s = max(0.0, s.recommend_delay_s * 0.5)
        elif outcome == "ratelimit":
            s.ratelimit += 1
            s.last_error_at = now
            # backoff more aggressively on explicit 429
            if retry_after_s is not None:
                s.recommend_delay_s = max(s.recommend_delay_s, float(retry_after_s))
            else:
                s.recommend_delay_s = min(max(s.recommend_delay_s * 1.5, 0.75), 10.0)
        elif outcome == "waf":
            s.waf += 1
            s.last_error_at = now
            s.recommend_delay_s = min(max(s.recommend_delay_s * 1.5, 5.0), 15.0)
        elif outcome == "server_error":
            s.server_error += 1
            s.last_error_at = now
            s.recommend_delay_s = min(max(s.recommend_delay_s * 1.3, 2.0), 10.0)
        elif outcome == "network_error":
            s.network_error += 1
            s.last_error_at = now
            s.recommend_delay_s = min(max(s.recommend_delay_s * 1.2, 1.0), 8.0)
        else:
            s.client_error += 1
            s.last_error_at = now
            # client errors do not usually imply rate backoff

    def recommend_delay(self, endpoint_tag: str) -> float:
        """Return current soft recommendation (seconds) for this endpoint."""
        return self._stats[endpoint_tag].recommend_delay_s if endpoint_tag in self._stats else 0.0

    def maybe_flush(self, run_id: str) -> Optional[Dict[str, List[Dict]]]:
        now = time.time()
        if now - self._last_flush_ts < self._flush_interval_s:
            return None
        self._last_flush_ts = now
        snaps = [self._stats[tag].to_snapshot(tag, run_id) for tag in list(self._stats.keys())]
        return {"snapshots": snaps}




