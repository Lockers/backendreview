import time
from typing import Any, Dict, List, Optional


class LearningTracker:
    """
    Tracks endpoint penalties and recoveries (e.g. 429s, WAF blocks).
    Persists batched snapshots periodically or after a threshold of OKs.
    """

    def __init__(self, flush_interval_s: int = 60, ok_flush_threshold: int = 100):
        self.snapshots: List[Dict[str, Any]] = []
        self.last_flush: float = time.time()
        self.flush_interval_s = flush_interval_s
        self.ok_flush_threshold = ok_flush_threshold
        self.ok_counter: int = 0

    def record_penalty(self, endpoint: str, category: str, penalty: float) -> None:
        self.snapshots.append(
            {
                "event": "penalty",
                "endpoint": endpoint,
                "category": category,
                "penalty": penalty,
                "ts": time.time(),
            }
        )

    def record_recovery(self, endpoint: str, category: str) -> None:
        self.snapshots.append(
            {
                "event": "recovery",
                "endpoint": endpoint,
                "category": category,
                "ts": time.time(),
            }
        )

    def record_ok(self, endpoint: str, category: str) -> None:
        # We don’t need endpoint/category in the OK snapshot; just increment counter.
        self.ok_counter += 1

    def maybe_flush(self, run_id: str, *, force: bool = False) -> Optional[Dict[str, Any]]:
        """
        Return a snapshot batch if it's time to flush.
        Includes penalties/recoveries and (if no issues) a baseline safe snapshot.
        If force=True, flush if we have any data or OKs even if thresholds not met.
        """
        now = time.time()
        should_flush = (
            force
            or (now - self.last_flush >= self.flush_interval_s)
            or (self.ok_counter >= self.ok_flush_threshold)
        )

        if not should_flush:
            return None

        snapshots_to_persist: List[Dict[str, Any]] = list(self.snapshots)

        if not snapshots_to_persist and self.ok_counter > 0:
            snapshots_to_persist.append(
                {
                    "event": "baseline_ok",
                    "ok_count": self.ok_counter,
                    "ts": now,
                }
            )

        if not snapshots_to_persist:
            return None

        snap = {
            "run_id": run_id,
            "ts": now,
            "snapshots": snapshots_to_persist,
        }

        self.snapshots.clear()
        self.ok_counter = 0
        self.last_flush = now
        return snap



