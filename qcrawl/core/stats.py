import logging
import math
from collections import defaultdict
from datetime import datetime
from threading import RLock

logger = logging.getLogger(__name__)


class StatsCollector:
    """Thread-safe, synchronous statistics collector.

    A single flat, slash-namespaced key store with intent-revealing operations
    so the right call is always obvious:

      - `inc(key, count=1)` for monotonic counters (requests, retries, items),
      - `set(key, value)` / `max(key, value)` / `min(key, value)` for gauges
        (current levels and high/low-water marks),
      - `label(key, text)` for string metadata (spider name, finish reason),
      - `get(key, default)` / `snapshot()` to read.
    """

    def __init__(self) -> None:
        self._stats: dict[str, int | float | str] = defaultdict(int)
        self._lock = RLock()  # Reentrant lock: safe for nested calls
        self._start_time: datetime | None = None
        self._finish_time: datetime | None = None

    # Counters

    def inc(self, key: str, count: int = 1) -> None:
        """Increment a counter by `count` (thread-safe; non-numeric resets to 0)."""
        with self._lock:
            current = self._stats[key]
            if not isinstance(current, (int, float)):
                current = 0
            self._stats[key] = current + count

    # Gauges

    def set(self, key: str, value: int | float) -> None:
        """Set a numeric gauge (thread-safe)."""
        if not isinstance(value, (int, float)):
            raise TypeError("set accepts only int or float")
        with self._lock:
            self._stats[key] = value

    def max(self, key: str, value: int | float) -> None:
        """Record a high-water mark: keep the larger of the stored value and `value`."""
        if not isinstance(value, (int, float)):
            raise TypeError("max accepts only int or float")
        with self._lock:
            current = self._stats.get(key)
            if not isinstance(current, (int, float)) or value > current:
                self._stats[key] = value

    def min(self, key: str, value: int | float) -> None:
        """Record a low-water mark: keep the smaller of the stored value and `value`."""
        if not isinstance(value, (int, float)):
            raise TypeError("min accepts only int or float")
        with self._lock:
            current = self._stats.get(key)
            if not isinstance(current, (int, float)) or value < current:
                self._stats[key] = value

    # Labels / metadata

    def label(self, key: str, text: str) -> None:
        """Set string metadata (thread-safe)."""
        if not isinstance(text, str):
            raise TypeError("label accepts only str")
        with self._lock:
            self._stats[key] = text

    # Reads

    def get(self, key: str, default: int | float | str | None = None) -> int | float | str | None:
        """Get a value (thread-safe)."""
        with self._lock:
            return self._stats.get(key, default)

    def snapshot(self) -> dict[str, int | float | str]:
        """Return a copy of all stats."""
        with self._lock:
            return self._stats.copy()

    def open_spider(self, spider) -> None:
        """Called when spider opens."""
        start = datetime.now()
        with self._lock:
            self._start_time = start
            self.label("start_time", self._start_time.isoformat())
            self.label("spider_name", getattr(spider, "name", "unknown"))

    def close_spider(self, spider, reason: str = "finished") -> None:
        """Called when spider closes."""
        finish = datetime.now()
        with self._lock:
            self._finish_time = finish
            self.label("finish_time", self._finish_time.isoformat())
            self.label("finish_reason", reason)

            if self._start_time:
                elapsed = (self._finish_time - self._start_time).total_seconds()
                self.set("elapsed_time_seconds", elapsed)

    def log_stats(self) -> str:
        """Log collected stats in pretty format."""
        stats = self.snapshot()
        lines = []
        for key in sorted(stats):
            value = stats[key]
            if isinstance(value, float):
                value = f"{value:.6g}" if math.isfinite(value) else str(value)
            elif isinstance(value, int):
                value = f"{value:,}"
            else:
                value = str(value)
            lines.append(f"  {key}: {value}")
        return "\n".join(lines)
