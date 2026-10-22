"""In-process TTL cache for per-parcel weather-map stats.

Weather stats change ~daily, but /extrapolate re-fetches them on every call —
comparing several crops on one parcel repeats the same slow HTTP round-trip,
which measured as the SLO-dominating cost (p95 ~666ms vs ~302ms without it).
Caching by (tenant, parcel) with a short TTL cuts that without new infra
(bioorch has no Redis). Fail-safe values (None) are never cached, so a transient
weather-map error is retried on the next call.
"""
import time
from typing import Any


class TTLCache:
    def __init__(self, ttl_seconds: float) -> None:
        self.ttl = ttl_seconds
        self._store: dict[Any, tuple[float, Any]] = {}

    def get(self, key: Any) -> Any | None:
        item = self._store.get(key)
        if item is None:
            return None
        ts, val = item
        if time.monotonic() - ts > self.ttl:
            self._store.pop(key, None)
            return None
        return val

    def set(self, key: Any, val: Any) -> None:
        self._store[key] = (time.monotonic(), val)

    def clear(self) -> None:
        self._store.clear()


# Per-parcel weather stats; 30-minute TTL (weather-map refreshes daily).
weather_stats_cache = TTLCache(1800)
