import pytest

from app.services.weather_stats_cache import TTLCache, weather_stats_cache
from app.graph.dao import GraphDAO


def test_ttl_cache_hit_and_miss():
    c = TTLCache(100)
    assert c.get("k") is None          # miss
    c.set("k", {"v": 1})
    assert c.get("k") == {"v": 1}      # hit


def test_ttl_cache_expires():
    c = TTLCache(0)                    # everything is immediately stale
    c.set("k", {"v": 1})
    assert c.get("k") is None


# ── integration: fetch_parcel_weather_stats caches successful reads ──────────

class _Resp:
    status_code = 200

    def json(self):
        return {"metrics": {"temperature_avg": {"heat_stress_pct": 10}}}


class _Client:
    calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, *a, **k):
        _Client.calls += 1
        return _Resp()


@pytest.fixture(autouse=True)
def _reset():
    weather_stats_cache.clear()
    _Client.calls = 0


@pytest.mark.asyncio
async def test_fetch_weather_stats_second_call_is_cached(monkeypatch):
    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _Client())

    first = await GraphDAO.fetch_parcel_weather_stats("urn:p1", "montiko")
    second = await GraphDAO.fetch_parcel_weather_stats("urn:p1", "montiko")

    assert first == {"temperature_avg": {"heat_stress_pct": 10}}
    assert second == first
    assert _Client.calls == 1  # second served from cache, no HTTP round-trip
