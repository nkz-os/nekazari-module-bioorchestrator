import asyncio
import logging
from datetime import date, timedelta

import httpx
import pytest

from app.planning import frost_history as fh

TODAY = date(2026, 11, 17)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    fh._CACHE.clear(); fh._FAILED.clear(); fh._LOADING.clear()
    monkeypatch.setattr(fh, "_today", lambda: TODAY)
    monkeypatch.setenv("HMAC_SECRET", "test-secret")
    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(fh.asyncio, "sleep", fake_sleep)
    yield sleeps
    fh._CACHE.clear(); fh._FAILED.clear(); fh._LOADING.clear()


def _body(validation=None, incomplete=None):
    b = {"days": [{"date": "1991-01-01", "tmin_c": -1.5, "tmax_c": 8.0},
                  {"date": "1991-01-02", "tmin_c": None, "tmax_c": None}],
         "validation": validation if validation is not None else {"tmin_c": [{"x": 1}], "tmax_c": "unvalidated_region"},
         "latest_available": "2026-11-16"}
    if incomplete:
        b["incomplete_years"] = incomplete
    return b


def _net(monkeypatch, responses):
    """Serve `responses` in order (the last repeats); returns the recorded requests."""
    calls: list[httpx.Request] = []

    def handler(req):
        calls.append(req)
        r = responses[min(len(calls) - 1, len(responses) - 1)]
        return r(req) if callable(r) else r

    real = httpx.AsyncClient
    monkeypatch.setattr(fh.httpx, "AsyncClient",
                        lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    return calls


async def test_load_ok_builds_days_and_caveat(monkeypatch):
    calls = _net(monkeypatch, [httpx.Response(200, json=_body())])
    await fh.schedule_frost_history("tenant-a", "p1")
    assert fh.frost_history_status("tenant-a", "p1") == "ready"
    h = fh.get_frost_history("tenant-a", "p1")
    assert h.tmin_caveat == "warm_bias"
    assert h.days[date(1991, 1, 1)] == (-1.5, 8.0)
    assert h.days[date(1991, 1, 2)] == (None, None)
    q = calls[0].url.params
    assert (q["resolution"], q["vars"], q["start"]) == ("daily", "tmin_c,tmax_c", "1991-01-01")
    assert q["end"] == (TODAY - timedelta(days=31)).isoformat()
    assert calls[0].url.path == "/api/weather/parcel/p1/history"
    assert "x-auth-signature" in calls[0].headers and calls[0].headers["x-tenant-id"] == "tenant-a"


async def test_unvalidated_region_caveat(monkeypatch):
    _net(monkeypatch, [httpx.Response(200, json=_body({"tmin_c": "unvalidated_region", "tmax_c": "unvalidated_region"}))])
    await fh.schedule_frost_history("tenant-a", "p1")
    assert fh.get_frost_history("tenant-a", "p1").tmin_caveat == "unvalidated_region"


async def test_incomplete_years_retries_after_retry_after(monkeypatch, _clean):
    calls = _net(monkeypatch, [httpx.Response(200, json=_body(incomplete=[1991]), headers={"Retry-After": "30"}),
                               httpx.Response(200, json=_body())])
    t = fh.schedule_frost_history("tenant-a", "p1")
    assert fh.frost_history_status("tenant-a", "p1") == "loading"
    await t
    assert len(calls) == 2 and _clean == [30]
    assert fh.frost_history_status("tenant-a", "p1") == "ready"


async def test_503_with_retry_after_retries_then_gives_up(monkeypatch, _clean):
    calls = _net(monkeypatch, [httpx.Response(503, headers={"Retry-After": "5"})])
    await fh.schedule_frost_history("tenant-a", "p1")
    assert len(calls) == fh.MAX_ATTEMPTS
    assert fh.frost_history_status("tenant-a", "p1") == "failed"
    assert fh.get_frost_history("tenant-a", "p1") is None


async def test_503_without_retry_after_fails_at_once(monkeypatch):
    calls = _net(monkeypatch, [httpx.Response(503)])
    await fh.schedule_frost_history("tenant-a", "p1")
    assert len(calls) == 1 and fh.frost_history_status("tenant-a", "p1") == "failed"


@pytest.mark.parametrize("code", [404, 422])
async def test_422_and_404_fail_without_retry(monkeypatch, caplog, code):
    calls = _net(monkeypatch, [httpx.Response(code)])
    with caplog.at_level(logging.WARNING):
        await fh.schedule_frost_history("tenant-a", "p1")
    assert len(calls) == 1 and fh.frost_history_status("tenant-a", "p1") == "failed"
    assert "tenant=tenant-a" in caplog.text and "parcel=p1" in caplog.text and f"status={code}" in caplog.text


async def test_failed_not_rescheduled_within_backoff(monkeypatch):
    _net(monkeypatch, [httpx.Response(422)])
    await fh.schedule_frost_history("tenant-a", "p1")
    assert fh.schedule_frost_history("tenant-a", "p1") is None
    t = fh._now() + fh.RETRY_BACKOFF_S + 1
    monkeypatch.setattr(fh, "_now", lambda: t)
    assert fh.schedule_frost_history("tenant-a", "p1") is not None


async def test_cache_is_per_tenant(monkeypatch):
    _net(monkeypatch, [httpx.Response(200, json=_body())])
    await fh.schedule_frost_history("tenant-a", "p1")
    assert fh.frost_history_status("tenant-a", "p1") == "ready"
    assert fh.frost_history_status("tenant-b", "p1") == "absent"
    assert fh.get_frost_history("tenant-b", "p1") is None


async def test_cache_bounded(monkeypatch):
    _net(monkeypatch, [httpx.Response(200, json=_body())])
    monkeypatch.setattr(fh, "MAX_ENTRIES", 2)
    for p in ("p1", "p2", "p3"):
        await fh.schedule_frost_history("tenant-a", p)
    assert fh.frost_history_status("tenant-a", "p1") == "absent"
    assert fh.frost_history_status("tenant-a", "p3") == "ready"


async def test_fresh_and_in_flight_not_rescheduled(monkeypatch):
    _net(monkeypatch, [httpx.Response(200, json=_body())])
    t = fh.schedule_frost_history("tenant-a", "p1")
    assert fh.schedule_frost_history("tenant-a", "p1") is None
    await t
    assert fh.schedule_frost_history("tenant-a", "p1") is None


@pytest.mark.parametrize("body", [{"days": []}, {"validation": {}}, ["x"]])
async def test_malformed_or_empty_body_fails(monkeypatch, body):
    _net(monkeypatch, [httpx.Response(200, json=body)])
    await fh.schedule_frost_history("tenant-a", "p1")
    assert fh.frost_history_status("tenant-a", "p1") == "failed"


async def test_still_incomplete_after_max_attempts_fails_not_cached(monkeypatch, caplog):
    calls = _net(monkeypatch, [httpx.Response(200, json=_body(incomplete=[1991]), headers={"Retry-After": "1"})])
    with caplog.at_level(logging.WARNING):
        await fh.schedule_frost_history("tenant-a", "p1")
    assert len(calls) == fh.MAX_ATTEMPTS
    assert fh.get_frost_history("tenant-a", "p1") is None
    assert fh.frost_history_status("tenant-a", "p1") == "failed" and "status=gave_up" in caplog.text


@pytest.mark.parametrize("raw,expected", [("9999", 300), ("inf", 30), ("abc", 30)])
async def test_retry_after_clamped_or_defaulted(monkeypatch, _clean, raw, expected):
    _net(monkeypatch, [httpx.Response(503, headers={"Retry-After": raw}), httpx.Response(200, json=_body())])
    await fh.schedule_frost_history("tenant-a", "p1")
    assert _clean == [expected]


async def test_gave_up_retries_sooner_than_other_failures(monkeypatch):
    _net(monkeypatch, [httpx.Response(503, headers={"Retry-After": "5"})])
    await fh.schedule_frost_history("tenant-a", "p1")  # gave_up
    _net(monkeypatch, [httpx.Response(422)])
    await fh.schedule_frost_history("tenant-a", "p2")  # hard failure
    t = fh._now() + fh.GAVE_UP_BACKOFF_S + 1
    monkeypatch.setattr(fh, "_now", lambda: t)
    assert fh.schedule_frost_history("tenant-a", "p2") is None
    assert fh.schedule_frost_history("tenant-a", "p1") is not None
    assert fh.GAVE_UP_BACKOFF_S < fh.RETRY_BACKOFF_S


async def test_requests_are_serialised(monkeypatch):
    active, peak = 0, 0

    def handler(req):
        return httpx.Response(200, json=_body())

    class Slow(httpx.AsyncClient):
        async def get(self, *a, **k):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return await super().get(*a, **k)

    monkeypatch.setattr(fh.httpx, "AsyncClient", lambda **kw: Slow(transport=httpx.MockTransport(handler), **kw))
    tasks = [fh.schedule_frost_history("tenant-a", f"p{i}") for i in range(3)]
    await asyncio.gather(*tasks)
    assert peak == 1
    assert all(fh.frost_history_status("tenant-a", f"p{i}") == "ready" for i in range(3))


async def test_retry_after_sleep_does_not_hold_the_slot(monkeypatch):
    gate = asyncio.Event()
    served: list[str] = []

    def handler(req):
        pid = req.url.path.split("/")[4]
        served.append(pid)
        if pid == "a" and served.count("a") == 1:
            return httpx.Response(503, headers={"Retry-After": "30"})
        return httpx.Response(200, json=_body())

    async def blocking_sleep(s):
        await gate.wait()  # parcel A "sleeps" until released

    real = httpx.AsyncClient
    monkeypatch.setattr(fh.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    monkeypatch.setattr(fh.asyncio, "sleep", blocking_sleep)
    ta = fh.schedule_frost_history("tenant-a", "a")
    tb = fh.schedule_frost_history("tenant-a", "b")
    await asyncio.wait_for(tb, 2)  # B completes while A is still asleep
    assert fh.frost_history_status("tenant-a", "b") == "ready"
    assert fh.frost_history_status("tenant-a", "a") == "loading"
    gate.set()
    await ta
    assert fh.frost_history_status("tenant-a", "a") == "ready"
