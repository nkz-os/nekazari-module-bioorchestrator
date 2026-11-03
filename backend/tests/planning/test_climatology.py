import logging
from datetime import date
from unittest.mock import AsyncMock

import pytest

from app.planning import climatology as c
from app.services.engines.aquacrop_engine import DailyWeather
from app.services.sim_errors import SimulationError
from app.services.sim_weather import _Fetched


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    c._CACHE.clear(); c._FAILED.clear(); c._LOADING.clear()
    monkeypatch.setattr(c, "_today", lambda: date(2026, 11, 3))
    monkeypatch.setenv("CLIMATOLOGY_YEARS", "2")
    yield
    c._CACHE.clear(); c._FAILED.clear(); c._LOADING.clear()


def _fetched():
    f = _Fetched()
    for y in (2024, 2025):
        for d in (date(y, 1, 1), date(y, 7, 1)):
            f.days[d] = DailyWeather(d, 5.0, 15.0, 2.0, 3.0)
    return f


def _fake(monkeypatch, **kw):
    m = AsyncMock(**kw)
    monkeypatch.setattr(c.sim_weather, "fetch_archive_daily", m)
    return m


def test_cell_key_snaps_to_quarter_degree():
    assert c.cell_key(41.30, 2.15) == c.cell_key(41.37, 2.20) == "41.25:2.25"
    assert c.cell_key(41.30, 2.15) != c.cell_key(41.60, 2.15)


async def test_cold_returns_none_and_one_load_serves_two_parcels(monkeypatch):
    m = _fake(monkeypatch, return_value=_fetched())
    cell = c.cell_key(41.30, 2.15)
    assert c.get_climatology(cell) is None
    t1 = c.schedule_climatology(cell)
    assert c.schedule_climatology(c.cell_key(41.37, 2.20)) is None  # same cell, in flight
    await t1
    assert c.schedule_climatology(cell) is None  # cached
    m.assert_awaited_once()
    assert m.await_args.args == (41.25, 2.25, date(2024, 1, 1), date(2025, 12, 31))
    got = c.get_climatology(cell)
    assert got["years"] == [2024, 2025]
    assert got["daily"][2024] == [{"doy": 1, "et0": 3.0, "precip": 2.0}, {"doy": 183, "et0": 3.0, "precip": 2.0}]


async def test_failure_returns_none_logs_and_backs_off(monkeypatch, caplog):
    m = _fake(monkeypatch, side_effect=SimulationError("archive_unavailable", "x", 503))
    with caplog.at_level(logging.WARNING):
        await c.schedule_climatology("41.25:2.25")
    assert c.get_climatology("41.25:2.25") is None
    assert "cell=41.25:2.25" in caplog.text and "[2024, 2025]" in caplog.text
    assert c.schedule_climatology("41.25:2.25") is None  # backoff
    m.assert_awaited_once()
    t = c._now() + c.RETRY_BACKOFF_S + 1
    monkeypatch.setattr(c, "_now", lambda: t)
    m.side_effect = None; m.return_value = _fetched()
    await c.schedule_climatology("41.25:2.25")
    assert m.await_count == 2


async def test_ttl_expiry_reloads(monkeypatch):
    m = _fake(monkeypatch, return_value=_fetched())
    await c.schedule_climatology("41.25:2.25")
    t = c._now() + c.TTL_S + 1
    monkeypatch.setattr(c, "_now", lambda: t)
    assert c.get_climatology("41.25:2.25") is None
    await c.schedule_climatology("41.25:2.25")
    assert m.await_count == 2
