import logging
import math
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
    assert got.years == [2024, 2025]
    et0, precip = got.year_series(2024)
    assert (et0[0], et0[182], precip[0]) == (3.0, 3.0, 2.0)
    assert math.isnan(et0[1]) and math.isnan(precip[100])  # a missing day is NaN, not zero
    assert c.climatology_status(cell) == "ready"


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
    old = c.get_climatology("41.25:2.25")
    await c.schedule_climatology("41.25:2.25")
    assert m.await_count == 2
    assert c.get_climatology("41.25:2.25") is not old  # replaced by the reload


def test_cell_key_negative_longitudes():
    assert c.cell_key(41.3, -3.7) == "41.25:-3.75"
    assert c.cell_key(0.05, -0.1) == "0.00:0.00"


async def test_lru_eviction_at_the_cap(monkeypatch):
    _fake(monkeypatch, return_value=_fetched())
    monkeypatch.setattr(c, "CLIMATOLOGY_MAX_CELLS", 2)
    for cell in ("1.00:1.00", "2.00:2.00"):
        await c.schedule_climatology(cell)
    assert c.get_climatology("1.00:1.00") is not None  # a use: 2.00 is now the oldest
    await c.schedule_climatology("3.00:3.00")
    assert c.climatology_status("2.00:2.00") == "absent"
    assert c.climatology_status("1.00:1.00") == c.climatology_status("3.00:3.00") == "ready"


async def test_stale_is_served_while_reloading_and_after_a_failed_reload(monkeypatch, caplog):
    m = _fake(monkeypatch, return_value=_fetched())
    await c.schedule_climatology("41.25:2.25")
    first = c.get_climatology("41.25:2.25")
    t = c._now() + c.TTL_S + 1
    monkeypatch.setattr(c, "_now", lambda: t)
    m.side_effect = SimulationError("archive_unavailable", "x", 503)
    task = c.schedule_climatology("41.25:2.25")
    assert task is not None and c.get_climatology("41.25:2.25") is first  # stale while loading
    assert c.climatology_status("41.25:2.25") == "ready"
    with caplog.at_level(logging.WARNING):
        await task
    assert c.get_climatology("41.25:2.25") is first
    assert c.climatology_status("41.25:2.25") == "ready"
    assert "stale_kept=True" in caplog.text


async def test_status_values(monkeypatch):
    assert c.climatology_status("9.00:9.00") == "absent"
    m = _fake(monkeypatch, return_value=_fetched())
    task = c.schedule_climatology("9.00:9.00")
    assert c.climatology_status("9.00:9.00") == "loading"
    await task
    assert c.climatology_status("9.00:9.00") == "ready"
    m.side_effect = SimulationError("archive_unavailable", "x", 503)
    await c.schedule_climatology("8.00:8.00")
    assert c.climatology_status("8.00:8.00") == "failed"


async def test_invalid_years_config_is_a_logged_failure(monkeypatch, caplog):
    m = _fake(monkeypatch, return_value=_fetched())
    monkeypatch.setenv("CLIMATOLOGY_YEARS", "abc")
    with caplog.at_level(logging.WARNING):
        await c.schedule_climatology("41.25:2.25")
    assert c.climatology_status("41.25:2.25") == "failed"
    assert "climatology load failed" in caplog.text
    m.assert_not_awaited()
