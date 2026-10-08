"""Daily weather assembly for the crop simulation: parcel series + archive, no invention."""
from __future__ import annotations

import asyncio
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services import sim_weather as sw
from app.services.sim_errors import SimulationError

PARCEL = "urn:ngsi-ld:AgriParcel:p1"
TENANT = "t1"
LAT, LON = 42.5, -1.6


def _day(d, **kw):
    base = {"date": d.isoformat(), "tmin_c": 3.0, "tmax_c": 12.0, "precip_mm": 1.0, "et0_mm": 1.5}
    base.update(kw)
    return base


def _parcel_payload(start, end, missing=(), **kw):
    days, d = [], start
    while d <= end:
        if d not in missing:
            days.append(_day(d, **kw))
        d += timedelta(days=1)
    return {"parcel_id": PARCEL, "days": days, "missing_days": [m.isoformat() for m in missing]}


def _archive_payload(start, end, nulls=(), **kw):
    t, d = [], start
    while d <= end:
        t.append(d)
        d += timedelta(days=1)
    v = {"temperature_2m_min": 2.0, "temperature_2m_max": 11.0,
         "precipitation_sum": 0.5, "et0_fao_evapotranspiration": 1.2}
    v.update(kw)
    daily = {"time": [x.isoformat() for x in t]}
    for k, val in v.items():
        daily[k] = [None if x in nulls else val for x in t]
    return {"daily": daily}


class _Net:
    """Fake httpx.AsyncClient: routes by URL, records calls."""

    def __init__(self, parcel=None, archive=None, parcel_status=200, archive_status=200,
                 raises=None):
        self.parcel, self.archive = parcel, archive
        self.parcel_status, self.archive_status, self.raises = parcel_status, archive_status, raises
        self.calls: list[tuple[str, dict, dict]] = []

    def __call__(self, *a, **k):
        net = self

        async def get(url, params=None, headers=None):
            net.calls.append((url, dict(params or {}), dict(headers or {})))
            if net.raises:
                raise net.raises
            resp = MagicMock()
            if "/api/weather/parcel/" in url:
                resp.status_code = net.parcel_status
                resp.json.return_value = net.parcel(params) if callable(net.parcel) else net.parcel
            else:
                resp.status_code = net.archive_status
                resp.json.return_value = net.archive(params) if callable(net.archive) else net.archive
            return resp

        client = MagicMock()
        client.get = get
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=client)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("WEATHER_API_URL", "http://weather.test")
    monkeypatch.setenv("OPENMETEO_ARCHIVE_URL", "http://archive.test")
    monkeypatch.delenv("CLIMATOLOGY_YEARS", raising=False)


def _parcel_range(params):
    return date.fromisoformat(params["start"]), date.fromisoformat(params["end"])


def _run(coro):
    return asyncio.run(coro)


# --------------------------------------------------------- parcel + archive
def test_parcel_series_covering_everything_needs_no_archive():
    planting, today = date(2026, 3, 1), date(2026, 3, 20)
    net = _Net(parcel=lambda p: _parcel_payload(*_parcel_range(p)))
    with patch("httpx.AsyncClient", net):
        res = _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert res.weather[0].day == planting - timedelta(days=365)
    assert res.weather[-1].day == today - timedelta(days=1)
    assert res.sim_start == res.weather[0].day
    assert [s["source"] for s in res.segments] == ["parcel_daily"]
    assert all("archive.test" not in c[0] for c in net.calls)
    assert sw.ARCHIVE_ALTITUDE_WARNING not in res.warnings
    # service-to-service identity headers
    assert net.calls[0][2]["X-Tenant-ID"] == TENANT and "X-User-ID" in net.calls[0][2]
    # 380 days in two requests of <= 400 days
    assert all((_parcel_range(c[1])[1] - _parcel_range(c[1])[0]).days < 400 for c in net.calls)


def test_leading_days_come_from_archive_and_are_tagged():
    planting, today = date(2026, 3, 1), date(2026, 3, 20)
    first_parcel = date(2025, 12, 1)

    def parcel(p):
        s, e = _parcel_range(p)
        s = max(s, first_parcel)
        return _parcel_payload(s, e) if s <= e else _parcel_payload(e, e - timedelta(days=1))

    net = _Net(parcel=parcel, archive=lambda p: _archive_payload(
        date.fromisoformat(p["start_date"]), date.fromisoformat(p["end_date"])))
    with patch("httpx.AsyncClient", net):
        res = _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert [s["source"] for s in res.segments] == ["archive", "parcel_daily"]
    assert res.segments[0]["end"] == (first_parcel - timedelta(days=1)).isoformat()
    assert res.segments[1]["start"] == first_parcel.isoformat()
    arch = next(c for c in net.calls if "archive.test" in c[0])
    assert arch[0].endswith("/v1/archive")
    assert arch[1]["latitude"] == LAT and arch[1]["longitude"] == LON
    assert arch[1]["timezone"] == "UTC"
    assert "et0_fao_evapotranspiration" in arch[1]["daily"]
    # archive value mapping
    assert res.weather[0].tmin_c == 2.0 and res.weather[0].et0_mm == 1.2
    assert sw.ARCHIVE_ALTITUDE_WARNING in res.warnings


def test_leading_days_without_archive_configured_is_503(monkeypatch):
    monkeypatch.setenv("OPENMETEO_ARCHIVE_URL", "")
    planting, today = date(2026, 3, 1), date(2026, 3, 20)
    net = _Net(parcel=lambda p: _parcel_payload(max(_parcel_range(p)[0], date(2026, 1, 1)),
                                                _parcel_range(p)[1]))
    with patch("httpx.AsyncClient", net), pytest.raises(SimulationError) as ei:
        _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert ei.value.code == "archive_not_configured" and ei.value.status_code == 503


def test_interior_gap_not_filled_is_weather_gaps():
    planting, today = date(2026, 3, 1), date(2026, 3, 20)
    hole = {date(2026, 3, 5), date(2026, 3, 6)}
    net = _Net(parcel=lambda p: _parcel_payload(*_parcel_range(p), missing=hole),
               archive=lambda p: _archive_payload(
                   date.fromisoformat(p["start_date"]), date.fromisoformat(p["end_date"]),
                   nulls=hole))
    with patch("httpx.AsyncClient", net), pytest.raises(SimulationError) as ei:
        _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert ei.value.code == "weather_gaps" and ei.value.status_code == 422
    assert "2026-03-05" in ei.value.message and "2026-03-06" in ei.value.message


def test_null_variable_counts_as_missing_not_zero():
    planting, today = date(2026, 3, 1), date(2026, 3, 10)

    def parcel(p):
        out = _parcel_payload(*_parcel_range(p))
        for d in out["days"]:
            if d["date"] == "2026-03-04":
                d["et0_mm"] = None
        return out

    net = _Net(parcel=parcel, archive=lambda p: _archive_payload(
        date.fromisoformat(p["start_date"]), date.fromisoformat(p["end_date"]),
        nulls={date(2026, 3, 4)}))
    with patch("httpx.AsyncClient", net), pytest.raises(SimulationError) as ei:
        _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert ei.value.code == "weather_gaps" and "2026-03-04" in ei.value.message


def test_trailing_missing_days_trim_the_series():
    planting, today = date(2026, 3, 1), date(2026, 3, 20)
    trail = {date(2026, 3, 18), date(2026, 3, 19)}
    net = _Net(parcel=lambda p: _parcel_payload(*_parcel_range(p), missing=trail))
    with patch("httpx.AsyncClient", net):
        res = _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert res.weather[-1].day == date(2026, 3, 17)


def test_empty_parcel_series_uses_archive_trimmed_to_availability():
    planting, today = date(2026, 3, 1), date(2026, 3, 20)
    last_arch = date(2026, 3, 14)
    net = _Net(parcel=lambda p: _parcel_payload(date(2026, 1, 2), date(2026, 1, 1)),
               archive=lambda p: _archive_payload(
                   date.fromisoformat(p["start_date"]), date.fromisoformat(p["end_date"]),
                   nulls={date(2026, 3, 15) + timedelta(days=i) for i in range(10)}))
    with patch("httpx.AsyncClient", net):
        res = _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert [s["source"] for s in res.segments] == ["archive"]
    assert res.weather[-1].day == last_arch


def test_history_starts_at_first_available_day_when_shorter_than_a_year():
    planting, today = date(2026, 3, 1), date(2026, 3, 20)
    first = date(2025, 11, 10)
    net = _Net(parcel=lambda p: _parcel_payload(max(first, _parcel_range(p)[0]), _parcel_range(p)[1]),
               archive=lambda p: _archive_payload(
                   date.fromisoformat(p["start_date"]), date.fromisoformat(p["end_date"]),
                   nulls={first - timedelta(days=i) for i in range(1, 400)}))
    with patch("httpx.AsyncClient", net):
        res = _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert res.sim_start == first


def test_planting_without_weather_is_weather_gaps():
    planting, today = date(2026, 3, 1), date(2026, 3, 1)  # nothing before today yet
    net = _Net(parcel=lambda p: _parcel_payload(date(2025, 3, 1), date(2026, 2, 27)))
    with patch("httpx.AsyncClient", net), pytest.raises(SimulationError) as ei:
        _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert ei.value.code == "weather_gaps"


def test_season_end_caps_observed_range():
    planting, today = date(2024, 3, 1), date(2026, 3, 20)
    net = _Net(parcel=lambda p: _parcel_payload(*_parcel_range(p)))
    with patch("httpx.AsyncClient", net):
        res = _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert res.weather[-1].day == planting + timedelta(days=364)


@pytest.mark.parametrize("net", [
    _Net(raises=RuntimeError("down")),
    _Net(parcel={}, parcel_status=503),
    _Net(parcel={}, parcel_status=400),
])
def test_parcel_service_unavailable_is_503(net):
    with patch("httpx.AsyncClient", net), pytest.raises(SimulationError) as ei:
        _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, date(2026, 3, 1), date(2026, 3, 20)))
    assert ei.value.code == "weather_unavailable" and ei.value.status_code == 503


def test_archive_unavailable_is_503():
    net = _Net(parcel=lambda p: _parcel_payload(date(2026, 1, 1), date(2026, 1, 1)),
               archive={}, archive_status=500)
    with patch("httpx.AsyncClient", net), pytest.raises(SimulationError) as ei:
        _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, date(2026, 3, 1), date(2026, 3, 20)))
    assert ei.value.code == "archive_unavailable" and ei.value.status_code == 503


# ------------------------------------------------------------- climatology
def test_climatology_years_default_and_env(monkeypatch):
    assert sw.climatology_years() == 15
    monkeypatch.setenv("CLIMATOLOGY_YEARS", "7")
    assert sw.climatology_years() == 7
    monkeypatch.setenv("CLIMATOLOGY_YEARS", "x")
    with pytest.raises(SimulationError) as ei:
        sw.climatology_years()
    assert ei.value.code == "config_error"


def test_analog_years_are_last_complete_years_for_season_span():
    # spring sowing: season within one calendar year
    assert sw.analog_year_candidates(date(2026, 3, 1), date(2026, 10, 8), 3) == [2023, 2024, 2025]
    # autumn sowing: season ends next year -> that year must be complete
    assert sw.analog_year_candidates(date(2025, 10, 1), date(2026, 10, 8), 3) == [2022, 2023, 2024]


def test_assemble_analogs_fetches_archive_and_drops_bad_years():
    planting, today = date(2026, 3, 1), date(2026, 10, 8)
    bad_day = date(2024, 6, 1)

    def archive(p):
        return _archive_payload(date.fromisoformat(p["start_date"]), date.fromisoformat(p["end_date"]),
                                nulls={bad_day})

    net = _Net(archive=archive)
    with patch("httpx.AsyncClient", net):
        analogs, warns = _run(sw.assemble_analogs(LAT, LON, planting, today, years=4))
    assert sorted(analogs) == [2022, 2023, 2025]
    assert any("2024" in w and "2024-06-01" in w for w in warns)
    arch_calls = [c for c in net.calls if "archive.test" in c[0]]
    assert len(arch_calls) == 1
    # analog-year planting day of the oldest year .. last campaign day of the newest
    assert arch_calls[0][1]["start_date"] == "2022-03-01"
    assert arch_calls[0][1]["end_date"] == "2026-02-28"


def test_assemble_analogs_requires_archive(monkeypatch):
    monkeypatch.setenv("OPENMETEO_ARCHIVE_URL", "")
    with pytest.raises(SimulationError) as ei:
        _run(sw.assemble_analogs(LAT, LON, date(2026, 3, 1), date(2026, 10, 8), years=3))
    assert ei.value.code == "archive_not_configured"


def test_assemble_analogs_warns_when_fewer_years_than_requested():
    planting, today = date(2026, 3, 1), date(2026, 10, 8)
    net = _Net(archive=lambda p: _archive_payload(
        date.fromisoformat(p["start_date"]), date.fromisoformat(p["end_date"]),
        nulls={date(2023, 1, 1) + timedelta(days=i) for i in range(366)}))
    with patch("httpx.AsyncClient", net):
        analogs, warns = _run(sw.assemble_analogs(LAT, LON, planting, today, years=3))
    assert sorted(analogs) == [2024, 2025]
    assert any("2023" in w for w in warns)


def test_small_positive_et0_is_raised_and_declared():
    planting, today = date(2026, 3, 1), date(2026, 3, 10)

    def parcel(p):
        out = _parcel_payload(*_parcel_range(p))
        for d in out["days"]:
            if d["date"] == "2026-03-04":
                d["et0_mm"] = 0.04
        return out

    with patch("httpx.AsyncClient", _Net(parcel=parcel)):
        res = _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    got = next(w for w in res.weather if w.day == date(2026, 3, 4))
    assert got.et0_mm == 0.1
    assert any("2026-03-04" in w and "ET0" in w for w in res.warnings)


def test_invalid_day_is_not_usable_and_becomes_gap():
    planting, today = date(2026, 3, 1), date(2026, 3, 10)

    def parcel(p):
        out = _parcel_payload(*_parcel_range(p))
        for d in out["days"]:
            if d["date"] == "2026-03-04":
                d["precip_mm"] = -2.0
        return out

    with patch("httpx.AsyncClient", _Net(parcel=parcel, archive=lambda p: _archive_payload(
            date.fromisoformat(p["start_date"]), date.fromisoformat(p["end_date"]),
            nulls={date(2026, 3, 4)}))), pytest.raises(SimulationError) as ei:
        _run(sw.assemble_observed(PARCEL, TENANT, LAT, LON, planting, today))
    assert ei.value.code == "weather_gaps"

