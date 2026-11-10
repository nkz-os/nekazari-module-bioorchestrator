"""/planning/parcels/{id}/phenology: milestone dates without the yield forecast gate or the engine."""
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from app.planning import phenology_dates
from app.planning.service import ParcelNotFound
from app.services import crop_simulation
from app.services.engines import aquacrop_engine as aq
from app.services.sim_errors import SimulationError
from app.services.sim_weather import ObservedWeather

TODAY = date(2026, 3, 15)
PARCEL_ID = "urn:ngsi-ld:AgriParcel:t:p1"
PARCEL = {"id": PARCEL_ID, "irrigationRegime": "rainfed",
          "location": {"type": "Polygon", "coordinates": [[[-1.6, 42.8], [-1.5, 42.8], [-1.5, 42.9],
                                                          [-1.6, 42.9], [-1.6, 42.8]]]}}
CROP_ID = "urn:ngsi-ld:AgriCrop:t:c1"
CROPS = {CROP_ID: {"id": CROP_ID, "species": "TRZAX", "irrigationRegime": "rainfed"}}


def sown(species="TRZAX", provenance="actual", day="2025-10-20"):
    return {"current": {"crop_id": CROP_ID, "species": species, "start": {"date": day, "provenance": provenance}}}


def loader(cycles=None, crops=CROPS, parcel=PARCEL):
    async def load(tenant, parcel_id):
        return parcel, crops, sown() if cycles is None else cycles
    return load


def _days(start, end, tmin=4.0, tmax=14.0):
    out, d = [], start
    while d <= end:
        out.append(aq.DailyWeather(d, tmin, tmax, 1.0, 2.0))
        d += timedelta(days=1)
    return out


@pytest.fixture(autouse=True)
def weather(monkeypatch):
    observed = _days(date(2024, 10, 20), date(2026, 3, 14))
    monkeypatch.setattr(phenology_dates, "assemble_observed", AsyncMock(return_value=ObservedWeather(
        weather=observed, sim_start=observed[0].day, segments=[{"source": "parcel_daily", "from": "x", "to": "y"}])))
    analog = _days(date(2005, 1, 1), date(2025, 12, 31), 5.0, 18.0)
    monkeypatch.setattr(phenology_dates, "assemble_analogs",
                        AsyncMock(return_value=({y: analog for y in range(2010, 2024)}, [])))


@pytest.fixture(autouse=True)
def no_engine(monkeypatch):
    boom = AsyncMock(side_effect=AssertionError("engine must not run"))
    monkeypatch.setattr(aq, "simulate", boom)
    monkeypatch.setattr(crop_simulation, "run_crop_simulation", boom)
    return boom


async def call(load=None, **over):
    kw = {"tenant_id": "t", "parcel_id": PARCEL_ID, "today": TODAY, "load": load or loader()}
    return await phenology_dates.parcel_phenology(**{**kw, **over})


async def test_done_answers_milestones_without_the_engine(no_engine):
    out = await call()
    assert out["state"] == "done" and out["unavailable"] == []
    assert set(out["phenology"]["milestones"]) == {"emergence", "flowering_start", "senescence_start", "maturity"}
    assert out["phenology"]["milestones"]["emergence"]["status"] == "reached"
    assert out["phenology"]["milestones"]["maturity"]["status"] == "projected"
    assert out["phenology"]["notes"] == ["no_stress_delay"]
    assert out["season"] == {"crop": "TRZAX", "regime": "rainfed",
                             "sowing": {"date": "2025-10-20", "source": "crop_cycle", "provenance": "actual"},
                             "as_of": "2026-03-14", "weather_source": "parcel_daily"}
    assert not no_engine.called


@pytest.mark.parametrize("species,regime", [("HORVX", "rainfed"), ("TRZAX", "irrigated")])
async def test_a_combination_the_forecast_gate_refuses_still_answers(species, regime):
    crops = {CROP_ID: {"id": CROP_ID, "species": species, "irrigationRegime": regime}}
    out = await call(load=loader(cycles=sown(species), crops=crops, parcel={**PARCEL, "irrigationRegime": regime}))
    assert out["state"] == "done" and out["season"]["regime"] == regime


async def test_unknown_regime_uses_the_rainfed_calendar_and_says_so():
    from app.services import crop_calendar as cal
    parcel = {k: v for k, v in PARCEL.items() if k != "irrigationRegime"}
    crops = {CROP_ID: {"id": CROP_ID, "species": "TRZAX"}}
    before = cal.effective_calendar("wheat", lat=42.85, lon=-1.55, irrigation="rainfed")
    out = await call(load=loader(crops=crops, parcel=parcel))
    assert out["state"] == "done" and out["season"]["regime"] is None
    assert out["phenology"]["notes"] == ["no_stress_delay", "regime_unknown"]
    assert (await call())["phenology"]["notes"] == ["no_stress_delay"]
    assert cal.effective_calendar("wheat", lat=42.85, lon=-1.55, irrigation="rainfed") == before


@pytest.mark.parametrize("cycles,reason", [
    ({"current": None, "pending": True}, "not_sown"),
    (sown(provenance="planned"), "not_sown"),
    ({"current": None}, "no_committed_crop"),
    (None, "cycles_unavailable"),
    (sown(day="not-a-date"), "no_sowing_date"),
    (sown(day="2026-04-01"), "sowing_in_future"),
    (sown(day="2024-01-01"), "season_too_old"),
    (sown(species="ZZZZZ"), "unknown_crop"),
    (sown(species="ZEAMX"), "unsupported_crop"),
])
async def test_unavailable_reasons(cycles, reason):
    async def load(tenant, parcel_id):
        return PARCEL, CROPS, cycles
    out = await call(load=load)
    assert out["state"] == "unavailable" and out["phenology"] is None and out["season"] is None
    assert out["unavailable"] == [{"provider_id": "aquacrop_calendar", "reason": reason}]


@pytest.mark.parametrize("failure", [SimulationError("weather_gaps", "m"), httpx.ConnectError("down")])
async def test_weather_that_cannot_be_assembled_is_unavailable(monkeypatch, failure):
    monkeypatch.setattr(phenology_dates, "assemble_observed", AsyncMock(side_effect=failure))
    out = await call()
    assert out["state"] == "unavailable" and out["phenology"] is None
    assert out["unavailable"][0]["reason"] == "weather_unavailable"


async def test_analog_weather_failing_is_unavailable_too(monkeypatch):
    monkeypatch.setattr(phenology_dates, "assemble_analogs",
                        AsyncMock(side_effect=SimulationError("archive_not_configured", "m", 503)))
    out = await call()
    assert out["unavailable"] == [{"provider_id": "aquacrop_calendar", "reason": "weather_unavailable",
                                   "code": "archive_not_configured"}]


async def test_missing_parcel_is_parcel_not_found():
    async def load(tenant, parcel_id):
        raise httpx.HTTPStatusError("nf", request=httpx.Request("GET", "http://x"), response=httpx.Response(404))
    with pytest.raises(ParcelNotFound):
        await call(load=load)


async def test_unreachable_broker_fails_with_a_code():
    async def load(tenant, parcel_id):
        raise httpx.ConnectError("down")
    out = await call(load=load)
    assert out["state"] == "failed"
    assert out["unavailable"] == [{"provider_id": "aquacrop_calendar", "reason": "parcel_unavailable",
                                   "code": "ConnectError"}]


async def test_an_unexpected_error_fails_with_a_code_and_is_logged(monkeypatch, caplog):
    monkeypatch.setattr(phenology_dates.cal, "phenology_block", MagicMock(side_effect=RuntimeError("x")))
    out = await call()
    assert out["state"] == "failed" and out["phenology"] is None
    assert out["unavailable"] == [{"provider_id": "aquacrop_calendar", "reason": "internal_error",
                                   "code": "RuntimeError"}]
    assert PARCEL_ID in caplog.text and "tenant=t" in caplog.text


URL = f"/api/graph/planning/parcels/{PARCEL_ID}/phenology"


@pytest.fixture
def client(monkeypatch):
    from fastapi.testclient import TestClient

    from app.main import app
    from neo4j import AsyncDriver
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    monkeypatch.setattr("app.core.knowledge.get_driver", lambda: MagicMock(spec=AsyncDriver))
    return TestClient(app)


def test_route_requires_a_verified_tenant(client, monkeypatch):
    import app.api.v1.planning as planning_api
    monkeypatch.setenv("AUTH_DISABLED", "false")
    called = AsyncMock()
    monkeypatch.setattr(planning_api, "parcel_phenology", called)
    assert client.get(URL).status_code == 401 and not called.called


def test_route_answers_the_contract(client, monkeypatch):
    import app.api.v1.planning as planning_api
    monkeypatch.setattr(planning_api, "parcel_phenology", AsyncMock(return_value={
        "state": "unavailable", "unavailable": [{"provider_id": "aquacrop_calendar", "reason": "not_sown"}],
        "season": None, "phenology": None}))
    r = client.get(URL)
    assert r.status_code == 200
    assert r.json() == {"contract_version": "2.8", "provider_id": "aquacrop_calendar", "state": "unavailable",
                        "unavailable": [{"provider_id": "aquacrop_calendar", "reason": "not_sown", "code": None}],
                        "season": None, "phenology": None}


def test_route_404_for_unknown_parcel(client, monkeypatch):
    import app.api.v1.planning as planning_api
    monkeypatch.setattr(planning_api, "parcel_phenology", AsyncMock(side_effect=ParcelNotFound("x")))
    assert client.get(URL).status_code == 404


async def test_a_malformed_geometry_fails_with_a_code_and_is_logged(caplog):
    parcel = {**PARCEL, "location": {"type": "Polygon", "coordinates": "garbage"}}
    out = await call(load=loader(parcel=parcel))
    assert out["state"] in ("failed", "unavailable") and out["phenology"] is None
    assert PARCEL_ID in caplog.text and "tenant=t" in caplog.text


async def test_resolution_that_raises_fails_with_a_code(monkeypatch, caplog):
    monkeypatch.setattr(phenology_dates, "resolve_sown_crop", MagicMock(side_effect=ValueError("x")))
    out = await call()
    assert out["state"] == "failed"
    assert out["unavailable"] == [{"provider_id": "aquacrop_calendar", "reason": "internal_error",
                                   "code": "ValueError"}]
    assert PARCEL_ID in caplog.text and "tenant=t" in caplog.text


async def test_no_observed_segments_means_no_weather_source(monkeypatch):
    observed = _days(date(2024, 10, 20), date(2026, 3, 14))
    monkeypatch.setattr(phenology_dates, "assemble_observed", AsyncMock(return_value=ObservedWeather(
        weather=observed, sim_start=observed[0].day, segments=[])))
    assert (await call())["season"]["weather_source"] is None
