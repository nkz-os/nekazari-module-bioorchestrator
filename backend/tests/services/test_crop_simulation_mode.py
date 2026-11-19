"""run_crop_simulation: the water-loss mode takes exactly the forecast's inputs, only the engine call differs."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import AsyncMock

import pytest

from app.services import crop_simulation as cs
from app.services import water_loss_by_stage as wls
from app.services.engines import aquacrop_engine as aq
from app.services.sim_crops import resolve_aquacrop_crop
from app.services.sim_errors import SimulationError
from app.services.sim_weather import ObservedWeather

SUMMARY = {"horizons": [
    {"depthFrom": 0, "depthTo": 30, "fieldCapacity": 0.30, "wiltingPoint": 0.15,
     "saturation": 0.45, "ksatSaturated": 10.0},
    {"depthFrom": 30, "depthTo": 120, "fieldCapacity": 0.27, "wiltingPoint": 0.14,
     "saturation": 0.43, "ksatSaturated": 8.0}]}
SOWING = date(2025, 10, 20)
TODAY = date(2026, 3, 15)
LAT, LON = 42.55, -1.55
FORECAST = {"yield_t_ha": {"p10": 3.0, "p50": 4.0, "p90": 5.0}, "potential_yield_t_ha": 6.0,
            "water_gap_pct": 20.0, "harvest_date": "2026-06-28", "ensemble": {"n_years": 2},
            "biomass_t_ha": 9.0, "last_weather_day": "2026-03-14", "daily": [], "status": "in_season",
            "initial_water": {}, "field_management": {"mulch_pct": None, "f_mulch": None},
            "engine_version": "3.1.0", "warnings": ["engine warning"]}
WATER_LOSS = {"season": "in_season", "as_of": "2026-03-14", "ensemble": {"n_years": 2, "years": [2010, 2011]},
              "total": {"water_gap_pct": 20.0, "water_gap_band": None,
                        "water_limited_t_ha": {"p10": 3.0, "p50": 4.0, "p90": 5.0}, "potential_t_ha": 6.0},
              "stages": [{"index": 1, "key": "establishment", "start": "2025-10-20", "end": "2025-11-10",
                          "state": "complete", "gain_t_ha_model": 0.5, "gain_pct_of_potential": 8.0,
                          "end_in_relief_run": None}],
              "engine_version": "3.1.0", "warnings": ["stage warning"]}


def _year(y):
    return [aq.DailyWeather(date(y, 9, 1) + timedelta(days=i), 5.0, 15.0, 0.5, 2.0) for i in range(420)]


ANALOGS = {2010: _year(2010), 2011: _year(2011)}


def _observed():
    days = [aq.DailyWeather(date(2025, 9, 1) + timedelta(days=i), 5.0, 15.0, 0.0, 2.0)
            for i in range((date(2026, 3, 14) - date(2025, 9, 1)).days + 1)]
    return ObservedWeather(weather=days, sim_start=date(2025, 9, 1),
                           segments=[{"source": "parcel_daily", "from": "2025-09-01", "to": "2026-03-14"}],
                           warnings=["observed warning"])


@pytest.fixture
def stubs(monkeypatch):
    captured: dict = {}

    async def soil(parcel_id, tenant_id):
        return SUMMARY

    async def observed(parcel_id, tenant_id, lat, lon, sowing, today):
        return _observed()

    async def analogs(lat, lon, sowing, today):
        return ANALOGS, ["analog warning"]

    async def simulate(obs, analog_fn, soil_layers, crop, planting, irrigation, sim_start,
                       crop_params=None, mulch_pct=None, progress=None):
        captured["forecast"] = {"observed": obs, "analogs": dict(await analog_fn()), "soil": soil_layers,
                                "crop": crop, "planting": planting, "sim_start": sim_start,
                                "crop_params": crop_params, "mulch_pct": mulch_pct}
        return {**FORECAST, "field_management": aq._field_management(mulch_pct)}

    async def compute(obs, analog_fn, soil_layers, crop, planting, sim_start, crop_params, mulch_pct,
                      progress=None):
        captured["water_loss"] = {"observed": obs, "analogs": dict(await analog_fn()), "soil": soil_layers,
                                  "crop": crop, "planting": planting, "sim_start": sim_start,
                                  "crop_params": crop_params, "mulch_pct": mulch_pct}
        return WATER_LOSS

    monkeypatch.setattr(cs, "get_parcel_soil_summary", soil)
    monkeypatch.setattr(cs, "assemble_observed", observed)
    monkeypatch.setattr(cs, "assemble_analogs", analogs)
    monkeypatch.setattr(aq, "simulate", simulate)
    monkeypatch.setattr(wls, "compute", compute)
    return captured


async def _run(**over):
    kw = {"engine_name": "aquacrop", "parcel_id": "urn:ngsi-ld:AgriParcel:acme:p1", "tenant_id": "acme",
          "crop": resolve_aquacrop_crop("wheat"), "sowing_date": SOWING, "sowing_source": "crop_cycle",
          "irrigation": "rainfed", "lat": LAT, "lon": LON, "today": TODAY, "sowing_provenance": "actual",
          "sowing_method": "direct"}
    return await cs.run_crop_simulation(**{**kw, **over})


async def test_forecast_mode_unchanged(stubs):
    default = await _run()
    explicit = await _run(mode="forecast")
    assert default == explicit
    assert default["yield_t_ha"]["p50"] == pytest.approx(4.0 / (1 - default["yield_basis"]["grain_moisture_pct"] / 100))
    assert default["water_gap_pct"] == 20.0 and default["warnings"][-1] == "engine warning"
    assert "water_loss" not in default and "water_loss" not in stubs


async def test_water_loss_mode_same_inputs(stubs):
    forecast = await _run()
    wl = await _run(mode="water_loss_by_stage")
    assert stubs["forecast"] == stubs["water_loss"]
    assert stubs["water_loss"]["mulch_pct"] == 30.0 and set(stubs["water_loss"]["analogs"]) == {2010, 2011}
    assert wl["inputs"] == forecast["inputs"]
    assert wl["yield_basis"] == forecast["yield_basis"]
    assert wl["warnings"] == ["observed warning", "analog warning", "stage warning"]


async def test_water_loss_mode_converts_t_ha_to_the_yield_basis(stubs):
    wl = await _run(mode="water_loss_by_stage")
    basis = wl["yield_basis"]
    factor = 1.0 / (1.0 - basis["grain_moisture_pct"] / 100.0)
    assert basis["moisture"] == "commercial"
    total, stage = wl["water_loss"]["total"], wl["water_loss"]["stages"][0]
    assert total["potential_t_ha"] == pytest.approx(6.0 * factor)
    assert total["water_limited_t_ha"]["p50"] == pytest.approx(4.0 * factor)
    assert stage["gain_t_ha_model"] == pytest.approx(0.5 * factor)
    # percentages are not t/ha
    assert stage["gain_pct_of_potential"] == 8.0 and total["water_gap_pct"] == 20.0
    # the compute result is not mutated (it may be shared)
    assert WATER_LOSS["total"]["potential_t_ha"] == 6.0


async def test_water_loss_mode_milestones_for_cereals_only(stubs):
    wheat = await _run(mode="water_loss_by_stage")
    assert wheat["milestones"]["status"] == "ok" and "emergence" in wheat["milestones"]["items"]
    assert wheat["milestones"]["items"]["emergence"]["status"] == "reached"
    maize = await _run(mode="water_loss_by_stage", crop=resolve_aquacrop_crop("maize"))
    assert maize["milestones"] == {"status": "unavailable", "reason": "no_operative_calendar", "items": {}}


async def test_water_loss_mode_calendar_failure_is_additive(stubs, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("calendar down")
    monkeypatch.setattr(cs.cal, "effective_calendar", boom)
    wl = await _run(mode="water_loss_by_stage")
    assert wl["milestones"] == {"status": "unavailable", "reason": "calendar_failed", "items": {}}
    assert wl["water_loss"]["stages"]


@pytest.mark.parametrize("raised, code", [
    (aq.WeatherEndsBeforeHarvestError("x"), "season_incomplete"),
    (aq.CropCannotMatureError("x"), "crop_cannot_mature"),
    (aq.EngineInputError("x"), "invalid_input"),
])
async def test_water_loss_mode_maps_engine_errors_like_the_forecast(stubs, monkeypatch, raised, code):
    monkeypatch.setattr(wls, "compute", AsyncMock(side_effect=raised))
    with pytest.raises(SimulationError) as e:
        await _run(mode="water_loss_by_stage")
    assert e.value.code == code


async def test_unknown_mode_is_refused(stubs):
    with pytest.raises(SimulationError) as e:
        await _run(mode="nope")
    assert e.value.code == "invalid_input"
