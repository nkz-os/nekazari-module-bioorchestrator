"""Crop simulation orchestration: inputs -> engine -> response.

Engines are interchangeable behind ``CropEngine``: each maps a species to its
own crop model, runs the season and returns the same result shape, declaring
what it can do in ``capabilities``. Only AquaCrop is implemented.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Protocol

from app.services.engines import aquacrop_engine as aq
from app.services.sim_crops import CropChoice, resolve_aquacrop_crop
from app.services.sim_errors import SimulationError
from app.services.sim_inputs import SimInputError, soil_layers_from_summary
from app.services.sim_weather import assemble_analogs, assemble_observed
from app.services.soil_client import SoilSummaryError, get_parcel_soil_summary

logger = logging.getLogger(__name__)

IRRIGATION_MODES = ("rainfed", "full")


class CropEngine(Protocol):
    name: str
    capabilities: dict[str, bool]

    def resolve_crop(self, identifier: str) -> CropChoice: ...

    async def simulate(
        self, *, observed, analogs, soil, crop: str, planting: date,
        irrigation: str, sim_start: date | None,
    ) -> dict[str, Any]: ...


class AquaCropEngine:
    name = "aquacrop"
    capabilities = aq.CAPABILITIES

    def resolve_crop(self, identifier: str) -> CropChoice:
        return resolve_aquacrop_crop(identifier)

    async def simulate(self, *, observed, analogs, soil, crop, planting, irrigation, sim_start):
        try:
            return await aq.simulate(observed, analogs, soil, crop, planting, irrigation, sim_start)
        except aq.WeatherEndsBeforeHarvestError as e:
            raise SimulationError("season_incomplete", str(e)) from e
        except aq.CropCannotMatureError as e:
            raise SimulationError("crop_cannot_mature", str(e)) from e
        except aq.EngineInputError as e:
            raise SimulationError("invalid_input", str(e)) from e


ENGINES: dict[str, CropEngine] = {"aquacrop": AquaCropEngine()}


def get_engine(name: str) -> CropEngine:
    engine = ENGINES.get(name)
    if engine is None:
        raise SimulationError(
            "unsupported_engine", f"Unknown engine '{name}'. Available: {', '.join(sorted(ENGINES))}.")
    return engine


def check_irrigation(irrigation: str) -> str:
    if irrigation not in IRRIGATION_MODES:
        raise SimulationError(
            "invalid_irrigation", f"irrigation must be one of: {', '.join(IRRIGATION_MODES)}.")
    return irrigation


def engine_soil(layers: list[dict]) -> list[aq.SoilLayer]:
    """Simulation layers -> engine layers; saturation is mandatory (no texture guess)."""
    missing = [f"{l['top_cm']:g}-{l['bottom_cm']:g} cm" for l in layers if l.get("sat") is None]
    if missing:
        raise SimulationError(
            "soil_incomplete", f"Soil saturation is missing for horizon(s): {', '.join(missing)}.")
    return [aq.SoilLayer(l["thickness_m"], l["wp"], l["fc"], l["sat"], l["ksat_mm_day"]) for l in layers]


async def run_crop_simulation(
    *,
    engine_name: str,
    parcel_id: str,
    tenant_id: str,
    crop: CropChoice,
    sowing_date: date,
    sowing_source: str,
    irrigation: str,
    lat: float,
    lon: float,
    today: date,
) -> dict[str, Any]:
    """Gather soil and weather, run the engine, build the response.

    Raises SimulationError (422 inputs, 503 upstream); never substitutes data.
    """
    engine = get_engine(engine_name)
    ctx = f"parcel={parcel_id} tenant={tenant_id} engine={engine_name} crop={crop.slug}"
    if sowing_date > today:
        raise SimulationError(
            "sowing_date_future", f"The sowing date {sowing_date.isoformat()} is in the future.")

    try:
        summary = await get_parcel_soil_summary(parcel_id, tenant_id)
        layers = soil_layers_from_summary(summary)
    except SoilSummaryError as e:
        logger.warning("crop_simulation_failed %s code=soil_unavailable error=%s", ctx, e)
        raise SimulationError("soil_unavailable", f"Soil data unavailable: {e}", 503) from e
    except SimInputError as e:
        logger.warning("crop_simulation_failed %s code=soil_incomplete error=%s", ctx, e)
        raise SimulationError("soil_incomplete", f"Soil data incomplete: {e}") from e
    soil = engine_soil(layers)

    observed = await assemble_observed(parcel_id, tenant_id, lat, lon, sowing_date, today)

    analog_warnings: list[str] = []

    async def analogs():
        found, warns = await assemble_analogs(lat, lon, sowing_date, today)
        analog_warnings.extend(warns)
        return found

    try:
        result = await engine.simulate(
            observed=observed.weather, analogs=analogs, soil=soil, crop=crop.aquacrop_crop,
            planting=sowing_date, irrigation=irrigation, sim_start=observed.sim_start)
    except SimulationError as e:
        logger.warning("crop_simulation_failed %s code=%s", ctx, e.code)
        raise

    first, last = observed.weather[0].day, observed.weather[-1].day
    warnings = [*observed.warnings, *analog_warnings, *result["warnings"]]
    out = {
        "engine": engine.name,
        "engine_version": result["engine_version"],
        "capabilities": dict(engine.capabilities),
        "crop_slug": crop.slug,
        "aquacrop_crop": crop.aquacrop_crop,
        "parcel_id": parcel_id,
        "sowing_date": sowing_date.isoformat(),
        "irrigation": irrigation,
        "initial_water": result["initial_water"],
        "status": result["status"],
        "yield_t_ha": result["yield_t_ha"],
        "potential_yield_t_ha": result["potential_yield_t_ha"],
        "water_gap_pct": result["water_gap_pct"],
        "harvest_date": result["harvest_date"],
        "ensemble": result["ensemble"],
        "biomass_t_ha": result["biomass_t_ha"],
        "last_weather_day": result["last_weather_day"],
        "daily": result["daily"],
        "inputs": {
            "weather": {
                "source": "+".join(dict.fromkeys(s["source"] for s in observed.segments)),
                "start": first.isoformat(),
                "end": last.isoformat(),
                "days": len(observed.weather),
                "segments": observed.segments,
            },
            "soil": {"layers": layers},
            "sowing": {"source": sowing_source},
        },
        "warnings": warnings,
    }
    if crop.note:
        out["crop_parameters_note"] = crop.note
    logger.info(
        "crop_simulation_done %s status=%s n_years=%s weather_days=%d",
        ctx, out["status"], (out["ensemble"] or {}).get("n_years"), len(observed.weather))
    return out
