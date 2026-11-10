"""Milestone dates of a sown cereal parcel, without the yield forecast.

Resolves the parcel, its current cycle, crop, sowing and regime with the same code as the
in-season forecast (``resolve_sown_crop``) but stops before the validation gate and never runs
the simulation engine: the dates need the crop, the sowing day, the place and the weather only.
The weather is assembled exactly as the simulation assembles it.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime

import httpx

from app.planning.in_season import (
    IRRIGATION,
    load_parcel,
    parcel_point,
    resolve_sown_crop,
    season_window_reason,
)
from app.planning.service import ParcelNotFound
from app.services import crop_calendar as cal
from app.services.crop_simulation import phenology_for
from app.services.sim_errors import SimulationError
from app.services.sim_weather import assemble_analogs, assemble_observed

logger = logging.getLogger(__name__)
PROVIDER_ID = "aquacrop_calendar"
REGIME_UNKNOWN = "regime_unknown"


def _unavailable(reason: str, code: str | None = None, state: str = "unavailable") -> dict:
    item = {"provider_id": PROVIDER_ID, "reason": reason}
    if code is not None:
        item["code"] = code
    return {"state": state, "unavailable": [item], "season": None, "phenology": None}


async def parcel_phenology(*, tenant_id: str, parcel_id: str, today: date | None = None,
                           load=load_parcel) -> dict:
    """Milestone dates for the parcel's current cycle, or why there are none. Never a 500."""
    today = today or datetime.now(UTC).date()
    ctx = f"parcel={parcel_id} tenant={tenant_id}"
    try:
        parcel, crops, cycles = await load(tenant_id, parcel_id)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise ParcelNotFound(parcel_id) from e
        code = f"http_{e.response.status_code}"
        logger.warning("parcel_phenology_parcel_unavailable %s code=%s", ctx, code)
        return _unavailable("parcel_unavailable", code, "failed")
    except httpx.HTTPError as e:
        code = type(e).__name__
        logger.warning("parcel_phenology_parcel_unavailable %s code=%s", ctx, code)
        return _unavailable("parcel_unavailable", code, "failed")

    try:
        sown = resolve_sown_crop(parcel, crops, cycles, today)
        if isinstance(sown, str):
            return _unavailable(sown)
        if sown.crop_slug not in cal.cereal_slugs():
            return _unavailable("unsupported_crop")
        reason = season_window_reason(sown.sowing_date, today)
        if reason is not None:
            return _unavailable(reason)
        point = parcel_point(parcel)
    except Exception as e:
        logger.exception("parcel_phenology_failed %s", ctx)
        return _unavailable("internal_error", type(e).__name__, "failed")
    if point is None:
        logger.warning("parcel_phenology_no_location %s", ctx)
        return _unavailable("weather_unavailable", "no_location")
    lat, lon = point

    try:
        observed = await assemble_observed(parcel_id, tenant_id, lat, lon, sown.sowing_date, today)
        analogs, _warnings = await assemble_analogs(lat, lon, sown.sowing_date, today)
    except (SimulationError, httpx.HTTPError) as e:
        code = e.code if isinstance(e, SimulationError) else type(e).__name__
        logger.warning("parcel_phenology_weather_unavailable %s code=%s", ctx, code)
        return _unavailable("weather_unavailable", code)
    except Exception as e:
        logger.exception("parcel_phenology_failed %s", ctx)
        return _unavailable("internal_error", type(e).__name__, "failed")

    try:
        # Unknown regime: the rainfed calendar, said in the notes (a fresh block per call).
        engine_regime = IRRIGATION[sown.regime or "rainfed"]
        block = await asyncio.to_thread(phenology_for, sown.crop_slug, engine_regime, lat, lon,
                                        sown.sowing_date, observed.weather, analogs)
    except Exception as e:
        logger.exception("parcel_phenology_failed %s", ctx)
        return _unavailable("internal_error", type(e).__name__, "failed")
    if sown.regime is None:
        block = {**block, "notes": [*block["notes"], REGIME_UNKNOWN]}
    logger.info("parcel_phenology_done %s crop=%s regime=%s", ctx, sown.crop_slug, sown.regime)
    return {"state": "done", "unavailable": [], "phenology": block,
            "season": {"crop": sown.eppo, "regime": sown.regime,
                       "sowing": {"date": sown.sowing_date.isoformat(), "source": "crop_cycle",
                                  "provenance": sown.sowing_provenance},
                       "as_of": observed.weather[-1].day.isoformat() if observed.weather else None,
                       "weather_source": "+".join(dict.fromkeys(s["source"] for s in observed.segments)) or None}}
