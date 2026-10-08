"""Crop-water simulation engine: thin, strict wrapper around AquaCrop-OSPy.

Design rules
------------
* Every input is validated up front; anything missing or invalid raises
  ``EngineInputError`` (never filled with a default). AquaCrop itself swallows
  several bad inputs silently (gaps, NaN ET0, empty results), hence the checks.
* Only AquaCrop built-in crops and parameters are used (``supported_crops()``).
* Soil is built as an AquaCrop ``custom`` soil from the supplied layers. The
  curve number (61) and readily evaporable water (9 mm) are AquaCrop's own
  ``custom`` defaults: ASSUMPTION: library defaults until the owner fixes a
  texture-based criterion; they differ from AquaCrop's named-texture presets.
* The simulation starts on the planting day and covers at most one year, so
  exactly one season is simulated.

``run_aquacrop`` output keys
----------------------------
engine, engine_version, crop, planting_date (ISO), harvest_date (ISO),
irrigation ("rainfed"|"full"), yield_t_ha (dry yield, AquaCrop column
"Dry yield (tonne/ha)"), biomass_t_ha (last daily ``biomass`` g/m2 / 100),
seasonal_irrigation_mm (column
"Seasonal irrigation (mm)"), warnings (list[str]) and ``daily``: list of
{day, canopy_cover, biomass, biomass_ns, root_zone_water_mm, water_stress}.
Daily columns come from ``get_crop_growth()`` (``canopy_cover``, ``biomass``,
``biomass_ns``) and ``get_water_flux()`` (``Wr``). ``water_stress`` =
1 - biomass/biomass_ns clipped to [0, 1] (cumulative biomass deficit; AquaCrop
has no daily stress column), None while biomass_ns is 0.
"""
from __future__ import annotations

import asyncio
import math
import os
import warnings
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta
from importlib.metadata import version as _pkg_version
from multiprocessing import get_context
from typing import Optional

ENGINE_NAME = "AquaCrop-OSPy"
_MIN_ET0 = 0.1  # AquaCrop divides by ET0; its own file reader clips to 0.1
_COMPARTMENT_M = 0.1
_PENETRABILITY_PCT = 100  # no root-restricting layer information is available
_IRRIGATION_MODES = ("rainfed", "full")
# AquaCrop crop_params entries that are templates, not crops
_NON_CROPS = {"Default", "custom"}


class EngineInputError(ValueError):
    """Invalid or missing input; the message says what."""


class UnsupportedCropError(EngineInputError):
    """Crop is not an AquaCrop built-in."""


@dataclass(frozen=True)
class DailyWeather:
    day: date
    tmin_c: float
    tmax_c: float
    precip_mm: float
    et0_mm: float


@dataclass(frozen=True)
class SoilLayer:
    thickness_m: float
    wp: float  # m3/m3
    fc: float  # m3/m3
    sat: float  # m3/m3
    ksat_mm_day: float


def supported_crops() -> list[str]:
    """AquaCrop built-in crop names (templates excluded)."""
    from aquacrop.entities.crops.crop_params import crop_params

    return sorted(k for k in crop_params if k not in _NON_CROPS)


# ------------------------------------------------------------- validation
def _num(value, name: str, where: str) -> float:
    if value is None or isinstance(value, bool):
        raise EngineInputError(f"{name} is missing ({where})")
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise EngineInputError(f"{name} is not a number ({where}): {value!r}")
    if math.isnan(f) or math.isinf(f):
        raise EngineInputError(f"{name} is NaN/inf ({where})")
    return f


def _validate_weather(weather: list[DailyWeather]) -> tuple[list[DailyWeather], list[str]]:
    if not weather:
        raise EngineInputError("weather is empty")
    warns: list[str] = []
    out: list[DailyWeather] = []
    prev: Optional[date] = None
    for w in weather:
        if not isinstance(w.day, date):
            raise EngineInputError(f"weather day is not a date: {w.day!r}")
        where = str(w.day)
        if prev is not None:
            expected = prev + timedelta(days=1)
            if w.day != expected:
                if w.day > expected:
                    raise EngineInputError(f"weather gap: first missing day {expected}")
                raise EngineInputError(
                    f"weather days must be sorted and unique: {w.day} follows {prev}")
        tmin = _num(w.tmin_c, "tmin_c", where)
        tmax = _num(w.tmax_c, "tmax_c", where)
        precip = _num(w.precip_mm, "precip_mm", where)
        et0 = _num(w.et0_mm, "et0_mm", where)
        if tmax < tmin:
            raise EngineInputError(f"tmax_c < tmin_c on {where}")
        if precip < 0:
            raise EngineInputError(f"precip_mm < 0 on {where}")
        if et0 < 0 or 0 < et0 < _MIN_ET0:
            raise EngineInputError(f"et0_mm invalid on {where}: {et0} (must be 0 or >= {_MIN_ET0})")
        if et0 == 0.0:
            # ET0 = 0 raises ZeroDivisionError inside AquaCrop; clip like its own reader.
            et0 = _MIN_ET0
            warns.append(f"et0_mm=0 on {where} clipped to {_MIN_ET0}")
        out.append(DailyWeather(w.day, tmin, tmax, precip, et0))
        prev = w.day
    return out, warns


def _validate_soil(soil: list[SoilLayer]) -> None:
    if not soil:
        raise EngineInputError("soil needs at least one layer")
    for i, l in enumerate(soil):
        where = f"soil layer {i}"
        th = _num(l.thickness_m, "thickness_m", where)
        wp = _num(l.wp, "wp", where)
        fc = _num(l.fc, "fc", where)
        sat = _num(l.sat, "sat", where)
        ks = _num(l.ksat_mm_day, "ksat_mm_day", where)
        if th <= 0:
            raise EngineInputError(f"thickness_m must be > 0 ({where})")
        if not (0 < wp < fc < sat <= 1):
            raise EngineInputError(f"need 0 < wp < fc < sat <= 1 ({where}): {wp}, {fc}, {sat}")
        if ks <= 0:
            raise EngineInputError(f"ksat_mm_day must be > 0 ({where})")


def _validate_crop_irrigation(crop: str, irrigation: str) -> None:
    if crop not in supported_crops():
        raise UnsupportedCropError(f"crop {crop!r} is not an AquaCrop built-in")
    if irrigation not in _IRRIGATION_MODES:
        raise EngineInputError(f"irrigation must be one of {_IRRIGATION_MODES}, got {irrigation!r}")


# ----------------------------------------------------------------- build
def _compartments(soil: list[SoilLayer]) -> list[float]:
    dz: list[float] = []
    for l in soil:
        n = max(1, math.ceil(round(l.thickness_m / _COMPARTMENT_M, 6)))
        dz.extend([l.thickness_m / n] * n)
    return dz


def _build_soil(soil: list[SoilLayer]):
    from aquacrop import Soil

    # REW and curve number come from our top layer with AquaCrop's own rules
    # (adj_rew=0: REW from FC/dry water content; calc_cn=1: CN from Ksat), not
    # from the package's fixed custom-soil defaults (cn=61, rew=9).
    s = Soil("custom", dz=_compartments(soil), adj_rew=0, calc_cn=1)
    for l in soil:
        s.add_layer(l.thickness_m, l.wp, l.fc, l.sat, l.ksat_mm_day, _PENETRABILITY_PCT)
    return s


def _version() -> str:
    return _pkg_version("aquacrop")


# ------------------------------------------------------------------- run
def run_aquacrop(
    weather: list[DailyWeather],
    soil: list[SoilLayer],
    crop: str,
    planting_date: date,
    irrigation: str = "rainfed",
) -> dict:
    """Simulate one season. See module docstring for the output keys."""
    clean, warns = _validate_weather(weather)
    _validate_soil(soil)
    _validate_crop_irrigation(crop, irrigation)
    if not isinstance(planting_date, date):
        raise EngineInputError(f"planting_date is not a date: {planting_date!r}")
    if planting_date < clean[0].day:
        raise EngineInputError(
            f"planting_date {planting_date} is before the first weather day {clean[0].day}")
    last = clean[-1].day
    if planting_date > last:
        raise EngineInputError(f"planting_date {planting_date} is after the last weather day {last}")

    import pandas as pd
    from aquacrop import (AquaCropModel, Crop, InitialWaterContent,
                          IrrigationManagement)

    # AquaCrop reads weather columns by position: Date must be LAST (as in
    # aquacrop.utils.prepare_weather), otherwise temperatures become Timestamps.
    df = pd.DataFrame({
        "MinTemp": [w.tmin_c for w in clean],
        "MaxTemp": [w.tmax_c for w in clean],
        "Precipitation": [w.precip_mm for w in clean],
        "ReferenceET": [w.et0_mm for w in clean],
        "Date": pd.to_datetime([w.day for w in clean]),
    })
    sim_end = min(last, planting_date + timedelta(days=364))  # one season only
    irr = (IrrigationManagement(irrigation_method=0) if irrigation == "rainfed"
           else IrrigationManagement(irrigation_method=1, SMT=[100] * 4))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = AquaCropModel(
            planting_date.strftime("%Y/%m/%d"), sim_end.strftime("%Y/%m/%d"), df,
            _build_soil(soil), Crop(crop, planting_date=planting_date.strftime("%m/%d")),
            InitialWaterContent(value=["FC"]), irrigation_management=irr)
        model.run_model(till_termination=True)
        stats = model.get_simulation_results()
        if stats is False or len(stats) == 0:
            raise EngineInputError(
                f"weather ends before harvest (last weather day {last}, planting {planting_date})")
        growth = model.get_crop_growth()
        flux = model.get_water_flux()

    row = stats.iloc[0]
    return _assemble(row, growth, flux, planting_date, crop, irrigation, warns)


def _assemble(row, growth, flux, planting_date, crop, irrigation, warns) -> dict:
    import pandas as pd

    # Rows after termination are zero padding; real season days have dap > 0.
    # Row position i is simulation day i (simulation starts on planting day).
    daily = []
    for i in range(len(growth)):
        g = growth.iloc[i]
        if g["dap"] <= 0:
            continue
        bio, bio_ns = float(g["biomass"]), float(g["biomass_ns"])
        stress = None
        if bio_ns > 0:
            stress = min(1.0, max(0.0, 1.0 - bio / bio_ns))
        daily.append({
            "day": (planting_date + timedelta(days=i)).isoformat(),
            "canopy_cover": float(g["canopy_cover"]),
            "biomass": bio,  # g/m2
            "biomass_ns": bio_ns,  # g/m2, no-stress counterpart
            "root_zone_water_mm": float(flux.iloc[i]["Wr"]),
            "water_stress": stress,
        })
    harvest = pd.Timestamp(row["Harvest Date (YYYY/MM/DD)"]).date()
    return {
        "engine": ENGINE_NAME,
        "engine_version": _version(),
        "crop": crop,
        "planting_date": planting_date.isoformat(),
        "harvest_date": harvest.isoformat(),
        "irrigation": irrigation,
        "yield_t_ha": float(row["Dry yield (tonne/ha)"]),
        "biomass_t_ha": daily[-1]["biomass"] / 100.0 if daily else None,  # g/m2 -> t/ha
        "seasonal_irrigation_mm": float(row["Seasonal irrigation (mm)"]),
        "daily": daily,
        "warnings": list(warns),
    }


def run_aquacrop_with_potential(
    weather: list[DailyWeather],
    soil: list[SoilLayer],
    crop: str,
    planting_date: date,
) -> dict:
    """Rainfed run plus a full-irrigation run (SMT=100) as yield potential.

    AquaCrop's own "Yield potential" column is NOT water-unlimited, hence the
    second simulation. ``water_gap_pct`` = 100 * (1 - water_limited/potential).
    """
    wl = run_aquacrop(weather, soil, crop, planting_date, "rainfed")
    pot = run_aquacrop(weather, soil, crop, planting_date, "full")
    p = pot["yield_t_ha"]
    gap = 100.0 * (1.0 - wl["yield_t_ha"] / p) if p > 0 else None
    return {"water_limited": wl, "potential": pot, "water_gap_pct": gap}


# ----------------------------------------------------------- concurrency
_pool: Optional[ProcessPoolExecutor] = None


def _get_pool() -> ProcessPoolExecutor:
    """Lazily create the pool (importing this module never starts processes)."""
    global _pool
    if _pool is None:
        raw = os.environ.get("AQUACROP_MAX_WORKERS", "2")
        try:
            workers = int(raw)
        except ValueError:
            raise EngineInputError(f"AQUACROP_MAX_WORKERS is not an integer: {raw!r}")
        if workers < 1:
            raise EngineInputError("AQUACROP_MAX_WORKERS must be >= 1")
        # spawn: avoid forking a process that already runs an event loop/threads
        _pool = ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn"))
    return _pool


def _shutdown_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.shutdown(wait=True)
        _pool = None


async def run_aquacrop_async(
    weather: list[DailyWeather],
    soil: list[SoilLayer],
    crop: str,
    planting_date: date,
    irrigation: str = "rainfed",
) -> dict:
    """``run_aquacrop`` in a process pool (~0.6 s and ~78 MB per season)."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _get_pool(), run_aquacrop, weather, soil, crop, planting_date, irrigation)
