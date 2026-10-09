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
* The simulation covers at most one year after planting, so exactly one season
  is simulated. By default it starts on the planting day with the soil at field
  capacity. With ``sim_start`` before planting (at least ``MIN_SPINUP_DAYS``
  earlier) AquaCrop runs the fallow period first (``off_season=True``) from field
  capacity at ``sim_start``, so the planting-day soil water comes from real
  weather. The response always declares which one was used (``initial_water``).
  The spin-up never reaches back to last year's planting anniversary (AquaCrop
  would plant there), so the longest spin-up is 364 days.

``run_aquacrop`` output keys
----------------------------
engine, engine_version, crop, planting_date (ISO), harvest_date (ISO),
initial_water ({method: "spinup"|"assumed_fc", spinup_days, start}), irrigation ("rainfed"|"full"), yield_t_ha (dry yield, AquaCrop column
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
import statistics
import warnings
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta
from importlib.metadata import version as _pkg_version
from multiprocessing import get_context
from typing import Any

import numpy as np

ENGINE_NAME = "AquaCrop-OSPy"
MIN_ET0 = 0.1  # AquaCrop divides by ET0; its own file reader clips to 0.1
_MIN_ET0 = MIN_ET0
_COMPARTMENT_M = 0.1
_PENETRABILITY_PCT = 100  # no root-restricting layer information is available
_IRRIGATION_MODES = ("rainfed", "full")
MIN_SPINUP_DAYS = 30  # shorter fallow histories do not say anything about soil water
# AquaCrop crop_params entries that are templates, not crops
_NON_CROPS = {"Default", "custom"}


class EngineInputError(ValueError):
    """Invalid or missing input; the message says what."""


class UnsupportedCropError(EngineInputError):
    """Crop is not an AquaCrop built-in."""


class WeatherEndsBeforeHarvestError(EngineInputError):
    """The weather series stops before the crop reaches harvest."""


class CropCannotMatureError(EngineInputError):
    """At this site and sowing date the crop needs more than one season to mature."""


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
    prev: date | None = None
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


def check_weather_day(w: DailyWeather) -> None:
    """Raise EngineInputError if a single day would be rejected by the engine."""
    _validate_weather([w])


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
    sim_start: date | None = None,
    topsoil_until: date | None = None,
) -> dict:
    """Simulate one season. See module docstring for the output keys.

    ``sim_start`` (optional, <= planting date, >= first weather day) requests a
    spin-up. Fewer than ``MIN_SPINUP_DAYS`` of history falls back to field
    capacity on the planting day, with a warning.

    ``topsoil_until`` adds ``topsoil_water``: ISO day -> volumetric water content
    of the first soil compartment (top 10 cm) for every simulated day up to that
    date (the fallow before planting when it precedes the planting date).
    """
    clean, warns = _validate_weather(weather)
    _validate_soil(soil)
    _validate_crop_irrigation(crop, irrigation)
    if not isinstance(planting_date, date):
        raise EngineInputError(f"planting_date is not a date: {planting_date!r}")
    if planting_date < clean[0].day:
        raise EngineInputError(
            f"planting_date {planting_date} is before the first weather day {clean[0].day}")
    if (planting_date.month, planting_date.day) == (2, 29):
        # AquaCrop-OSPy takes the planting day as "mm/dd" and parses it in a
        # non-leap year, so 29 February cannot be represented.
        raise EngineInputError("planting_date 29 February is not supported by AquaCrop-OSPy")
    last = clean[-1].day
    if planting_date > last:
        raise EngineInputError(f"planting_date {planting_date} is after the last weather day {last}")
    start = planting_date
    if sim_start is not None:
        if not isinstance(sim_start, date):
            raise EngineInputError(f"sim_start is not a date: {sim_start!r}")
        if sim_start > planting_date:
            raise EngineInputError(f"sim_start {sim_start} is after planting_date {planting_date}")
        if sim_start < clean[0].day:
            raise EngineInputError(
                f"sim_start {sim_start} is before the first weather day {clean[0].day}")
        if (planting_date - sim_start).days >= MIN_SPINUP_DAYS:
            start = sim_start
        else:
            warns.append(
                f"spin-up shorter than {MIN_SPINUP_DAYS} days: initial soil water assumed at "
                "field capacity on the planting day")
    if start < planting_date:
        # AquaCrop plants on the first "mm/dd" on or after the simulation start, so a
        # start on (or before) last year's planting anniversary would plant a year early.
        try:
            prev = planting_date.replace(year=planting_date.year - 1)
        except ValueError:  # Feb 29
            prev = date(planting_date.year - 1, 2, 28)
        if start <= prev:
            start = prev + timedelta(days=1)
    spinup_days = (planting_date - start).days
    initial_water = {
        "method": "spinup" if spinup_days else "assumed_fc",
        "spinup_days": spinup_days,
        "start": start.isoformat(),
    }

    import pandas as pd
    from aquacrop import AquaCropModel, Crop, InitialWaterContent, IrrigationManagement

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
            start.strftime("%Y/%m/%d"), sim_end.strftime("%Y/%m/%d"), df,
            _build_soil(soil), Crop(crop, planting_date=planting_date.strftime("%m/%d")),
            InitialWaterContent(value=["FC"]), irrigation_management=irr,
            off_season=spinup_days > 0)
        try:
            model.run_model(till_termination=True)
        except AssertionError as e:
            # GDD crops: AquaCrop asserts (instead of returning no result) when the
            # weather up to the end of the run has too few degree days to mature.
            if "longer than 1 year to mature" in str(e):
                raise CropCannotMatureError(
                    f"crop {crop} does not reach maturity within one season when sown on "
                    f"{planting_date}: too few degree days at this site") from e
            if "not enough growing degree days" not in str(e):
                raise
            if sim_end < last:
                # The weather goes on past the one-season window: it is the site, not the series.
                raise CropCannotMatureError(
                    f"crop {crop} does not reach maturity within one season when sown on "
                    f"{planting_date}: {e}") from e
            raise WeatherEndsBeforeHarvestError(
                f"weather ends before harvest: {e} (last weather day {last}, "
                f"planting {planting_date})") from e
        stats = model.get_simulation_results()
        if stats is False or len(stats) == 0:
            raise WeatherEndsBeforeHarvestError(
                f"weather ends before harvest (last weather day {last}, planting {planting_date})")
        growth = model.get_crop_growth()
        flux = model.get_water_flux()

    row = stats.iloc[0]
    out = _assemble(row, growth, flux, start, planting_date, crop, irrigation, warns, initial_water)
    if topsoil_until is not None:
        storage = model.get_water_storage()
        out["topsoil_water"] = {
            (start + timedelta(days=i)).isoformat(): float(storage.iloc[i]["th1"])
            for i in range(min(len(storage), (topsoil_until - start).days + 1))
        }
    return out


def _assemble(row, growth, flux, start, planting_date, crop, irrigation, warns, initial_water) -> dict:
    import pandas as pd

    # Rows after termination are zero padding; real season days have dap > 0.
    # Row position i is simulation day i (``start``: planting day or spin-up start).
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
            "day": (start + timedelta(days=i)).isoformat(),
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
        "initial_water": initial_water,
        "irrigation": irrigation,
        "yield_t_ha": float(row["Dry yield (tonne/ha)"]),
        "biomass_t_ha": daily[-1]["biomass"] / 100.0 if daily else None,  # g/m2 -> t/ha
        "seasonal_irrigation_mm": float(row["Seasonal irrigation (mm)"]),
        "daily": daily,
        "warnings": list(warns),
    }


def fallow_topsoil_water(
    weather: list[DailyWeather],
    soil: list[SoilLayer],
    crop: str,
    start: date,
    end: date,
    sim_start: date | None = None,
) -> dict[date, float]:
    """Top-10-cm water content (m3/m3) of the bare soil on each day of [start, end].

    Runs the season with the crop planted the day after ``end`` (two days after
    when that is 29 February), so every day of the window is fallow; spin-up from ``sim_start`` as in ``run_aquacrop``.
    Raises the same errors as ``run_aquacrop``.
    """
    plant = end + timedelta(days=1)
    if (plant.month, plant.day) == (2, 29):  # not plantable; the window stays fallow anyway
        plant += timedelta(days=1)
    res = run_aquacrop(weather, soil, crop, plant, "rainfed", sim_start, topsoil_until=end)
    out = {}
    d = start
    while d <= end:
        v = res["topsoil_water"].get(d.isoformat())
        if v is not None:
            out[d] = v
        d += timedelta(days=1)
    return out


def run_aquacrop_with_potential(
    weather: list[DailyWeather],
    soil: list[SoilLayer],
    crop: str,
    planting_date: date,
    sim_start: date | None = None,
) -> dict:
    """Rainfed run plus a full-irrigation run (SMT=100) as yield potential.

    AquaCrop's own "Yield potential" column is NOT water-unlimited, hence the
    second simulation. ``water_gap_pct`` = 100 * (1 - water_limited/potential).
    """
    wl = run_aquacrop(weather, soil, crop, planting_date, "rainfed", sim_start)
    pot = run_aquacrop(weather, soil, crop, planting_date, "full", sim_start)
    p = pot["yield_t_ha"]
    gap = 100.0 * (1.0 - wl["yield_t_ha"] / p) if p > 0 else None
    return {"water_limited": wl, "potential": pot, "water_gap_pct": gap}


# ----------------------------------------------------------- concurrency
_pool: ProcessPoolExecutor | None = None


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


# -------------------------------------------------------------- ensemble
# Capabilities of this engine; a future engine (e.g. WOFOST 8.1) returns the
# same response shape and declares its own flags.
CAPABILITIES = {"water_limited": True, "potential": True, "nitrogen": False}
# ASSUMPTION: three members is the smallest set for which P10/P50/P90 are three
# different members; below it the spread is not an estimate of anything.
MIN_ENSEMBLE_MEMBERS = 3
_DAILY_FIELDS = ("canopy_cover", "biomass", "root_zone_water_mm", "water_stress")


def analog_date(campaign_day: date, planting_year: int, analog_year: int) -> date:
    """Calendar-equivalent day of ``campaign_day`` in the analog season.

    The analog season starts on the planting date in ``analog_year``; a campaign
    day in the following calendar year maps to the following analog year. Feb 29
    of a campaign has no counterpart in a non-leap analog year and takes Feb 28
    (duplicated); a leap-year Feb 29 of the analog is skipped when the campaign
    has none.
    """
    year = analog_year + (campaign_day.year - planting_year)
    try:
        return campaign_day.replace(year=year)
    except ValueError:  # Feb 29 -> non-leap year
        return date(year, 2, 28)


def project_weather(
    observed: list[DailyWeather],
    analog: list[DailyWeather],
    planting_date: date,
    analog_year: int,
) -> list[DailyWeather]:
    """Observed weather, then ``analog`` weather re-dated to the campaign.

    Covers up to ``planting_date + 364 d``. Raises EngineInputError naming the
    first analog day that is missing.
    """
    if not observed:
        raise EngineInputError("observed weather is empty")
    by_day = {w.day: w for w in analog}
    end = planting_date + timedelta(days=364)
    out = list(observed)
    d = observed[-1].day + timedelta(days=1)
    while d <= end:
        src_day = analog_date(d, planting_date.year, analog_year)
        src = by_day.get(src_day)
        if src is None:
            raise EngineInputError(f"analog year {analog_year} has no weather for {src_day}")
        out.append(DailyWeather(d, src.tmin_c, src.tmax_c, src.precip_mm, src.et0_mm))
        d += timedelta(days=1)
    return out


def _pct(values: list[float]) -> dict:
    """P10/P50/P90 by linear interpolation between order statistics (numpy default)."""
    p10, p50, p90 = np.percentile(values, [10, 50, 90])
    return {"p10": float(p10), "p50": float(p50), "p90": float(p90)}


def _median_daily(members: list[dict], last_observed: date) -> list[dict]:
    """Observed days (identical in every member) then the per-day median.

    From the day after ``last_observed`` each field is the median over the
    members that still have that day (a member stops at its harvest); None
    values are ignored. Rows are flagged ``projected``.
    """
    rows = [{**r, "projected": False} for r in members[0]["daily"]
            if date.fromisoformat(r["day"]) <= last_observed]
    by_day: dict[str, list[dict]] = {}
    for m in members:
        for r in m["daily"]:
            if date.fromisoformat(r["day"]) > last_observed:
                by_day.setdefault(r["day"], []).append(r)
    for day in sorted(by_day):
        row: dict[str, Any] = {"day": day}
        for f in _DAILY_FIELDS:
            vals = [r[f] for r in by_day[day] if r[f] is not None]
            row[f] = float(statistics.median(vals)) if vals else None
        row["projected"] = True
        rows.append(row)
    return rows


def _combine(members: list[dict], observed_last: date, irrigation: str, status: str) -> dict:
    """Fold per-member (water-limited, potential) runs into the response body."""
    pick = "potential" if irrigation == "full" else "water_limited"
    n = len(members)
    wl = [m["water_limited"] for m in members]
    pot = [m["potential"] for m in members]
    chosen = [m[pick] for m in members]
    if status == "complete":
        main = chosen[0]
        yield_v: Any = main["yield_t_ha"]
        pot_v: Any = pot[0]["yield_t_ha"]
        harvest = main["harvest_date"]
        biomass = main["biomass_t_ha"]
        daily = [{**r, "projected": False} for r in main["daily"]]
        ens = None
        wl_ref, pot_ref = wl[0]["yield_t_ha"], pot[0]["yield_t_ha"]
    else:
        yield_v = _pct([c["yield_t_ha"] for c in chosen])
        pot_v = _pct([p["yield_t_ha"] for p in pot])
        harvest = date.fromordinal(statistics.median_low(
            [date.fromisoformat(c["harvest_date"]).toordinal() for c in chosen])).isoformat()
        bio = [c["biomass_t_ha"] for c in chosen if c["biomass_t_ha"] is not None]
        biomass = float(statistics.median(bio)) if bio else None
        daily = _median_daily(chosen, observed_last)
        ens = {"n_years": n, "method": "climatological_ensemble"}
        wl_ref = statistics.median([w["yield_t_ha"] for w in wl])
        pot_ref = statistics.median([p["yield_t_ha"] for p in pot])
    gap = 100.0 * (1.0 - wl_ref / pot_ref) if pot_ref > 0 else None
    warns: list[str] = []
    for m in members[:1]:  # observed-part warnings are identical in every member
        warns.extend(m["water_limited"]["warnings"])
    return {
        "engine": ENGINE_NAME,
        "engine_version": _version(),
        "capabilities": dict(CAPABILITIES),
        "irrigation": irrigation,
        "status": status,
        "initial_water": members[0]["water_limited"]["initial_water"],
        "yield_t_ha": yield_v,
        "potential_yield_t_ha": pot_v,
        "water_gap_pct": gap,
        "harvest_date": harvest,
        "ensemble": ens,
        "biomass_t_ha": biomass,
        "last_weather_day": observed_last.isoformat(),
        "daily": daily,
        "warnings": warns,
    }


async def simulate(
    observed: list[DailyWeather],
    analogs: dict[int, list[DailyWeather]] | Callable[[], Awaitable[dict[int, list[DailyWeather]]]],
    soil: list[SoilLayer],
    crop: str,
    planting_date: date,
    irrigation: str = "rainfed",
    sim_start: date | None = None,
) -> dict:
    """Run the season: single run if ``observed`` reaches harvest, else ensemble.

    ``analogs`` maps analog year -> that year's daily weather (archive). With
    observed weather ending before harvest, one member per analog year is run
    (water-limited and potential), observed weather up to the last observed
    day followed by the analog year's weather mapped onto the campaign dates.
    ``yield_t_ha`` follows ``irrigation`` (rainfed: water-limited run, full:
    irrigated run); ``potential_yield_t_ha`` is always the irrigated run and
    ``water_gap_pct`` always compares rainfed with it (median-based for the
    ensemble). Percentiles are P10/P50/P90 by linear interpolation.
    Members run in the process pool. ``analogs`` may be an async callable, awaited
    only when the ensemble is actually needed (so a fully observed season never
    loads climatology).
    """
    _validate_crop_irrigation(crop, irrigation)
    loop = asyncio.get_running_loop()
    pool = _get_pool()
    last = observed[-1].day if observed else None
    try:
        single = await loop.run_in_executor(
            pool, run_aquacrop_with_potential, observed, soil, crop, planting_date, sim_start)
    except WeatherEndsBeforeHarvestError:
        if last >= planting_date + timedelta(days=364):
            raise
        if callable(analogs):
            analogs = await analogs()
        if not analogs:
            raise
    else:
        return _combine([single], last, irrigation, "complete")
    if len(analogs) < MIN_ENSEMBLE_MEMBERS:
        raise EngineInputError(
            f"climatological ensemble needs >= {MIN_ENSEMBLE_MEMBERS} analog years, got {len(analogs)}")
    years = sorted(analogs)
    futs = [
        loop.run_in_executor(
            pool, run_aquacrop_with_potential,
            project_weather(observed, analogs[y], planting_date, y), soil, crop, planting_date, sim_start)
        for y in years
    ]
    members = list(await asyncio.gather(*futs))
    out = _combine(members, last, irrigation, "in_season")
    out["ensemble"]["years"] = years
    return out
