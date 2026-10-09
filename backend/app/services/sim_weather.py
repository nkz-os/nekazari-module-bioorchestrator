"""Daily weather for the crop simulation: parcel series first, archive for the rest.

Sources, per day (never mixed within a day, never interpolated, never defaulted):
  * ``parcel_daily``: weather-api ``/api/weather/parcel/{id}/daily`` (altitude
    corrected parcel series).
  * ``archive``: self-hosted Open-Meteo archive (``OPENMETEO_ARCHIVE_URL``), used
    for days before the parcel series existed (spin-up, early campaign), for
    holes inside it, and for the climatological analog years.

A day is usable only if tmin, tmax, precipitation and ET0 are all present.
Unfilled days between the first and last usable day are an error
(``weather_gaps``); days after the last usable day are simply "not published yet"
and end the observed series.
"""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import httpx

from app.services.engines.aquacrop_engine import (
    MIN_ET0,
    DailyWeather,
    EngineInputError,
    analog_date,
    check_weather_day,
)
from app.services.sim_errors import SimulationError
from app.services.soil_headers import soil_module_headers

logger = logging.getLogger(__name__)

SPINUP_DAYS = 365  # fallow history requested before sowing (spec D2)
SEASON_DAYS = 365  # sowing day + 364 days, one season
_PARCEL_MAX_DAYS = 400  # weather-api limit per request
_MAX_LISTED_DAYS = 20
_ARCHIVE_DAILY = "temperature_2m_min,temperature_2m_max,precipitation_sum,et0_fao_evapotranspiration"
# era5_seamless blends ERA5-Land and ERA5; ERA5-Land alone lacks the inputs of ET0.
_ARCHIVE_MODELS = "era5_seamless"


@dataclass(frozen=True)
class ObservedWeather:
    weather: list[DailyWeather]
    sim_start: date  # first usable day (<= sowing); the spin-up start
    segments: list[dict[str, Any]]  # run-length encoded per-day source tags
    warnings: list[str] = field(default_factory=list)


# ------------------------------------------------------------------ config
def _weather_api_url() -> str:
    return os.getenv("WEATHER_API_URL", "http://weather-api-service:8000").rstrip("/")


def _archive_url() -> str:
    url = os.getenv("OPENMETEO_ARCHIVE_URL", "").strip().rstrip("/")
    if not url:
        raise SimulationError(
            "archive_not_configured",
            "The weather archive is not configured; historical weather is unavailable.",
            503)
    return url


def climatology_years() -> int:
    raw = os.getenv("CLIMATOLOGY_YEARS", "15")
    try:
        n = int(raw)
    except ValueError:
        n = 0
    if n < 1:
        logger.error("sim_config_invalid var=CLIMATOLOGY_YEARS")
        raise SimulationError("config_error", "CLIMATOLOGY_YEARS must be a positive integer", 503)
    return n


# ------------------------------------------------------------------ parsing
def _finite(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _usable(
    day: date, tmin: Any, tmax: Any, precip: Any, et0: Any,
) -> tuple[DailyWeather | None, bool]:
    """(day, et0_raised). A positive ET0 below AquaCrop's minimum is raised to it
    (what AquaCrop's own weather reader does) and reported; anything else the
    engine would reject makes the day unusable."""
    vals = [_finite(x) for x in (tmin, tmax, precip, et0)]
    if any(v is None for v in vals):
        return None, False
    raised = 0 < vals[3] < MIN_ET0
    if raised:
        vals[3] = MIN_ET0
    w = DailyWeather(day, *vals)
    try:
        check_weather_day(w)
    except EngineInputError:
        return None, False
    return w, raised


@dataclass
class _Fetched:
    days: dict[date, DailyWeather] = field(default_factory=dict)
    et0_raised: set[date] = field(default_factory=set)


# ------------------------------------------------------------------ fetching
async def fetch_parcel_daily(
    parcel_id: str, tenant_id: str, start: date, end: date,
) -> _Fetched:
    """Usable days of the parcel series in [start, end] (requests of <= 400 days)."""
    out = _Fetched()
    url = f"{_weather_api_url()}/api/weather/parcel/{parcel_id}/daily"
    s = start
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            while s <= end:
                e = min(s + timedelta(days=_PARCEL_MAX_DAYS - 1), end)
                resp = await client.get(
                    url, params={"start": s.isoformat(), "end": e.isoformat()},
                    headers=soil_module_headers(tenant_id))
                if resp.status_code != 200:
                    raise SimulationError(
                        "weather_unavailable",
                        f"The weather service answered HTTP {resp.status_code}.", 503)
                body = resp.json()
                for row in (body.get("days") if isinstance(body, dict) else None) or []:
                    try:
                        d = date.fromisoformat(str(row["date"])[:10])
                    except (KeyError, ValueError):
                        continue
                    w, raised = _usable(d, row.get("tmin_c"), row.get("tmax_c"),
                                        row.get("precip_mm"), row.get("et0_mm"))
                    if w is not None:
                        out.days[d] = w
                        if raised:
                            out.et0_raised.add(d)
                s = e + timedelta(days=1)
    except SimulationError as err:
        logger.error("sim_weather_fetch_failed source=parcel_daily parcel=%s tenant=%s code=%s",
                     parcel_id, tenant_id, err.code)
        raise
    except Exception as err:
        logger.error("sim_weather_fetch_failed source=parcel_daily parcel=%s tenant=%s url=%s error=%s",
                     parcel_id, tenant_id, url, type(err).__name__)
        raise SimulationError("weather_unavailable", "The weather service is unreachable.", 503) from err
    return out


async def fetch_archive_daily(lat: float, lon: float, start: date, end: date) -> _Fetched:
    """Usable days of the Open-Meteo archive at (lat, lon) in [start, end]."""
    base = _archive_url()
    url = f"{base}/v1/archive"
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "daily": _ARCHIVE_DAILY, "models": _ARCHIVE_MODELS, "timezone": "UTC",
    }
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(url, params=params)
        if resp.status_code != 200:
            raise SimulationError(
                "archive_unavailable", f"The weather archive answered HTTP {resp.status_code}.", 503)
        daily = resp.json().get("daily") or {}
        times = daily["time"]
        cols = [daily[k] for k in ("temperature_2m_min", "temperature_2m_max",
                                   "precipitation_sum", "et0_fao_evapotranspiration")]
        if any(len(c) != len(times) for c in cols):
            raise ValueError("column length mismatch")
    except SimulationError as err:
        logger.error("sim_weather_fetch_failed source=archive code=%s", err.code)
        raise
    except Exception as err:
        logger.error("sim_weather_fetch_failed source=archive error=%s", type(err).__name__)
        raise SimulationError(
            "archive_unavailable", "The weather archive is unreachable or answered unexpectedly.",
            503) from err
    out = _Fetched()
    for i, t in enumerate(times):
        d = date.fromisoformat(str(t)[:10])
        w, raised = _usable(d, cols[0][i], cols[1][i], cols[2][i], cols[3][i])
        if w is not None:
            out.days[d] = w
            if raised:
                out.et0_raised.add(d)
    return out


# ----------------------------------------------------------------- observed
def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def _list_days(days: list[date]) -> str:
    shown = ", ".join(d.isoformat() for d in days[:_MAX_LISTED_DAYS])
    more = f" (+{len(days) - _MAX_LISTED_DAYS} more)" if len(days) > _MAX_LISTED_DAYS else ""
    return f"{shown}{more}"


def _et0_warning(raised: list[date]) -> list[str]:
    if not raised:
        return []
    msg = (f"ET0 raised to AquaCrop's minimum of {MIN_ET0} mm on {len(raised)} day(s): "
           f"{_list_days(raised)}")
    return [msg]


# The parcel series is lapse-rate corrected to the parcel altitude; the archive
# is corrected to the archive's own terrain model at the point, which can differ
# from it. Mixing them is declared, never silent.
ARCHIVE_ALTITUDE_WARNING = (
    "archive weather is corrected to the archive's terrain height at the point, "
    "not to the parcel altitude")


def _segments(tags: list[tuple[date, str]]) -> list[dict[str, Any]]:
    segs: list[dict[str, Any]] = []
    for d, src in tags:
        if segs and segs[-1]["source"] == src:
            segs[-1]["end"] = d.isoformat()
            segs[-1]["days"] += 1
        else:
            segs.append({"source": src, "start": d.isoformat(), "end": d.isoformat(), "days": 1})
    return segs


async def assemble_observed(
    parcel_id: str, tenant_id: str, lat: float, lon: float, planting: date, today: date,
) -> ObservedWeather:
    """Observed daily weather from ``planting - 365 d`` to yesterday (<= season end).

    Raises SimulationError ``weather_gaps`` (422) for unfilled days inside the
    series or when the sowing day itself has no weather, and 503 codes when a
    source is unavailable or the archive is needed but not configured.
    """
    wanted_start = planting - timedelta(days=SPINUP_DAYS)
    end = min(today - timedelta(days=1), planting + timedelta(days=SEASON_DAYS - 1))
    parcel_fetch = await fetch_parcel_daily(parcel_id, tenant_id, wanted_start, end)
    parcel = parcel_fetch.days

    # The archive fills days up to the last parcel day (leading/interior holes);
    # without any parcel day it covers the whole range. Days after the last parcel
    # day are not yet published and are not back-filled from another source.
    upper = max(parcel) if parcel else end
    archive_fetch = _Fetched()
    if any(d not in parcel for d in _days(wanted_start, upper)):
        archive_fetch = await fetch_archive_daily(lat, lon, wanted_start, upper)
    archive = archive_fetch.days

    values: dict[date, tuple[DailyWeather, str]] = {}
    for d in _days(wanted_start, end):
        if d in parcel:
            values[d] = (parcel[d], "parcel_daily")
        elif d <= upper and d in archive:
            values[d] = (archive[d], "archive")
    if not values:
        raise SimulationError("weather_gaps", "No weather is available for this parcel.")
    first, last = min(values), max(values)
    gaps = [d for d in _days(first, last) if d not in values]
    if gaps:
        logger.warning("sim_weather_gaps parcel=%s tenant=%s n=%d", parcel_id, tenant_id, len(gaps))
        raise SimulationError(
            "weather_gaps", f"Weather is missing for {len(gaps)} day(s): {_list_days(gaps)}.")
    if not first <= planting <= last:
        raise SimulationError(
            "weather_gaps",
            f"No weather on the sowing date {planting.isoformat()} "
            f"(available {first.isoformat()} to {last.isoformat()}).")
    days = _days(first, last)
    raised = sorted(
        d for d in days
        if d in (parcel_fetch.et0_raised if values[d][1] == "parcel_daily" else archive_fetch.et0_raised))
    return ObservedWeather(
        weather=[values[d][0] for d in days],
        sim_start=first,
        segments=_segments([(d, values[d][1]) for d in days]),
        warnings=[
            *_et0_warning(raised),
            *([ARCHIVE_ALTITUDE_WARNING] if any(v[1] == "archive" for v in values.values()) else []),
        ],
    )


# ----------------------------------------------------------------- analogs
def analog_year_candidates(planting: date, today: date, n_years: int) -> list[int]:
    """The ``n_years`` seasons before the campaign, newest last.

    A season starting on the planting date in year Y ends (planting + 364 d) in
    Y or Y + 1; every Y <= planting.year - 1 has ended before the sowing date,
    so its data is complete by construction. ``today`` is kept for symmetry with
    the other assemblers.
    """
    last = planting.year - 1
    return list(range(last - n_years + 1, last + 1))


async def assemble_analogs(
    lat: float, lon: float, planting: date, today: date, years: int | None = None,
) -> tuple[dict[int, list[DailyWeather]], list[str]]:
    """Archive weather per analog year, for the climatological ensemble.

    Returns ``({year: series}, warnings)``. Years whose season has any missing
    or invalid day are dropped and named in the warnings (never filled). Every
    year shares the one series fetched, which covers all the seasons.
    """
    n = years if years is not None else climatology_years()
    _archive_url()  # fail early with archive_not_configured
    cands = analog_year_candidates(planting, today, n)
    season_end = planting + timedelta(days=SEASON_DAYS - 1)
    fetch_start = analog_date(planting, planting.year, cands[0])
    fetch_end = analog_date(season_end, planting.year, cands[-1])
    fetched = await fetch_archive_daily(lat, lon, fetch_start, fetch_end)
    series = fetched.days
    campaign = _days(planting, season_end)
    ordered = [series[d] for d in sorted(series)]
    out: dict[int, list[DailyWeather]] = {}
    warns: list[str] = []
    for y in cands:
        missing = [m for m in (analog_date(d, planting.year, y) for d in campaign) if m not in series]
        if missing:
            warns.append(
                f"analog year {y} dropped: no usable archive weather for {_list_days(missing)}")
        else:
            out[y] = ordered
    if len(out) < n:
        warns.append(f"climatological ensemble uses {len(out)} of {n} requested years")
    if out:
        used = [d for y in out for d in (analog_date(c, planting.year, y) for c in campaign)]
        raised = sorted(set(used) & fetched.et0_raised)
        warns.extend(_et0_warning(raised))
        warns.append(f"analog years: {ARCHIVE_ALTITUDE_WARNING}")
    return out, warns
