"""Daily ET0 and precipitation climatology per 0.25 degree cell, loaded in the background.

Seasonal irrigation need needs the daily ET0 and rain of the last years at the parcel. A cold archive
fetch takes about 80 s, so a fit request never waits for it: it asks `get_climatology` (the cache or
None) and `schedule_climatology` starts the load. The cache key is the cell, never the tenant: climate
is not tenant data and nothing tenant-specific is stored. The cache lives in this process (like
`sim_jobs`): one copy per pod, lost on restart. A failed cell is not retried before RETRY_BACKOFF_S.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date
from typing import TypedDict

from app.services import sim_weather

logger = logging.getLogger(__name__)

GRID_DEG = 0.25  # ERA5 grid
TTL_S = 30 * 24 * 3600
RETRY_BACKOFF_S = 15 * 60


class DayClimate(TypedDict):
    doy: int
    et0: float
    precip: float


class Climatology(TypedDict):
    years: list[int]
    daily: dict[int, list[DayClimate]]


_CACHE: dict[str, tuple[float, Climatology]] = {}  # cell -> (loaded_at, data)
_FAILED: dict[str, float] = {}  # cell -> failed_at
_LOADING: dict[str, asyncio.Task] = {}


def _now() -> float:
    return time.monotonic()


def _today() -> date:
    return date.today()


def _snap(v: float) -> float:
    return round(v / GRID_DEG) * GRID_DEG


def cell_key(lat: float, lon: float) -> str:
    return f"{_snap(lat):.2f}:{_snap(lon):.2f}"


def _centre(cell: str) -> tuple[float, float]:
    lat, lon = cell.split(":")
    return float(lat), float(lon)


def get_climatology(cell: str) -> Climatology | None:
    """The cached climatology of the cell, or None (cold, expired or failed). Never loads."""
    hit = _CACHE.get(cell)
    if hit is None:
        return None
    loaded_at, data = hit
    if _now() - loaded_at >= TTL_S:
        return None
    return data


def _years(today: date) -> list[int]:
    n = sim_weather.climatology_years()
    return list(range(today.year - n, today.year))  # complete calendar years only


async def _load(cell: str) -> None:
    years: list[int] = []
    try:
        years = _years(_today())
        lat, lon = _centre(cell)
        fetched = await sim_weather.fetch_archive_daily(lat, lon, date(years[0], 1, 1), date(years[-1], 12, 31))
        daily: dict[int, list[DayClimate]] = {y: [] for y in years}
        for day in sorted(fetched.days):
            w = fetched.days[day]
            if day.year in daily:
                daily[day.year].append({"doy": day.timetuple().tm_yday, "et0": w.et0_mm, "precip": w.precip_mm})
        daily = {y: rows for y, rows in daily.items() if rows}
        if not daily:
            raise ValueError("no usable archive days")
        _CACHE[cell] = (_now(), {"years": sorted(daily), "daily": daily})
        _FAILED.pop(cell, None)
    except Exception:
        _FAILED[cell] = _now()
        logger.warning("climatology load failed cell=%s years=%s", cell, years, exc_info=True)
    finally:
        _LOADING.pop(cell, None)


def schedule_climatology(cell: str) -> asyncio.Task | None:
    """Start the background load unless the cell is fresh, loading, or recently failed. Needs a running loop."""
    if get_climatology(cell) is not None or cell in _LOADING:
        return None
    failed_at = _FAILED.get(cell)
    if failed_at is not None and _now() - failed_at < RETRY_BACKOFF_S:
        return None
    task = asyncio.create_task(_load(cell))
    _LOADING[cell] = task  # also keeps the reference until it ends
    return task
