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
from array import array
from collections import OrderedDict
from datetime import date
from typing import Literal

from app.services import sim_weather

logger = logging.getLogger(__name__)

GRID_DEG = 0.25  # ERA5 grid
TTL_S = 30 * 24 * 3600
RETRY_BACKOFF_S = 15 * 60
CLIMATOLOGY_MAX_CELLS = 200  # ~12 KB per cell and year; bounds the process memory


DAYS = 366  # slots per year, indexed by day-of-year - 1
_NAN = float("nan")


class Climatology:
    """Daily ET0 and rain of the cell's last complete years, two float32 arrays per year.

    Each array has 366 slots indexed by day-of-year - 1; NaN marks a day without usable archive data
    (and day 366 of a non-leap year). Gaps are explicit, never zero.
    """

    def __init__(self, series: dict[int, tuple[array, array]]):
        self._series = series
        self.years: list[int] = sorted(series)

    def year_series(self, year: int) -> tuple[array, array]:
        """(et0_mm, precip_mm) of the year; KeyError if the year is not in `years`."""
        return self._series[year]


# Least recently used first; a stale entry stays (served while it reloads) until replaced or evicted.
_CACHE: OrderedDict[str, tuple[float, Climatology]] = OrderedDict()  # cell -> (loaded_at, data)
_FAILED: dict[str, float] = {}  # cell -> failed_at, cleared by a success
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
    """The cached climatology of the cell, or None. Never loads. Past the TTL the stale entry is still
    returned while a reload runs (or after it failed); counts as a use for the LRU."""
    hit = _CACHE.get(cell)
    if hit is None:
        return None
    _CACHE.move_to_end(cell)
    return hit[1]


def _fresh(cell: str) -> bool:
    hit = _CACHE.get(cell)
    return hit is not None and _now() - hit[0] < TTL_S


def climatology_status(cell: str) -> Literal["ready", "loading", "failed", "absent"]:
    if cell in _CACHE:
        return "ready"
    if cell in _LOADING:
        return "loading"
    if cell in _FAILED:
        return "failed"
    return "absent"


def _years(today: date) -> list[int]:
    n = sim_weather.climatology_years()
    return list(range(today.year - n, today.year))  # complete calendar years only


async def _load(cell: str) -> None:
    years: list[int] = []
    try:
        years = _years(_today())
        lat, lon = _centre(cell)
        fetched = await sim_weather.fetch_archive_daily(lat, lon, date(years[0], 1, 1), date(years[-1], 12, 31))
        series = {y: (array("f", [_NAN] * DAYS), array("f", [_NAN] * DAYS)) for y in years}
        for day, w in fetched.days.items():
            if day.year in series:
                i = day.timetuple().tm_yday - 1
                series[day.year][0][i] = w.et0_mm
                series[day.year][1][i] = w.precip_mm
        series = {y: ab for y, ab in series.items() if any(v == v for v in ab[0])}
        if not series:
            raise ValueError("no usable archive days")
        _CACHE[cell] = (_now(), Climatology(series))
        _CACHE.move_to_end(cell)
        while len(_CACHE) > CLIMATOLOGY_MAX_CELLS:
            _CACHE.popitem(last=False)
        _FAILED.pop(cell, None)
    except Exception:
        _FAILED.pop(cell, None)
        _FAILED[cell] = _now()
        while len(_FAILED) > CLIMATOLOGY_MAX_CELLS:
            _FAILED.pop(next(iter(_FAILED)))
        logger.warning("climatology load failed cell=%s years=%s stale_kept=%s", cell, years, cell in _CACHE,
                       exc_info=True)
    finally:
        _LOADING.pop(cell, None)


def schedule_climatology(cell: str) -> asyncio.Task | None:
    """Start the background load unless the cell is fresh (a stale one reloads), loading, or recently failed. Needs a running loop."""
    if _fresh(cell) or cell in _LOADING:
        return None
    failed_at = _FAILED.get(cell)
    if failed_at is not None and _now() - failed_at < RETRY_BACKOFF_S:
        return None
    task = asyncio.create_task(_load(cell))
    _LOADING[cell] = task  # also keeps the reference until it ends
    return task
