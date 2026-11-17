"""Daily Tmin/Tmax history (1991 to yesterday) of a parcel, loaded in the background.

The late-frost fit needs ~35 years of daily temperature at the parcel. The weather API serves it
(`/api/weather/parcel/{id}/history`) but a cold first call can take longer than a request may wait, so
a fit request never waits: it asks `get_frost_history` (the cache or None) and `schedule_frost_history`
starts the load. The weather API may answer 200 with `incomplete_years` (+ Retry-After) or 503 with
Retry-After while it prepares the data; both are retried here, up to MAX_ATTEMPTS calls.

Unlike climatology (climate, not tenant data) the key is (tenant, parcel): parcel data is tenant-scoped.
The cache lives in this process: one copy per pod, lost on restart. A failed key is not retried before
RETRY_BACKOFF_S.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from datetime import UTC, date, datetime, timedelta
from typing import Literal

import httpx

from app.planning.late_frost import FrostHistory
from app.services.sim_weather import _weather_api_url
from app.services.soil_headers import soil_module_headers

logger = logging.getLogger(__name__)

TTL_S = 30 * 24 * 3600
RETRY_BACKOFF_S = 15 * 60
MAX_ENTRIES = 200
MAX_ATTEMPTS = 12
HISTORY_START = date(1991, 1, 1)
HTTP_TIMEOUT_S = 30  # the server-side deadline is 20 s
DEFAULT_RETRY_AFTER_S = 30
MAX_RETRY_AFTER_S = 300  # ASSUMPTION: bound a bogus Retry-After so one load cannot hang for hours

_Key = tuple[str, str]  # (tenant_id, parcel_id)

# Least recently used first; a stale entry stays (served while it reloads) until replaced or evicted.
_CACHE: OrderedDict[_Key, tuple[float, FrostHistory]] = OrderedDict()
_FAILED: dict[_Key, float] = {}  # key -> failed_at, cleared by a success
_LOADING: dict[_Key, asyncio.Task] = {}


class _LoadFailed(Exception):
    def __init__(self, status: str, attempts: int = 0):
        super().__init__(status)
        self.status = status
        self.attempts = attempts


def _now() -> float:
    return time.monotonic()


def _today() -> date:
    return datetime.now(UTC).date()


def get_frost_history(tenant_id: str, parcel_id: str) -> FrostHistory | None:
    """The cached history, or None. Never loads. A stale entry is still returned while it reloads."""
    key = (tenant_id, parcel_id)
    hit = _CACHE.get(key)
    if hit is None:
        return None
    _CACHE.move_to_end(key)
    return hit[1]


def _fresh(key: _Key) -> bool:
    hit = _CACHE.get(key)
    return hit is not None and _now() - hit[0] < TTL_S


def frost_history_status(tenant_id: str, parcel_id: str) -> Literal["ready", "loading", "failed", "absent"]:
    key = (tenant_id, parcel_id)
    if key in _CACHE:
        return "ready"
    if key in _LOADING:
        return "loading"
    if key in _FAILED:
        return "failed"
    return "absent"


def _retry_after(resp: httpx.Response) -> int | None:
    """Seconds from Retry-After; None when the header is absent; the default when not numeric."""
    raw = resp.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return max(1, min(int(float(raw)), MAX_RETRY_AFTER_S))
    except ValueError:
        return DEFAULT_RETRY_AFTER_S


def _num(v: object) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _parse(body: object) -> FrostHistory:
    if not isinstance(body, dict):
        raise _LoadFailed("bad_body")
    days: dict[date, tuple[float | None, float | None]] = {}
    for row in body.get("days") or []:
        try:
            d = date.fromisoformat(str(row["date"])[:10])
        except (KeyError, ValueError, TypeError):
            continue
        days[d] = (_num(row.get("tmin_c")), _num(row.get("tmax_c")))
    if not days:
        raise _LoadFailed("no_days")
    validation = body.get("validation")
    tmin_val = validation.get("tmin_c") if isinstance(validation, dict) else None
    # A non-empty list = the series was validated against stations (still warm-biased);
    # "unvalidated_region", or anything else / missing, = no station validation.
    caveat = "warm_bias" if isinstance(tmin_val, list) and tmin_val else "unvalidated_region"
    return FrostHistory(days=days, tmin_caveat=caveat)


async def _fetch(tenant_id: str, parcel_id: str) -> tuple[FrostHistory, int]:
    url = f"{_weather_api_url()}/api/weather/parcel/{parcel_id}/history"
    params = {"start": HISTORY_START.isoformat(), "end": (_today() - timedelta(days=1)).isoformat(),
              "resolution": "daily", "vars": "tmin_c,tmax_c"}
    attempts = 0
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
        while attempts < MAX_ATTEMPTS:
            attempts += 1
            resp = await client.get(url, params=params, headers=soil_module_headers(tenant_id))
            wait = _retry_after(resp)
            if resp.status_code == 503 and wait is not None:
                await asyncio.sleep(wait)  # busy or preparing
                continue
            if resp.status_code != 200:
                raise _LoadFailed(str(resp.status_code), attempts)  # 400/404/422 and a cache-less 503: no retry
            body = resp.json()
            if isinstance(body, dict) and body.get("incomplete_years"):
                await asyncio.sleep(wait if wait is not None else DEFAULT_RETRY_AFTER_S)
                continue
            try:
                return _parse(body), attempts
            except _LoadFailed as bad:
                raise _LoadFailed(bad.status, attempts) from None
    # ASSUMPTION: a history still missing years after MAX_ATTEMPTS is not cached; a partial series
    # would understate the frost frequency.
    raise _LoadFailed("gave_up", attempts)


async def _load(key: _Key) -> None:
    tenant_id, parcel_id = key
    try:
        history, attempts = await _fetch(tenant_id, parcel_id)
        _CACHE[key] = (_now(), history)
        _CACHE.move_to_end(key)
        while len(_CACHE) > MAX_ENTRIES:
            _CACHE.popitem(last=False)
        _FAILED.pop(key, None)
        logger.info("frost_history_loaded tenant=%s parcel=%s days=%d attempts=%d caveat=%s",
                    tenant_id, parcel_id, len(history.days), attempts, history.tmin_caveat)
    except Exception as err:
        status = err.status if isinstance(err, _LoadFailed) else type(err).__name__
        _FAILED.pop(key, None)
        _FAILED[key] = _now()
        while len(_FAILED) > MAX_ENTRIES:
            _FAILED.pop(next(iter(_FAILED)))
        logger.warning("frost_history_load_failed tenant=%s parcel=%s status=%s attempts=%s stale_kept=%s",
                       tenant_id, parcel_id, status, getattr(err, "attempts", "n/a"), key in _CACHE,
                       exc_info=not isinstance(err, _LoadFailed))
    finally:
        _LOADING.pop(key, None)


def schedule_frost_history(tenant_id: str, parcel_id: str) -> asyncio.Task | None:
    """Start the background load unless fresh, loading, or recently failed. Needs a running loop."""
    key = (tenant_id, parcel_id)
    if _fresh(key) or key in _LOADING:
        return None
    failed_at = _FAILED.get(key)
    if failed_at is not None and _now() - failed_at < RETRY_BACKOFF_S:
        return None
    task = asyncio.create_task(_load(key))
    _LOADING[key] = task
    return task
