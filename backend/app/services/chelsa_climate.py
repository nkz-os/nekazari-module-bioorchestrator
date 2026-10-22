"""CHELSA-derived parcel climate helpers.

Köppen–Geiger classification follows Peel, Finlayson & McMahon (2007),
HESS 11:1633-1644, computed from 12 monthly normals (Jan..Dec).
"""
from __future__ import annotations

import asyncio
import logging
import math
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# ASSUMPTION: 5 degC margin over the coldest-month minimum for frost screening (editable).
DEFAULT_FROST_MARGIN_C: float = 5.0
CHELSA_BASE = "https://os.unil.cloud.switch.ch/chelsa02/chelsa/global/"
NODATA = 65535
GRID_STEPS_PER_DEG = 120  # 30 arc-seconds
SOURCE_LABEL = "CHELSA v2.1 1981-2010"
_MONTHS = [f"{m:02d}" for m in range(1, 13)]


def cell_key(lat: float, lon: float) -> str:
    return f"{math.floor(lat * GRID_STEPS_PER_DEG)}:{math.floor(lon * GRID_STEPS_PER_DEG)}"


def cell_center(lat: float, lon: float) -> tuple[float, float]:
    """(lat, lon) of the centre of the 30-arc-second cell containing the point."""
    i = math.floor(lat * GRID_STEPS_PER_DEG)
    j = math.floor(lon * GRID_STEPS_PER_DEG)
    return (i + 0.5) / GRID_STEPS_PER_DEG, (j + 0.5) / GRID_STEPS_PER_DEG


def layer_paths() -> dict[str, str]:
    paths: dict[str, str] = {}
    for var in ("tas", "pr"):
        for m in _MONTHS:
            paths[f"{var}_{m}"] = f"climatologies/{var}/1981-2010/CHELSA_{var}_{m}_1981-2010_V.2.1.tif"
    paths["bio06"] = "bioclim/bio06/1981-2010/CHELSA_bio06_1981-2010_V.2.1.tif"
    paths["petmean"] = "bioclim/petmean/1981-2010/CHELSA_petmean_1981-2010_V.2.1.tif"
    return paths


def convert(name: str, raw: float | None) -> float | None:
    """Raw CHELSA integer to physical units (constants fixed in code)."""
    if raw is None or raw == NODATA:
        return None
    if name.startswith("tas_") or name == "bio06":
        return raw * 0.1 - 273.15
    if name.startswith("pr_"):
        return raw * 0.1
    if name == "petmean":
        return raw * 0.01 * 12
    raise ValueError(f"unknown CHELSA layer: {name}")


def summarize(values: dict[str, float | None], lat: float) -> dict | None:
    tas = [values.get(f"tas_{m}") for m in _MONTHS]
    pr = [values.get(f"pr_{m}") for m in _MONTHS]
    complete = all(v is not None for v in tas) and all(v is not None for v in pr)
    koppen = koppen_peel(tas, pr, lat) if complete else None
    annual_temp = sum(tas) / 12.0 if all(v is not None for v in tas) else None
    annual_rain = sum(pr) if all(v is not None for v in pr) else None
    et0 = values.get("petmean")
    cold = values.get("bio06")
    if koppen is None and annual_temp is None and annual_rain is None and et0 is None and cold is None:
        return None
    return {
        "koppen": koppen,
        "annual_temp_c": annual_temp,
        "annual_rainfall_mm": annual_rain,
        "annual_et0_mm": et0,
        "coldest_month_min_c": cold,
        "monthly_tas_c": tas,
        "monthly_pr_mm": pr,
        "source": SOURCE_LABEL,
    }


def _sample_dataset(ds, lon: float, lat: float):
    """Raw value at a point, or None outside the raster (rasterio would return the fill value 0)."""
    b = ds.bounds
    if not (b.left <= lon <= b.right and b.bottom <= lat <= b.top):
        return None
    raw = next(ds.sample([(lon, lat)]))[0]
    raw = raw.item() if hasattr(raw, "item") else raw
    return None if raw == NODATA else raw


def _rasterio_sampler(path: str, lon: float, lat: float):
    import rasterio  # lazy: module must import without rasterio

    with rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        GDAL_HTTP_TIMEOUT="20",
        GDAL_HTTP_MAX_RETRY="2",
        VSI_CACHE="TRUE",
        GDAL_HTTP_MULTIPLEX="YES",
        GDAL_HTTP_VERSION="2",
    ), rasterio.open(f"/vsicurl/{CHELSA_BASE}{path}") as ds:
        return _sample_dataset(ds, lon, lat)


async def read_cell(lat: float, lon: float, *, sampler=None, timeout_s: float = 30.0) -> dict | None:
    """Sample the 26 CHELSA layers at the cell centre; None on timeout or any error."""
    sampler = sampler or _rasterio_sampler
    key = cell_key(lat, lon)
    c_lat, c_lon = cell_center(lat, lon)
    paths = layer_paths()
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=13)
    try:
        futures = {
            name: loop.run_in_executor(executor, sampler, path, c_lon, c_lat)
            for name, path in paths.items()
        }
        raws = await asyncio.wait_for(asyncio.gather(*futures.values()), timeout=timeout_s)
        values = {name: convert(name, raw) for name, raw in zip(futures, raws)}
        return summarize(values, c_lat)
    except Exception as exc:  # noqa: BLE001 - any failure must degrade to None (incl. timeout)
        logger.warning("chelsa read failed for cell %s: %s", key, type(exc).__name__)
        return None
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def koppen_peel(tas_c: list[float], pr_mm: list[float], lat: float) -> str | None:
    """Return the Köppen–Geiger class, or None for invalid input."""
    if tas_c is None or pr_mm is None or len(tas_c) != 12 or len(pr_mm) != 12:
        return None
    if any(v is None for v in tas_c) or any(v is None for v in pr_mm):
        return None

    mat = sum(tas_c) / 12.0
    pann = sum(pr_mm)
    tcold = min(tas_c)
    thot = max(tas_c)
    tmon10 = sum(1 for t in tas_c if t > 10)

    summer_idx = (3, 4, 5, 6, 7, 8) if lat >= 0 else (9, 10, 11, 0, 1, 2)
    winter_idx = tuple(i for i in range(12) if i not in summer_idx)
    p_summer = [pr_mm[i] for i in summer_idx]
    p_winter = [pr_mm[i] for i in winter_idx]
    ps, pw = sum(p_summer), sum(p_winter)
    psdry, pswet = min(p_summer), max(p_summer)
    pwdry, pwwet = min(p_winter), max(p_winter)
    pdry = min(pr_mm)

    if pw >= 0.7 * pann:
        pth = 2 * mat
    elif ps >= 0.7 * pann:
        pth = 2 * mat + 28
    else:
        pth = 2 * mat + 14

    if thot < 10:
        return "ET" if thot > 0 else "EF"

    if pann < 10 * pth:
        return ("BW" if pann < 5 * pth else "BS") + ("h" if mat >= 18 else "k")

    if tcold >= 18:
        if pdry >= 60:
            return "Af"
        if pdry >= 100 - pann / 25:
            return "Am"
        return "Aw"

    group = "C" if tcold > 0 else "D"
    if psdry < 40 and psdry < pwwet / 3:
        second = "s"
    elif pwdry < pswet / 10:
        second = "w"
    else:
        second = "f"
    if thot >= 22:
        third = "a"
    elif tmon10 >= 4:
        third = "b"
    elif group == "D" and tcold < -38:
        third = "d"
    else:
        third = "c"
    return group + second + third
