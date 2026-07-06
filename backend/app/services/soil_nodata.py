"""Soil provider no-data sentinels (mirrors nkz-module-soil nkz_soil.util.nodata)."""

from __future__ import annotations

SOILGRIDS_NODATA: tuple[float, ...] = (-9999.0, -3276.8, -3.40282347e38)

_SOIL_NUMERIC_KEYS = frozenset({
    "ph",
    "bulk_density_g_cm3",
    "organic_matter_pct",
    "awc_mm",
})


def is_soilgrids_nodata(value: float | int | None) -> bool:
    if value is None:
        return False
    try:
        fv = float(value)
    except (TypeError, ValueError):
        return False
    return any(abs(fv - sentinel) < 1e-6 for sentinel in SOILGRIDS_NODATA)


def sanitize_soil_properties(props: dict) -> dict:
    """Drop raster/REST nodata sentinels before exposing soil.actual in crop-context."""
    cleaned = dict(props)
    for key in _SOIL_NUMERIC_KEYS:
        if key in cleaned and is_soilgrids_nodata(cleaned[key]):
            cleaned[key] = None
    bd = cleaned.get("bulk_density_g_cm3")
    if bd is not None and (bd < 0.1 or bd > 2.65):
        cleaned["bulk_density_g_cm3"] = None
    ph = cleaned.get("ph")
    if ph is not None and (ph < 0 or ph > 14):
        cleaned["ph"] = None
    return cleaned
