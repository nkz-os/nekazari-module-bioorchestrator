"""Soil module HTTP client — fetches actual soil properties per parcel."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from cachetools import TTLCache

logger = logging.getLogger(__name__)

SOIL_API_URL = os.getenv("SOIL_API_URL", "http://soil-api-service:5000")

# Cache soil properties per parcel for 24h (soil doesn't change day-to-day)
_soil_cache: TTLCache[str, dict[str, Any]] = TTLCache(maxsize=256, ttl=86400)


async def get_parcel_soil_properties(parcel_id: str) -> dict[str, Any]:
    """Fetch actual soil properties for a parcel from the Soil module.

    Returns dict with: ph, texture, awc_mm, organic_matter_pct,
    bulk_density_g_cm3, depth_cm, source, data_available.

    If Soil module is unreachable, returns data_available=False.
    Results cached per parcel_id for 24h.
    """
    cached = _soil_cache.get(parcel_id)
    if cached is not None:
        return cached

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{SOIL_API_URL}/api/v1/soil/parcel/{parcel_id}/properties",
            )
            if resp.status_code == 200:
                data = resp.json()
                result = {
                    "ph": data.get("ph"),
                    "texture": data.get("texture"),
                    "awc_mm": data.get("awc_mm"),
                    "organic_matter_pct": data.get("organic_matter_pct"),
                    "bulk_density_g_cm3": data.get("bulk_density_g_cm3"),
                    "depth_cm": data.get("depth_cm"),
                    "source": data.get("source", "soilgrids"),
                    "data_available": True,
                }
                _soil_cache[parcel_id] = result
                return result
            else:
                logger.warning(
                    "Soil module returned %d for parcel %s",
                    resp.status_code, parcel_id,
                )
    except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPError) as e:
        logger.warning("Soil module unreachable for parcel %s: %s", parcel_id, e)

    result: dict[str, Any] = {"data_available": False, "source": "unavailable"}
    _soil_cache[parcel_id] = result
    return result


def compute_soil_suitability(
    requirements: dict | None,
    actual: dict | None,
) -> dict | None:
    """Compare crop soil requirements against actual parcel soil.

    Returns None if no requirements or actual data available.
    """
    if not requirements or not actual or not actual.get("data_available"):
        return None

    warnings: list[str] = []
    ph_match = True
    texture_match = True

    ph = actual.get("ph")
    if (
        ph is not None
        and requirements.get("ph_min") is not None
        and requirements.get("ph_max") is not None
    ):
        ph_match = requirements["ph_min"] <= ph <= requirements["ph_max"]
        if not ph_match:
            warnings.append(
                f"Soil pH {ph} outside crop range "
                f"[{requirements['ph_min']}, {requirements['ph_max']}]"
            )

    texture = actual.get("texture")
    req_textures = requirements.get("textures", [])
    if texture and req_textures:
        texture_match = any(t.lower() in texture.lower() for t in req_textures)
        if not texture_match:
            warnings.append(
                f"Soil texture '{texture}' not in crop preference: {req_textures}"
            )

    awc_match = actual.get("awc_mm") is not None and actual["awc_mm"] > 0

    return {
        "ph_match": ph_match,
        "texture_match": texture_match,
        "awc_sufficient": awc_match,
        "overall": "suitable" if (ph_match and texture_match) else "unsuitable",
        "warnings": warnings,
    }
