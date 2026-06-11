"""Soil module HTTP client — fetches actual soil properties per parcel."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from cachetools import TTLCache

logger = logging.getLogger(__name__)

SOIL_API_URL = os.getenv("SOIL_API_URL", "http://soil-module-service:8000")

# Cache soil properties per parcel for 24h (soil doesn't change day-to-day)
_soil_cache: TTLCache[str, dict[str, Any]] = TTLCache(maxsize=256, ttl=86400)


async def get_parcel_soil_properties(parcel_id: str) -> dict[str, Any]:
    """Fetch actual soil properties for a parcel from the Soil module.

    Calls GET /v1/soil/parcel/{id}/summary (AgriSoilExtended entity)
    and extracts the top-horizon properties.

    Returns dict with: ph, texture, awc_mm, organic_matter_pct,
    bulk_density_g_cm3, depth_cm, source, data_available.

    If Soil module is unreachable or parcel has no soil data,
    returns data_available=False.
    Results cached per parcel_id for 24h.
    """
    cached = _soil_cache.get(parcel_id)
    if cached is not None:
        return cached

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{SOIL_API_URL}/v1/soil/parcel/{parcel_id}/summary",
            )
            if resp.status_code == 200:
                data = resp.json()
                # NGSI-LD entity: horizons live inside {"type":"Property","value":[...]}
                horizons = (data.get("horizons") or {}).get("value", [])
                if not horizons:
                    result: dict[str, Any] = {"data_available": False, "source": "no_horizons"}
                    _soil_cache[parcel_id] = result
                    return result
                h = horizons[0]
                # Organic matter ≈ organic_carbon × 1.724 (van Bemmelen factor)
                oc = h.get("organicCarbon")
                om_pct = round(oc * 1.724, 2) if oc is not None else None
                result = {
                    "ph": h.get("ph"),
                    "texture": h.get("usdaTextureClass"),
                    "awc_mm": (
                        round((h["fieldCapacity"] - h["wiltingPoint"]) * (h["depthTo"] - h["depthFrom"]) * 10, 1)
                        if h.get("fieldCapacity") is not None and h.get("wiltingPoint") is not None
                        else None
                    ),
                    "organic_matter_pct": om_pct,
                    "bulk_density_g_cm3": h.get("bulkDensity"),
                    "depth_cm": f"{h.get('depthFrom', 0)}-{h.get('depthTo', 30)}",
                    "source": (data.get("dataSource") or {}).get("value", "soilgrids"),
                    "data_available": True,
                }
                _soil_cache[parcel_id] = result
                return result
            elif resp.status_code == 404:
                logger.info("No soil data for parcel %s (404)", parcel_id)
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
