"""Soil module HTTP client — fetches actual soil properties per parcel."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from cachetools import TTLCache

from app.services.soil_headers import soil_module_headers

logger = logging.getLogger(__name__)

SOIL_API_URL = os.getenv("SOIL_API_URL", "http://soil-module-service:8000")

# Cache soil properties per parcel for 24h (soil doesn't change day-to-day)
_soil_cache: TTLCache[str, dict[str, Any]] = TTLCache(maxsize=256, ttl=86400)


async def get_parcel_soil_properties(parcel_id: str, tenant_id: str = "") -> dict[str, Any]:
    """Fetch actual soil properties for a parcel from the Soil module.

    Calls GET /v1/soil/parcel/{id}/summary (AgriSoilExtended entity)
    and extracts the top-horizon properties.

    Returns dict with: ph, texture, awc_mm, organic_matter_pct,
    bulk_density_g_cm3, depth_cm, source, data_available.

    If Soil module is unreachable or parcel has no soil data,
    returns data_available=False.
    Results cached per parcel_id for 24h.
    """
    cache_key = f"{tenant_id}:{parcel_id}" if tenant_id else parcel_id
    cached = _soil_cache.get(cache_key)
    if cached is not None:
        return cached

    headers = soil_module_headers(tenant_id) if tenant_id else {}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{SOIL_API_URL}/v1/soil/parcel/{parcel_id}/summary",
                headers=headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                horizons_raw = data.get("horizons")
                if isinstance(horizons_raw, dict):
                    horizons = horizons_raw.get("value", [])
                elif isinstance(horizons_raw, list):
                    horizons = horizons_raw
                else:
                    horizons = []
                if not horizons:
                    result: dict[str, Any] = {"data_available": False, "source": "no_horizons"}
                    _soil_cache[cache_key] = result
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
                    "drainage": derive_drainage_class(h),
                    "hydrologic_group": h.get("hydrologicGroup"),
                    "source": (data.get("dataSource") or {}).get("value", "soilgrids"),
                    "data_available": True,
                }
                _soil_cache[cache_key] = result
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
    _soil_cache[cache_key] = result
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


# ── Soil-suitability gate (C.5) ──────────────────────────────────────────────
# Compares a crop's STANDARD tolerance (EcoCrop / CropSoilSuitability) against
# a parcel's REAL soil (read live from the Soil module) and returns a graded
# verdict: suitable / marginal / unsuitable / unknown.

# pH band (units) outside the crop's [min, max] still counted as marginal
# rather than unsuitable — soil pH is noisy and amendable at the margin.
_PH_MARGIN = 0.5

# USDA texture classes ordered by increasing fineness (clay content). Distance
# on this scale is an agronomically-defensible proxy for texture mismatch:
# sand↔clay is far (drainage/water-holding opposite), loam↔sandy_loam is near.
_TEXTURE_ORDINAL: dict[str, int] = {
    "sand": 0,
    "loamy sand": 1,
    "sandy loam": 2,
    "loam": 3,
    "silt loam": 3,
    "silt": 3,
    "sandy clay loam": 4,
    "clay loam": 4,
    "silty clay loam": 4,
    "sandy clay": 5,
    "silty clay": 5,
    "clay": 6,
}

_RANK = {"suitable": 0, "marginal": 1, "unsuitable": 2}
_BY_RANK = {0: "suitable", 1: "marginal", 2: "unsuitable"}

# SCS hydrologic soil group → crop drainage class. Group A drains fastest
# (excessively/well drained), D slowest (poorly drained → waterlogging). The
# Soil module derives the group from Ksat (Saxton-Rawls); crop-health consumes
# the same `hydrologicGroup`/`ksat` for its (dynamic) waterlogging engine.
_SCS_TO_DRAINAGE = {"A": "well_drained", "B": "well_drained", "C": "moderate", "D": "poor"}

# Crop drainage vocabulary ordered from free-draining to waterlogged. Distance
# on this scale grades a crop's drainage preference against the parcel's class.
_DRAINAGE_ORDINAL = {"well_drained": 0, "moderate": 1, "poor": 2}


def _scs_from_ksat(ksat: float) -> str:
    """NRCS SCS hydrologic group from saturated conductivity (mm/h)."""
    if ksat > 36:
        return "A"
    if ksat > 3.6:
        return "B"
    if ksat > 0.36:
        return "C"
    return "D"


def derive_drainage_class(horizon: dict) -> str | None:
    """Map a soil horizon to a crop drainage class (well_drained/moderate/poor).

    Prefers the horizon's `hydrologicGroup`; falls back to deriving the SCS
    group from `saturatedHydraulicConductivity`. Returns None if neither is
    present (→ drainage dimension not assessable → honest `unknown`).
    """
    group = horizon.get("hydrologicGroup")
    if group is None:
        ksat = horizon.get("saturatedHydraulicConductivity")
        if ksat is None:
            return None
        group = _scs_from_ksat(ksat)
    return _SCS_TO_DRAINAGE.get(str(group).strip().upper())


def _normalize_texture(texture: str) -> str:
    return texture.strip().lower().replace("_", " ").replace("-", " ")


def _texture_ordinal(texture: str | None) -> int | None:
    if not texture:
        return None
    return _TEXTURE_ORDINAL.get(_normalize_texture(texture))


def _assess_ph(ph, ph_min, ph_max) -> tuple[str | None, str | None]:
    """Return (verdict, reason) for pH, or (None, None) if not assessable."""
    if ph is None or ph_min is None or ph_max is None:
        return None, None
    if ph_min <= ph <= ph_max:
        return "suitable", None
    if ph < ph_min:
        gap, side, bound = ph_min - ph, "below", ph_min
    else:
        gap, side, bound = ph - ph_max, "above", ph_max
    verdict = "marginal" if gap <= _PH_MARGIN else "unsuitable"
    return verdict, f"Soil pH {ph} {side} crop {'min' if side == 'below' else 'max'} {bound}"


def _assess_texture(texture, req_textures) -> tuple[str | None, str | None]:
    """Return (verdict, reason) for texture, or (None, None) if not assessable."""
    parcel_ord = _texture_ordinal(texture)
    req_ords = [o for t in (req_textures or []) if (o := _texture_ordinal(t)) is not None]
    if parcel_ord is None or not req_ords:
        return None, None
    dist = min(abs(parcel_ord - o) for o in req_ords)
    if dist <= 1:
        return "suitable", None
    verdict = "marginal" if dist <= 3 else "unsuitable"
    return verdict, f"Soil texture '{texture}' distant from crop preference {req_textures}"


def _assess_drainage(drainage, req_drainage) -> tuple[str | None, str | None]:
    """Return (verdict, reason) for drainage, or (None, None) if not assessable."""
    parcel_ord = _DRAINAGE_ORDINAL.get(drainage) if drainage else None
    req_ords = [o for d in (req_drainage or []) if (o := _DRAINAGE_ORDINAL.get(d)) is not None]
    if parcel_ord is None or not req_ords:
        return None, None
    dist = min(abs(parcel_ord - o) for o in req_ords)
    if dist == 0:
        return "suitable", None
    verdict = "marginal" if dist == 1 else "unsuitable"
    return verdict, f"Soil drainage '{drainage}' vs crop preference {req_drainage}"


def assess_soil_suitability(crop_tolerance: dict | None, parcel_soil: dict | None) -> dict:
    """Grade a crop's fit to a parcel's real soil.

    Args:
        crop_tolerance: crop STANDARD tolerance (from GraphDAO.get_soil_suitability):
            {ph_min, ph_max, textures, drainage, ...}. None/empty → unknown.
        parcel_soil: parcel's REAL soil (from get_parcel_soil_properties):
            {ph, texture, ..., data_available}. Unavailable → unknown.

    Returns:
        {'verdict': 'suitable'|'marginal'|'unsuitable'|'unknown', 'reason': str,
         'ph': {value, min, max, verdict}, 'texture': {value, preferred, verdict},
         'confidence': 'high'|'medium'|'low', 'source': str}
    Never silently treats unknown as suitable (fail-safe = honesty).
    """
    parcel_src = (parcel_soil or {}).get("source", "unknown")
    source = f"crop tolerance (EcoCrop) × parcel soil ({parcel_src})"

    ph_val = (parcel_soil or {}).get("ph")
    texture_val = (parcel_soil or {}).get("texture")
    drainage_val = (parcel_soil or {}).get("drainage")
    ph_min = (crop_tolerance or {}).get("ph_min")
    ph_max = (crop_tolerance or {}).get("ph_max")
    req_textures = (crop_tolerance or {}).get("textures") or []
    req_drainage = (crop_tolerance or {}).get("drainage") or []

    ph_detail = {"value": ph_val, "min": ph_min, "max": ph_max, "verdict": None}
    texture_detail = {"value": texture_val, "preferred": req_textures, "verdict": None}
    drainage_detail = {"value": drainage_val, "preferred": req_drainage, "verdict": None}

    if not crop_tolerance:
        return {
            "verdict": "unknown",
            "reason": "No soil tolerance data for this crop",
            "ph": ph_detail, "texture": texture_detail, "drainage": drainage_detail,
            "confidence": "low", "source": source,
        }
    if not parcel_soil or not parcel_soil.get("data_available"):
        return {
            "verdict": "unknown",
            "reason": "Parcel soil unavailable from Soil module — gate cannot run",
            "ph": ph_detail, "texture": texture_detail, "drainage": drainage_detail,
            "confidence": "low", "source": source,
        }

    ph_verdict, ph_reason = _assess_ph(ph_val, ph_min, ph_max)
    texture_verdict, texture_reason = _assess_texture(texture_val, req_textures)
    drainage_verdict, drainage_reason = _assess_drainage(drainage_val, req_drainage)
    ph_detail["verdict"] = ph_verdict
    texture_detail["verdict"] = texture_verdict
    drainage_detail["verdict"] = drainage_verdict

    assessed = [
        (v, r) for v, r in (
            (ph_verdict, ph_reason),
            (texture_verdict, texture_reason),
            (drainage_verdict, drainage_reason),
        ) if v is not None
    ]
    if not assessed:
        return {
            "verdict": "unknown",
            "reason": "No comparable soil dimension (pH/texture/drainage) for crop and parcel",
            "ph": ph_detail, "texture": texture_detail, "drainage": drainage_detail,
            "confidence": "low", "source": source,
        }

    worst_rank = max(_RANK[v] for v, _ in assessed)
    verdict = _BY_RANK[worst_rank]
    reasons = [r for v, r in assessed if r and _RANK[v] == worst_rank]
    if verdict == "suitable":
        reason = "Soil pH, texture and drainage within crop tolerance"
    else:
        reason = "; ".join(reasons) or f"Soil {verdict} for crop"

    # Two+ agreeing dimensions → medium; a single dimension → low (thinner evidence).
    confidence = "medium" if len(assessed) >= 2 else "low"

    return {
        "verdict": verdict,
        "reason": reason,
        "ph": ph_detail,
        "texture": texture_detail,
        "drainage": drainage_detail,
        "confidence": confidence,
        "source": source,
    }
