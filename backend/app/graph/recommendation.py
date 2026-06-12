"""Weather-adjusted variety scoring for BioOrchestrator recommendations.

Applies environmental stress penalties to variety rankings based on
real-time weather statistics from Orion-LD (written by Weather-Map module).

Penalty rules:
  - Drought:  -20% when water_balance.deficit_area_pct > 30%
            (applied to crops not drought-tolerant)
  - Heat:     -15% when temperature_avg.heat_stress_pct > 20%
            (applied to crops not heat-tolerant)
  - Frost:    -25% when frost_risk.high_risk_pct > 50%
            (applied to crops not frost-tolerant)

Crop tolerance is determined from Neo4j CropHeatTolerance data:
  - heatDamageThresholdC >= 38°C → drought-tolerant (heuristic)
  - heatDamageThresholdC >= 35°C → heat-tolerant
  - frostDamageThresholdC <= -5°C → frost-tolerant
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Tolerance thresholds ────────────────────────────────────────────────
# Inferred from CropHeatTolerance data in Neo4j.
DROUGHT_TOLERANT_HEAT_C = 38.0
HEAT_TOLERANT_THRESHOLD_C = 35.0
FROST_TOLERANT_THRESHOLD_C = -5.0

# ── Penalty magnitudes ─────────────────────────────────────────────────
DROUGHT_PENALTY = 0.20   # 20%
HEAT_PENALTY = 0.15      # 15%
FROST_PENALTY = 0.25     # 25%


async def apply_weather_penalties(
    weather_stats: dict | None,
    ranked_varieties: list[dict],
    crop: str,
    dao,
) -> tuple[list[dict], dict | None, dict[str, Any]]:
    """Apply weather-based yield penalties to ranked variety list.

    Args:
        weather_stats: Parsed weatherStats dict from Orion-LD
            (temperature_avg, water_balance, eto, frost_risk).
        ranked_varieties: List of variety dicts from extrapolate_varieties().
        crop: EPPO code or crop name for tolerance lookup in Neo4j.
        dao: GraphDAO instance (for get_heat_tolerance lookup).

    Returns:
        Tuple of (adjusted_varieties, weather_stats, penalties_applied).
        If no penalties are triggered, varieties are returned unchanged.
    """
    if not weather_stats:
        return ranked_varieties, weather_stats, {}

    # ── Determine crop tolerance from Neo4j ──────────────────────────
    tolerance = await _get_crop_tolerance(crop, dao)

    # ── Check weather trigger conditions ─────────────────────────────
    penalties_applied: dict[str, Any] = {
        "drought_penalty": 0.0,
        "heat_penalty": 0.0,
        "frost_penalty": 0.0,
        "total_penalty": 0.0,
        "reasons": [],
        "tolerance": tolerance,
    }

    # Drought check
    deficit_pct = _safe_get(weather_stats, "water_balance.deficit_area_pct")
    if deficit_pct is not None and deficit_pct > 30.0:
        if not tolerance.get("drought_tolerant", False):
            penalties_applied["drought_penalty"] = DROUGHT_PENALTY
            penalties_applied["reasons"].append(
                f"deficit_area_pct={deficit_pct:.1f}% > 30% → "
                f"-{DROUGHT_PENALTY * 100:.0f}% drought penalty"
            )

    # Heat check
    heat_pct = _safe_get(weather_stats, "temperature_avg.heat_stress_pct")
    if heat_pct is not None and heat_pct > 20.0:
        if not tolerance.get("heat_tolerant", False):
            penalties_applied["heat_penalty"] = HEAT_PENALTY
            penalties_applied["reasons"].append(
                f"heat_stress_pct={heat_pct:.1f}% > 20% → "
                f"-{HEAT_PENALTY * 100:.0f}% heat penalty"
            )

    # Frost check
    frost_pct = _safe_get(weather_stats, "frost_risk.high_risk_pct")
    if frost_pct is not None and frost_pct > 50.0:
        if not tolerance.get("frost_tolerant", False):
            penalties_applied["frost_penalty"] = FROST_PENALTY
            penalties_applied["reasons"].append(
                f"high_risk_pct={frost_pct:.1f}% > 50% → "
                f"-{FROST_PENALTY * 100:.0f}% frost penalty"
            )

    total_penalty = (
        penalties_applied["drought_penalty"]
        + penalties_applied["heat_penalty"]
        + penalties_applied["frost_penalty"]
    )

    if total_penalty <= 0:
        penalties_applied["total_penalty"] = 0.0
        penalties_applied["reasons"] = ["No weather penalties triggered"]
        return ranked_varieties, weather_stats, penalties_applied

    penalties_applied["total_penalty"] = round(total_penalty, 2)

    # ── Apply penalties to each variety ─────────────────────────────
    adjusted: list[dict] = []
    for v in ranked_varieties:
        adj = dict(v)
        base_yield = v.get("mean_yield_kg_ha")
        if base_yield is not None and base_yield > 0:
            adj["original_yield_kg_ha"] = base_yield
            adj["mean_yield_kg_ha"] = round(base_yield * (1 - total_penalty), 1)
            adj["weather_penalty"] = {
                "total_penalty": round(total_penalty, 2),
                "drought_penalty_applied": penalties_applied["drought_penalty"] > 0,
                "heat_penalty_applied": penalties_applied["heat_penalty"] > 0,
                "frost_penalty_applied": penalties_applied["frost_penalty"] > 0,
                "drought_tolerant": tolerance.get("drought_tolerant", False),
                "heat_tolerant": tolerance.get("heat_tolerant", False),
                "frost_tolerant": tolerance.get("frost_tolerant", False),
            }
        adjusted.append(adj)

    # Re-sort by adjusted yield (descending)
    adjusted.sort(key=lambda x: x.get("mean_yield_kg_ha") or 0, reverse=True)

    return adjusted, weather_stats, penalties_applied


async def _get_crop_tolerance(crop: str, dao) -> dict[str, Any]:
    """Look up heat/frost/drought tolerance for a crop from Neo4j.

    Uses CropHeatTolerance data via dao.get_heat_tolerance().
    Drought tolerance is inferred from heat damage threshold:
    crops that tolerate very high heat (>= 38°C) are also
    assumed drought-tolerant (C4 plants like sorghum, maize).
    """
    try:
        ht = await dao.get_heat_tolerance(crop)
        if ht:
            heat_damage_c = ht.get("heat_damage_c")
            frost_damage_c = ht.get("frost_damage_c")
            return {
                "drought_tolerant": (
                    heat_damage_c is not None
                    and heat_damage_c >= DROUGHT_TOLERANT_HEAT_C
                ),
                "heat_tolerant": (
                    heat_damage_c is not None
                    and heat_damage_c >= HEAT_TOLERANT_THRESHOLD_C
                ),
                "frost_tolerant": (
                    frost_damage_c is not None
                    and frost_damage_c <= FROST_TOLERANT_THRESHOLD_C
                ),
                "heat_damage_threshold_c": heat_damage_c,
                "frost_damage_threshold_c": frost_damage_c,
            }
    except Exception as exc:
        logger.warning(
            "Failed to get heat tolerance for crop %s: %s", crop, exc,
        )

    # Default: no tolerance info → assume sensitive, apply penalties
    return {
        "drought_tolerant": False,
        "heat_tolerant": False,
        "frost_tolerant": False,
        "heat_damage_threshold_c": None,
        "frost_damage_threshold_c": None,
    }


def _safe_get(d: dict | None, key_path: str, default=None):
    """Safely traverse a nested dict path.

    E.g. _safe_get(stats, "water_balance.deficit_area_pct")
    returns None if any intermediate key is missing.
    """
    if d is None:
        return default
    keys = key_path.split(".")
    current: Any = d
    for k in keys:
        if isinstance(current, dict):
            current = current.get(k)
            if current is None:
                return default
        else:
            return default
    return current
