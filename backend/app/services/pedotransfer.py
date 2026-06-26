"""Pedotransfer functions — soil texture → hydraulic properties.

Implements Saxton-Rawls (2006) equations to derive:
  - θ_sat  (saturated water content, m³/m³)
  - θ_fc   (field capacity, m³/m³)
  - θ_wp   (wilting point, m³/m³)
  - K_sat  (saturated hydraulic conductivity, mm/day)
  - AWC    (available water capacity = θ_fc - θ_wp, mm/mm)

from sand %, clay %, organic matter %, and compaction.

Reference:
  Saxton, K.E. & Rawls, W.J. (2006). Soil Water Characteristic Estimates
  by Texture and Organic Matter for Hydrologic Solutions.
  Soil Science Society of America Journal, 70:1569-1578.

Usage:
    from app.services.pedotransfer import texture_to_hydraulic_props
    props = texture_to_hydraulic_props(sand_pct=40, clay_pct=25, om_pct=2.0)
    # → {theta_sat: 0.46, theta_fc: 0.28, theta_wp: 0.14, k_sat_mm_d: 250, awc_mm_mm: 0.14}
"""

from __future__ import annotations

import math


def texture_to_hydraulic_props(
    sand_pct: float,
    clay_pct: float,
    om_pct: float = 2.0,
    compaction: float = 1.0,
) -> dict:
    """Derive soil hydraulic properties from texture using Saxton-Rawls (2006).

    Args:
        sand_pct: Sand fraction (%, 0-100)
        clay_pct: Clay fraction (%, 0-100). Silt = 100 - sand - clay.
        om_pct: Organic matter (%, 0-10, default 2.0)
        compaction: Density factor (0.9=lose, 1.0=normal, 1.1=compact)

    Returns:
        Dict with:
          - theta_sat (m³/m³): saturated water content
          - theta_fc (m³/m³): field capacity at 33 kPa
          - theta_wp (m³/m³): permanent wilting point at 1500 kPa
          - k_sat_mm_d (mm/day): saturated hydraulic conductivity
          - awc_mm_mm (mm/mm): available water capacity (= theta_fc - theta_wp)
          - awc_mm (mm): AWC per metre of soil depth
    """
    # Normalise
    sand = max(0.1, min(99.9, sand_pct)) / 100.0
    clay = max(0.1, min(99.9, clay_pct)) / 100.0
    silt = max(0.0, 1.0 - sand - clay)
    om = max(0.1, min(10.0, om_pct)) / 100.0

    # ── θ_1500 (wilting point, 1500 kPa) ──
    # Saxton-Rawls Eq [1]
    theta_1500 = (
        -0.024 * sand
        + 0.487 * clay
        + 0.006 * om
        + 0.005 * (sand * om)
        - 0.013 * (clay * om)
        + 0.068 * (sand * clay)
        + 0.031
    )
    # Apply first-order correction (Eq [2])
    theta_1500 = theta_1500 + (0.14 * theta_1500 - 0.02)

    # ── θ_33 (field capacity, 33 kPa) ──
    # Saxton-Rawls Eq [3]
    theta_33 = (
        -0.251 * sand
        + 0.195 * clay
        + 0.011 * om
        + 0.006 * (sand * om)
        - 0.027 * (clay * om)
        + 0.452 * (sand * clay)
        + 0.299
    )
    theta_33 = theta_33 + (1.283 * theta_33 ** 2 - 0.374 * theta_33 - 0.015)

    # ── θ_sat (saturation) ──
    # Saxton-Rawls Eq [4]: θ_s-33 = θ_33 + θ_(s-33)
    theta_s_minus_33 = (
        0.278 * sand
        + 0.034 * clay
        + 0.022 * om
        - 0.018 * (sand * om)
        - 0.027 * (clay * om)
        - 0.584 * (sand * clay)
        + 0.078
    )
    theta_s_minus_33 = theta_s_minus_33 + (
        0.636 * theta_s_minus_33 - 0.107
    )
    theta_sat = theta_33 + theta_s_minus_33 - 0.097 * sand + 0.043

    # ── K_sat (saturated hydraulic conductivity, mm/h → mm/day) ──
    # Saxton-Rawls Eq [5] for λ (pore size distribution index)
    B = (math.log(1500.0) - math.log(33.0)) / (math.log(theta_33) - math.log(theta_1500))
    lam = 1.0 / B

    # Saxton-Rawls Eq [6] for K_sat (mm/h)
    k_sat_mm_h = 1930.0 * (theta_sat - theta_33) ** (3.0 - lam)
    k_sat_mm_d = k_sat_mm_h * 24.0

    # ── Apply compaction / OM adjustments ──
    # OM increases AWC, compaction reduces K_sat
    theta_fc = theta_33 * compaction
    theta_wp = theta_1500
    awc_mm_mm = max(0.01, theta_fc - theta_wp)

    return {
        "theta_sat": round(theta_sat, 3),
        "theta_fc": round(theta_fc, 3),
        "theta_wp": round(theta_wp, 3),
        "k_sat_mm_d": round(k_sat_mm_d, 1),
        "awc_mm_mm": round(awc_mm_mm, 3),
        "awc_mm_per_metre": round(awc_mm_mm * 1000, 1),
        "bulk_density_estimated": round(1.6 - 0.1 * (sand_pct / 100), 2),  # rough est
        "method": "Saxton-Rawls (2006)",
        "inputs": {"sand_pct": sand_pct, "clay_pct": clay_pct, "om_pct": om_pct},
    }


def texture_to_awc_mm(
    sand_pct: float,
    clay_pct: float,
    rooting_depth_m: float = 1.0,
    om_pct: float = 2.0,
) -> float:
    """Convenience: return total AWC in mm for a given rooting depth.

    Args:
        sand_pct, clay_pct: texture fractions
        rooting_depth_m: effective rooting depth in metres (default 1.0)
        om_pct: organic matter %

    Returns:
        Total available water capacity in mm
    """
    props = texture_to_hydraulic_props(sand_pct, clay_pct, om_pct)
    return round(props["awc_mm_per_metre"] * rooting_depth_m, 1)
