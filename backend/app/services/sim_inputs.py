"""Pure resolvers for the real inputs of the crop simulation.

No defaults are invented: missing or inconsistent data raises SimInputError.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any


class SimInputError(ValueError):
    """A required simulation input is missing or invalid."""


def _unwrap(raw: Any) -> Any:
    """Unwrap NGSI-LD Property and {"@type","@value"} forms to a plain value."""
    if isinstance(raw, dict):
        if "value" in raw:
            return _unwrap(raw["value"])
        if "@value" in raw:
            return raw["@value"]
        return None
    return raw


def _parse_date(raw: Any) -> date | None:
    v = _unwrap(raw)
    if not isinstance(v, str) or not v.strip():
        return None
    try:
        return datetime.fromisoformat(v.strip().replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(v.strip()[:10])
        except ValueError:
            return None


def resolve_sowing_date(operations: list[dict], today: date) -> date:
    """Most recent real sowing date not after ``today``.

    Only sowing operations that are not status "suggested" count. The date is
    startedAt, else plannedDate, else plannedStartAt.
    """
    candidates: list[date] = []
    for op in operations or []:
        op_type = str(_unwrap(op.get("operationType")) or "").lower()
        if op_type != "sowing":
            continue
        if str(_unwrap(op.get("status")) or "").lower() == "suggested":
            continue
        d = None
        for key in ("startedAt", "plannedDate", "plannedStartAt"):
            d = _parse_date(op.get(key))
            if d is not None:
                break
        if d is not None and d <= today:
            candidates.append(d)
    if not candidates:
        raise SimInputError("no sowing operation with a date on or before today")
    return max(candidates)


def _num(h: dict, key: str) -> float | None:
    v = _unwrap(h.get(key))
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v)


def soil_layers_from_summary(summary: dict) -> list[dict]:
    """Map the Soil module summary horizons to ordered simulation layers.

    Units: fc/wp/sat in cm3/cm3; ksatSaturated mm/h converted to mm/day.
    ``sat`` is None unless the horizon carries ``saturation``.
    """
    horizons = _unwrap((summary or {}).get("horizons"))
    if not isinstance(horizons, list) or not horizons:
        raise SimInputError("soil summary has no horizons")
    source = _unwrap((summary or {}).get("dataSource")) or "soil_module"

    layers: list[dict] = []
    for h in horizons:
        top, bottom = _num(h, "depthFrom"), _num(h, "depthTo")
        if top is None or bottom is None or bottom <= top or top < 0:
            raise SimInputError(f"soil horizon has invalid depths: {h.get('depthFrom')}-{h.get('depthTo')}")
        label = f"{top:g}-{bottom:g} cm"
        fc, wp = _num(h, "fieldCapacity"), _num(h, "wiltingPoint")
        if fc is None:
            raise SimInputError(f"soil horizon {label} missing fieldCapacity")
        if wp is None:
            raise SimInputError(f"soil horizon {label} missing wiltingPoint")
        if not (0 < wp < fc <= 1):
            raise SimInputError(f"soil horizon {label} invalid water content: need 0 < wp < fc <= 1 (wp={wp}, fc={fc})")
        ksat_h = _num(h, "ksatSaturated")
        if ksat_h is None or ksat_h <= 0:
            raise SimInputError(f"soil horizon {label} missing or non-positive ksatSaturated")
        sat = _num(h, "saturation")
        if sat is not None and not (fc < sat <= 1):
            raise SimInputError(f"soil horizon {label} invalid saturation {sat} (need fc < sat <= 1)")
        layers.append({
            "top_cm": top, "bottom_cm": bottom,
            "thickness_m": (bottom - top) / 100.0,
            "fc": fc, "wp": wp,
            "ksat_mm_day": ksat_h * 24.0,
            "sat": sat, "source": source,
        })
    return sorted(layers, key=lambda l: l["top_cm"])


def hydraulic_props_from_layers(layers: list[dict]) -> dict:
    """Collapse soil layers into the single-profile props the engine expects.

    theta_fc / theta_wp / theta_sat: thickness-weighted arithmetic mean.
    k_sat_mm_d: thickness-weighted harmonic mean (flow in series across layers).
    awc_mm_per_metre: weighted mean of (fc - wp) * 1000.
    theta_sat is None unless every layer carries a saturation value.
    """
    if not layers:
        raise SimInputError("no soil layers")
    total = sum(l["thickness_m"] for l in layers)
    if total <= 0:
        raise SimInputError("soil layers have zero total thickness")

    def wmean(key: str) -> float:
        return sum(l[key] * l["thickness_m"] for l in layers) / total

    sat = wmean("sat") if all(l.get("sat") is not None for l in layers) else None
    return {
        "theta_fc": wmean("fc"),
        "theta_wp": wmean("wp"),
        "theta_sat": sat,
        "k_sat_mm_d": total / sum(l["thickness_m"] / l["ksat_mm_day"] for l in layers),
        "awc_mm_per_metre": sum((l["fc"] - l["wp"]) * l["thickness_m"] for l in layers) / total * 1000.0,
        "profile_depth_cm": layers[-1]["bottom_cm"] - layers[0]["top_cm"],
    }
