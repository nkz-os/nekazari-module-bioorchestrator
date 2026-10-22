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


# How the crop was sown, as the ESYRCE records it for cereals (MAPA, ESYRCE 2022, 3.6):
# "Siembra directa (D)" on the previous crop's stubble, "Siembra tradicional (N)" after tillage.
SOWING_METHODS = ("direct", "conventional")


def operation_sowing_day(op: dict) -> date | None:
    """Day of a completed sowing operation (endedAt, else startedAt); None for anything else."""
    if str(_unwrap(op.get("operationType")) or "").lower() != "sowing":
        return None
    if str(_unwrap(op.get("status")) or "").lower() != "completed":
        return None
    return _parse_date(op.get("endedAt")) or _parse_date(op.get("startedAt"))


def resolve_sowing_date(operations: list[dict], today: date) -> date:
    """Most recent completed sowing not after ``today``.

    What happened counts, never a plan (the platform's crop-cycle rule): only sowing
    operations with status "completed", dated by endedAt, else startedAt.
    """
    candidates = [d for d in (operation_sowing_day(op) for op in operations or [])
                  if d is not None and d <= today]
    if not candidates:
        raise SimInputError("no completed sowing operation on or before today")
    return max(candidates)


def sowing_method_from_operations(operations: list[dict], sowing_date: date) -> tuple[str | None, list[str]]:
    """``sowingMethod`` of the completed sowing(s) on ``sowing_date``, with warnings.

    None when no such sowing records a method. A value outside ``SOWING_METHODS`` is
    ignored (never interpreted); sowings of that day that disagree give None too.
    """
    found: set[str] = set()
    warns: list[str] = []
    for op in operations or []:
        if operation_sowing_day(op) != sowing_date:
            continue
        raw = _unwrap(op.get("sowingMethod"))
        if raw is None:
            continue
        if raw not in SOWING_METHODS:
            warns.append(f"sowing method: unknown value {raw!r} on the sowing of {sowing_date} ignored")
            continue
        found.add(raw)
    if len(found) > 1:
        warns.append(f"sowing method: completed sowings on {sowing_date} disagree "
                     f"({', '.join(sorted(found))}); bare soil (conventional) assumed")
        return None, warns
    return (found.pop() if found else None), warns


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
