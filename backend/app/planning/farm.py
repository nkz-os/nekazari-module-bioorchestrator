"""Farm plan matrix (pure): parcels x campaigns, regime, warnings and summary.

ASSUMPTION: the default campaign to plan rolls over in July (after the winter-cereal
harvest). The user can always pick another one.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import date

from app.planning.history import (
    cell_from_cycle,
    cycle_campaigns,
    cycles_of,
    first_campaign,
    history_complete,
    past_crops,
    plain,
    species_slug,
)
from app.planning.rotation import rotation_reasons
from app.species_registry import get_species_info

ROLLOVER_MONTH = 7
_IRRIGATED_WORDS = ("drip", "sprinkler", "flood", "pivot", "irrigat", "regad")


def default_target_campaign(today: date) -> int:
    return today.year + 1 if today.month >= ROLLOVER_MONTH else today.year


def campaign_columns(target: int) -> list[int]:
    return [target - 3, target - 2, target - 1, target]


def _regime_word(value) -> str | None:
    text = str(plain(value) or "").lower().strip()
    if not text:
        return None
    return "regadío" if any(w in text for w in _IRRIGATED_WORDS) else "secano"


def parcel_regime(parcel: dict, target_crops: list[dict]) -> tuple[str | None, str]:
    for crop in target_crops:
        regime = _regime_word(crop.get("irrigationRegime"))
        if regime:
            return regime, "crop"
    for attr in ("irrigationSystemType", "irrigationRegime"):
        regime = _regime_word(parcel.get(attr))
        if regime:
            return regime, f"parcel:{attr}"
    return None, "unknown"


def _area(parcel: dict) -> float | None:
    raw = plain(parcel.get("area"))
    if isinstance(raw, bool):  # float(True) would read as 1 ha
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    # NaN/inf do not survive JSON and a negative area is not a measurement.
    return value if math.isfinite(value) and value >= 0 else None


def _farm_id(parcel: dict) -> str | None:
    value = plain(parcel.get("hasAgriFarm") or parcel.get("refAgriFarm"))
    if isinstance(value, dict):
        value = value.get("object")
    return value if isinstance(value, str) else None


def _rotation_warnings(payload: dict, target: int, constraints) -> list[dict]:
    history = past_crops(payload, target)
    warnings = []
    for cycle in cycles_of(payload):
        slug = species_slug(cycle.get("species"))
        # Rotation is checked where a crop is sown, not for a perennial that continues.
        if slug and first_campaign(cycle) == target:
            for reason in rotation_reasons(history, slug, target, constraints):
                if reason["code"] == "rotation_restriction":
                    warnings.append({"code": "rotation_restriction", "campaign": target,
                                     "params": {**reason["params"], "species": slug}})
    return warnings


def parcel_row(parcel: dict, crops_by_id: dict, payload: dict | None, target: int, constraints) -> dict:
    columns = campaign_columns(target)
    cells: dict[str, list[dict]] = {str(c): [] for c in columns}
    target_crops, warnings = [], []
    for cycle in cycles_of(payload):
        crop = crops_by_id.get(cycle.get("crop_id"))
        # An annual sits in the campaign it ends in; a perennial fills every campaign it occupies.
        for label in cycle_campaigns(cycle, until=target):
            if label in columns:
                cells[str(label)].append(cell_from_cycle(cycle, crop))
                if label == target and crop:
                    target_crops.append(crop)
    regime, regime_source = parcel_regime(parcel, target_crops)
    if payload is None:
        warnings.append({"code": "cycles_unavailable", "campaign": None, "params": {}})
    elif not constraints or not history_complete(payload):
        # No rotation rules, or a history that cannot be fully placed: silence must not read as "rotation OK".
        warnings.append({"code": "rotation_unknown", "campaign": target, "params": {}})
    else:
        warnings.extend(_rotation_warnings(payload, target, constraints))
    if regime is None:
        warnings.append({"code": "regime_missing", "campaign": target, "params": {}})
    return {"parcel_id": parcel["id"], "name": plain(parcel.get("name")), "area_ha": _area(parcel),
            "farm_id": _farm_id(parcel), "regime": regime, "regime_source": regime_source,
            "cycles_available": payload is not None, "cells": cells, "warnings": warnings}


def farm_payload(parcels: list[dict], farms: list[dict], crops_by_id: dict, cycles: dict,
                 target: int, constraints) -> dict:
    rows = [parcel_row(p, crops_by_id, cycles.get(p["id"]), target, constraints) for p in parcels]
    area: dict[str | None, float] = defaultdict(float)
    for row in rows:
        for cell in row["cells"][str(target)]:
            if cell["role"] == "main_crop" and row["area_ha"]:
                area[cell["species"]] += row["area_ha"]
    names = {f["id"]: plain(f.get("name")) for f in farms}
    linked = sorted({r["farm_id"] for r in rows if r["farm_id"]})
    return {
        "campaign": target, "campaigns": campaign_columns(target),
        "farms": [{"farm_id": f, "name": names.get(f)} for f in linked] or [{"farm_id": None, "name": None}],
        "parcels": rows,
        "summary": {
            "area_by_crop": [
                {"species": s, "names": dict(((get_species_info(s) if s else None) or {}).get("common_names") or {}),
                 "area_ha": round(a, 2)}
                for s, a in sorted(area.items(), key=lambda kv: -kv[1])],
            "undecided": sum(1 for r in rows if not r["cells"][str(target)]),
            "rotation_warnings": sum(1 for r in rows for w in r["warnings"] if w["code"] == "rotation_restriction"),
        },
    }
