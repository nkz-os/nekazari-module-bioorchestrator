"""Fit of a species to a parcel for one campaign (pure): a verdict with reasons, never a yield.

- no_fit: a blocker. Soil unsuitable, or winter frost risk for a crop that stands
  through winter (autumn-sown or perennial).
- caveats: anything marginal or unknown, a rotation restriction, a high rainfed water
  deficit, or an undeclared regime. Missing data never yields ``fits``.
- fits: every evaluated gate known and suitable (a rotation benefit is information).

The winter minimum is compared only for crops in the field in winter: a spring or summer
crop is not exposed to the coldest month, and testing it there would block maize in
every climate with frost. Late spring frost is not evaluated in v1.
"""
from __future__ import annotations

from dataclasses import dataclass

FROST_MARGIN_C = 5.0  # the recommender's default (ASSUMPTION there: conservative, editable)
DEFAULT_SEASON_DAYS = 180  # the recommender's _DEFAULT_GROWING_SEASON_DAYS
WINTER_SOWING_TYPES = frozenset({"autumn", "perennial"})
NON_WINTER_SOWING_TYPES = frozenset({"spring", "summer"})  # the only types exempt from the winter check
KNOWN_SOIL_VERDICTS = frozenset({"suitable", "marginal", "unsuitable"})
WATER_HIGH_DEFICIT_MM = 150.0  # the recommender's "high" band: deficit / 100 >= 1.5
RAIN_EFFECTIVE_FRACTION = 0.7  # the recommender's effective-rain factor

REGIME_MISSING = {"code": "regime_missing", "gate": "data", "params": {}}
ROTATION_UNKNOWN = {"code": "rotation_unknown", "gate": "rotation", "params": {}}
CLIMATE_UNKNOWN = {"code": "climate_unknown", "gate": "data", "params": {}}  # a caveat on every species, not a blocker
MULTI_YEAR = {"code": "multi_year", "gate": "data", "params": {}}  # a herbaceous perennial occupies several campaigns
BLOCKERS = frozenset({"soil_unsuitable", "soil_ph_unsuitable", "soil_texture_unsuitable",
                      "soil_drainage_unsuitable", "frost_risk"})
INFORMATIVE = frozenset({"rotation_benefit", "multi_year"})
VERDICT_ORDER = {"fits": 0, "caveats": 1, "no_fit": 2}


@dataclass(frozen=True)
class ParcelClimate:
    coldest_month_min_c: float | None
    annual_rainfall_mm: float | None
    annual_et0_mm: float | None


def soil_reasons(gate: dict, *, parcel_soil_available: bool, tolerance_available: bool) -> list[dict]:
    if not tolerance_available:
        return [{"code": "soil_unknown", "gate": "soil", "params": {"why": "no_tolerance"}}]
    if not parcel_soil_available:
        return [{"code": "soil_unknown", "gate": "soil", "params": {"why": "no_parcel_soil"}}]
    if gate.get("verdict") not in KNOWN_SOIL_VERDICTS:
        return [{"code": "soil_unknown", "gate": "soil", "params": {"why": "not_comparable"}}]
    out = []
    for dim in ("ph", "texture", "drainage"):
        detail = gate.get(dim) or {}
        if detail.get("verdict") in ("marginal", "unsuitable"):
            params = {"value": detail.get("value")}
            if dim == "ph":
                params.update(min=detail.get("min"), max=detail.get("max"))
            out.append({"code": f"soil_{dim}_{detail['verdict']}", "gate": "soil", "params": params})
    if out:
        return out
    # No dimension explains the overall verdict, so the overall verdict itself decides.
    if gate["verdict"] == "unsuitable":
        return [{"code": "soil_unsuitable", "gate": "soil", "params": {}}]
    if gate["verdict"] == "marginal":
        return [{"code": "soil_marginal", "gate": "soil", "params": {}}]
    return []


def frost_reasons(sowing_type: str | None, coldest_min: float | None, frost_damage_c: float | None,
                  margin: float = FROST_MARGIN_C) -> list[dict]:
    if sowing_type in NON_WINTER_SOWING_TYPES:
        return []
    if sowing_type not in WINTER_SOWING_TYPES:
        return [{"code": "frost_unknown", "gate": "frost", "params": {"why": "season_unknown"}}]
    if coldest_min is None:
        return [{"code": "frost_unknown", "gate": "frost", "params": {"why": "no_climate"}}]
    if frost_damage_c is None:
        return [{"code": "frost_unknown", "gate": "frost", "params": {"why": "no_tolerance"}}]
    if coldest_min - margin <= frost_damage_c:
        return [{"code": "frost_risk", "gate": "frost", "params": {
            "coldest_min_c": coldest_min, "threshold_c": frost_damage_c, "margin_c": margin}}]
    return []


def water_reasons(regime: str | None, climate: ParcelClimate, season_days: int | None) -> list[dict]:
    if regime != "secano":
        return []
    rain, et0 = climate.annual_rainfall_mm, climate.annual_et0_mm
    if rain is None or not et0:
        return [{"code": "water_unknown", "gate": "water", "params": {}}]
    use_default = season_days is None or season_days <= 0
    days = DEFAULT_SEASON_DAYS if use_default else season_days
    deficit = max(0.0, (days / 365) * et0 - rain * RAIN_EFFECTIVE_FRACTION)
    if deficit >= WATER_HIGH_DEFICIT_MM:
        return [{"code": "water_deficit_high", "gate": "water", "params": {
            "deficit_mm": round(deficit), "season_days_default": use_default}}]
    return []


def verdict_of(reasons: list[dict]) -> str:
    codes = {r["code"] for r in reasons}
    if codes & BLOCKERS:
        return "no_fit"
    if codes - INFORMATIVE:
        return "caveats"
    return "fits"
