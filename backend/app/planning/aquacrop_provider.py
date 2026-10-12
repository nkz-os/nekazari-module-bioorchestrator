"""AquaCrop as a yield-estimates provider: which crops it may run for, and its result as estimates.

The simulation itself is the engine team's (``/agriculture/crop-simulation``); this module only
decides whether planning may ask for it (the validated scope) and maps its answer to the
estimates contract. Both functions are pure.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

PROVIDER_ID = "aquacrop"
MAX_SOWING_AHEAD_DAYS = 366  # the engine's pre-season window: [today, today + 366 d]

# (EPPO, regime, country) the engine is validated for. Source: the engine team's validation
# table (2026-10-11): rainfed wheat in Spain, out-of-sample against MAPA 2008-2022, 44
# provinces, RMSE 1.68 t/ha, anomaly r 0.50. Barley (AquaCrop only has spring barley),
# irrigated maize, sunflower and the rest are NOT validated: they never run the engine.
VALIDATED: dict[tuple[str, str, str], dict[str, str]] = {
    ("TRZAX", "secano", "ES"): {"metric": "RMSE 1.68 t/ha, anomaly r 0.50",
                                "scope": "ES, 44 provinces, MAPA 2008–2022"},
}


def aquacrop_gate(crop_eppo: str, regime: str, country: str | None, sowing_date: date | None,
                  today: date) -> str | None:
    """Why AquaCrop must not run for this input (a reason code), or None when it may."""
    if (crop_eppo, regime, country) not in VALIDATED:
        return "not_validated"
    if sowing_date is None:
        return "needs_sowing_date"
    if not today <= sowing_date <= today + timedelta(days=MAX_SOWING_AHEAD_DAYS):
        return "sowing_date_out_of_range"
    return None


def _moisture(basis: Any) -> dict:
    basis = basis if isinstance(basis, dict) else {}
    kind = basis.get("moisture")
    if kind == "commercial":
        return {"basis": "commercial", "pct": basis.get("grain_moisture_pct")}
    if kind == "dry_matter":
        return {"basis": "dry_matter", "pct": None}
    return {"basis": "as_reported", "pct": None}  # an unknown basis is never presented as a known one


def _metric(validation: Any, static: str) -> str:
    """The engine's own out-of-sample metrics when it sends them, else the static text."""
    if isinstance(validation, dict):
        rmse, r = validation.get("rmse_kg_ha"), validation.get("anomaly_r_median")
        if isinstance(rmse, (int, float)) and isinstance(r, (int, float)):
            return f"RMSE {rmse / 1000:.2f} t/ha, anomaly r {r:.2f}"
    return static


def map_result(result: dict, provider_version: str) -> tuple[list[dict], list[dict]]:
    """A finished simulation -> (estimates, unavailable): expected yield first, then the ceiling."""
    static = VALIDATED[("TRZAX", "secano", "ES")]
    moisture = _moisture(result.get("yield_basis"))
    n = (result.get("ensemble") or {}).get("n_years") or 0
    base = {"provider_id": PROVIDER_ID, "provider_version": provider_version, "unit": "t/ha", "n": n,
            "n_kind": "years", "moisture": moisture, "warnings": [], "attributions": [], "graph_id": None}
    estimates: list[dict] = []
    unavailable: list[dict] = []

    expected = result.get("expected_yield")
    rng = expected.get("range_t_ha") if isinstance(expected, dict) else None
    if isinstance(expected, dict) and expected.get("status") == "ok" and isinstance(rng, dict) \
            and rng.get("low") is not None and rng.get("high") is not None:
        validation = (expected.get("calibration") or {}).get("validation")
        estimates.append({
            **base, "basis": "gap_corrected", "percentiles": None,
            "range": {"low": round(rng["low"], 2), "high": round(rng["high"], 2)},
            "coverage": expected.get("coverage"),
            "validation": {"status": "validated", "metric": _metric(validation, static["metric"]),
                           "scope": static["scope"]}})
    elif isinstance(expected, dict) and expected.get("status") == "unavailable":
        unavailable.append({"provider_id": PROVIDER_ID, "reason": f"expected_{expected.get('reason') or 'unknown'}"})
    else:
        unavailable.append({"provider_id": PROVIDER_ID, "reason": "expected_missing"})

    ceiling = result.get("yield_t_ha")
    if isinstance(ceiling, dict) and all(isinstance(ceiling.get(k), (int, float)) for k in ("p10", "p50", "p90")):
        estimates.append({
            **base, "basis": "water_limited", "range": None, "coverage": None,
            "percentiles": {k: round(ceiling[k], 2) for k in ("p10", "p50", "p90")},
            "validation": {"status": "not_applicable"}})  # a ceiling, not a forecast
    else:
        unavailable.append({"provider_id": PROVIDER_ID, "reason": "water_limited_missing"})
    return estimates, unavailable
