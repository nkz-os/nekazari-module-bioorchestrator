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
# table (2026-10-13, soil evaporation to 0.15 m): rainfed wheat in Spain, out-of-sample against
# MAPA 2008-2022, 44 provinces, RMSE 1.65 t/ha, anomaly r 0.48. Barley (simulated as BarleyES),
# irrigated maize, sunflower and the rest are NOT validated: they never run the engine.
VALIDATED: dict[tuple[str, str, str], dict[str, str]] = {
    ("TRZAX", "rainfed", "ES"): {"metric": "RMSE 1.65 t/ha, anomaly r 0.48",
                                "scope": "ES, 44 provinces, MAPA 2008–2022"},
}


# Extra (EPPO, regime, country) combinations opened for the in-season forecast ONLY, each with the
# scope sentence shown on its estimate. They return a water-limited ceiling labelled `unvalidated`
# and never an expected harvest. Planning's VALIDATED, its gate and its providers are not affected.
SCOPE_BARLEY = "Water-limited ceiling; not an expected harvest; not validated."
SCOPE_DURUM = "Simulated with soft-wheat parameters; water-limited ceiling; not an expected harvest; not validated."
SCOPE_IRRIGATED = "Potential under full irrigation; no regional calibration; not validated."
SCOPE_DURUM_IRRIGATED = ("Simulated with soft-wheat parameters; potential under full irrigation; "
                         "no regional calibration; not validated.")
UNVALIDATED_IN_SEASON: dict[tuple[str, str, str], str] = {
    ("HORVX", "rainfed", "ES"): SCOPE_BARLEY,
    ("TRZDU", "rainfed", "ES"): SCOPE_DURUM,
    ("TRZAX", "irrigated", "ES"): SCOPE_IRRIGATED,
    ("TRZDU", "irrigated", "ES"): SCOPE_DURUM_IRRIGATED,
    ("HORVX", "irrigated", "ES"): SCOPE_IRRIGATED,
}


# What the model figure is worth today (measured offline against official provincial yields):
# it does not improve on the provincial normal, so it is never shown as validated.
MODEL_SCOPE = "what the model sees this season; today it does not improve on the provincial normal"
MODEL_METRIC = ("RMSE 1.6 t/ha; year-to-year r 0.14 (1 Apr), 0.34 (1 May), 0.49 (complete season); "
                "the provincial normal: 0.76 t/ha")


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


def poll_in_scope(crop_eppo: str, regime: str, sowing_date: date, today: date) -> bool:
    """Whether a light poll (no parcel lookup) may read a job: the gate, for some validated country.

    The parcel's own country was gated when its job was started, and the job id covers the parcel.
    """
    return any(aquacrop_gate(crop_eppo, regime, country, sowing_date, today) is None
               for _, _, country in VALIDATED)


def _moisture(basis: Any) -> dict:
    basis = basis if isinstance(basis, dict) else {}
    kind = basis.get("moisture")
    if kind == "commercial":
        return {"basis": "commercial", "pct": basis.get("grain_moisture_pct")}
    if kind == "dry_matter":
        return {"basis": "dry_matter", "pct": None}
    return {"basis": "as_reported", "pct": None}  # an unknown basis is never presented as a known one


def map_result(result: dict, provider_version: str) -> tuple[list[dict], list[dict]]:
    """A finished simulation -> (estimates, unavailable): expected yield first, then the ceiling."""
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
        estimates.append({
            **base, "basis": "gap_corrected", "percentiles": None,
            "range": {"low": round(rng["low"], 2), "high": round(rng["high"], 2)},
            "coverage": expected.get("coverage"),
            "validation": {"status": "experimental", "metric": MODEL_METRIC, "scope": MODEL_SCOPE}})
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


# Skill of the in-season forecast by issue date, measured offline against official provincial yields
# (rainfed soft wheat, ES, 44 provinces, campaigns 2008-2022). The forecast ranks areas correctly from
# sowing, but the year-to-year signal is close to zero before April and only reaches the complete-season
# value in June; the error of the level does not shrink during the season. Provincial statistics are not
# parcels, so the estimate stays experimental while the season runs.
IN_SEASON_METRIC = ("year-to-year r by issue date: 0.14 (1 Apr), 0.34 (1 May), 0.45 (1 Jun), "
                    "0.49 (complete season); RMSE 1.6 t/ha at every date")


def map_in_season(result: dict, provider_version: str,
                  unvalidated_scope: str | None = None) -> tuple[list[dict], list[dict]]:
    """A finished in-season simulation -> (estimates, unavailable).

    The expected yield is ``experimental`` while the season runs: it was validated on complete
    seasons, and the skill by forecast date is not measured yet (owner, 2026-10-13). A complete
    season (observed weather only) keeps the validated status; its single water-limited ceiling
    is reported as P10 = P50 = P90 and ``n`` is one season.

    With ``unvalidated_scope`` (an opened combination) the result is a ceiling only: any
    gap-corrected estimate is dropped and every remaining estimate is labelled ``unvalidated``.
    """
    complete = result.get("status") == "complete"
    ceiling = result.get("yield_t_ha")
    if isinstance(ceiling, (int, float)) and not isinstance(ceiling, bool):
        result = {**result, "yield_t_ha": {"p10": ceiling, "p50": ceiling, "p90": ceiling}}
    estimates, unavailable = map_result(result, provider_version)
    for e in estimates:
        if complete:
            e["n"] = 1
        elif e["basis"] == "gap_corrected":
            e["validation"] = {"status": "experimental", "metric": IN_SEASON_METRIC, "scope": MODEL_SCOPE}
    if unvalidated_scope is None:
        return estimates, unavailable
    kept = [e for e in estimates if e["basis"] != "gap_corrected"]
    if len(kept) != len(estimates):
        unavailable = [*unavailable, {"provider_id": PROVIDER_ID, "reason": "expected_crop_not_calibrated"}]
    for e in kept:
        e["validation"] = {"status": "unvalidated", "metric": None, "scope": unvalidated_scope}
    return kept, unavailable
