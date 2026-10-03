"""Pure rules of the crop recommendation contract (no I/O).

Kept apart from the DAO so ranking and the evidence shape can be tested and
reasoned about without Neo4j, and so the same output feeds the UI expert mode
and the agent unchanged.
"""

from __future__ import annotations

import hashlib
import json

from app.services.crop_reference import get_season_slots

RESISTANT_THRESHOLD = 0.7
MIN_REFERENCE_TRIALS = 3
LOW_TRIAL_COUNT = 3
_SLOT_TO_SOWING = {"winter": "autumn", "summer": "spring"}


def relative_yield(expected: float | None, median: float | None, n_ref: int) -> tuple[float | None, str | None]:
    if expected is None:
        return None, "no_expected_yield"
    if median is None or n_ref < MIN_REFERENCE_TRIALS:
        return None, "reference_too_small"
    if median == 0:
        return None, "reference_zero"
    return round((expected / median - 1) * 100, 1), None


def stability_cv(mean: float | None, sd: float | None, n: int) -> tuple[float | None, str | None]:
    if mean is None or sd is None or n < 2 or mean == 0:
        return None, "cv_undefined"
    return round(sd / mean, 3), None


def disease_summary(disease_scores: dict) -> dict:
    values = [v.get("value") for v in (disease_scores or {}).values() if isinstance(v, dict)]
    values = [v for v in values if isinstance(v, (int, float))]
    return {"resistant": sum(1 for v in values if v >= RESISTANT_THRESHOLD), "total": len(values)}


def sowing_info(eppo: str, koppen: str | None, table_rows: list[dict]) -> dict:
    for row in table_rows:
        if row["eppo"] == eppo and (koppen is None or koppen in row.get("koppen", [])):
            return {
                "sowing_type": row["sowing_type"],
                "sowing_window": {"start_month": row["start_month"], "end_month": row["end_month"]},
                "cycle_days": row.get("cycle_days"),
                "source": row["source"],
            }
    slots = get_season_slots(eppo)
    sowing_type = _SLOT_TO_SOWING[next(iter(slots))] if len(slots) == 1 else None
    return {"sowing_type": sowing_type, "sowing_window": None, "cycle_days": None,
            "source": "crop_season_slot"}


def count_blockers(rec: dict) -> int:
    s = rec["suitability"]
    return int(s["soil"]["level"] == "unsuitable") + int(s["frost"]["level"] == "risk")


def rank_recommendations(recs: list[dict]) -> list[dict]:
    def key(rec: dict):
        rel = rec["fit"]["relative_yield_pct"]
        return (count_blockers(rec), rel is None, -(rel or 0.0), -rec["yield"]["n_trials"], rec["crop"]["eppo"])
    return sorted(recs, key=key)


def recommendation_id(conditions: dict, eppo: str, sowing_type: str | None) -> str:
    payload = json.dumps({"c": conditions, "e": eppo, "s": sowing_type}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _trust_level(n_trials: int, confidence: str | None) -> str:
    if n_trials < LOW_TRIAL_COUNT:
        return "low"
    if confidence == "high" and n_trials >= 10:
        return "high"
    return "medium"


def build_recommendation(*, eppo, scientific_name, conditions, varieties, reference, soil_verdict,
                         water, frost_level, sowing, data_gaps_extra, assumptions) -> dict | None:
    if not varieties:
        return None
    best = varieties[0]
    # Numeric trials only: a trial without a yield value backs no yield figure.
    n_trials = int(best.get("numeric_yield_count") or 0)
    rel, rel_gap = relative_yield(best.get("mean_yield_kg_ha"), reference.get("median_kg_ha"),
                                  int(reference.get("n_trials") or 0))
    cv, cv_gap = stability_cv(best.get("mean_yield_kg_ha"), best.get("stddev_yield_kg_ha"), n_trials)
    gaps = [g for g in (rel_gap, cv_gap) if g] + list(data_gaps_extra)
    if n_trials < LOW_TRIAL_COUNT:
        gaps.append("low_trial_count")
    if sowing["source"] == "crop_season_slot":
        gaps.append("sowing_window_unavailable")
    soil_level = soil_verdict.get("verdict", "unknown")
    soil_reason = soil_verdict.get("reason")
    sites = sorted({s for v in varieties for s in (v.get("trial_sites") or [])})
    years = sorted({y for v in varieties for y in (v.get("trial_years") or [])})
    sources = sorted({s for v in varieties for s in (v.get("source_ids") or [])})
    return {
        "recommendation_id": recommendation_id(conditions, eppo, sowing["sowing_type"]),
        "crop": {"eppo": eppo, "scientific_name": scientific_name, "sowing_type": sowing["sowing_type"]},
        "fit": {"relative_yield_pct": rel, "stability_cv": cv, "reference": reference},
        "yield": {
            "expected_kg_ha": best.get("mean_yield_kg_ha"),
            "interval": [best.get("min_yield_kg_ha"), best.get("max_yield_kg_ha")],
            "interval_method": "observed_range",
            "sd": best.get("stddev_yield_kg_ha"),
            "n_trials": n_trials,
            "n_sites": len(best.get("trial_sites") or []),
        },
        "suitability": {
            "soil": {"level": soil_level,
                     "warnings": [soil_reason] if soil_reason and soil_level != "suitable" else []},
            "water": water or {"level": "unknown", "etc_mm": None},
            "frost": {"level": frost_level},
        },
        "season": {"sowing_window": sowing["sowing_window"], "cycle_days": sowing["cycle_days"],
                   "source": sowing["source"]},
        "trust": {"level": _trust_level(n_trials, best.get("confidence")), "data_gaps": gaps},
        "varieties": [
            {"variety": v.get("variety"), "variety_uri": v.get("variety_uri"),
             "expected_kg_ha": v.get("mean_yield_kg_ha"),
             "interval": [v.get("min_yield_kg_ha"), v.get("max_yield_kg_ha")],
             "n_trials": int(v.get("numeric_yield_count") or 0),
             "disease_summary": disease_summary(v.get("disease_scores") or {})}
            for v in varieties[:5]
        ],
        # trial_count = all trials (numeric or not) of the listed varieties;
        # years is null when no trial year is known.
        "evidence": {"trial_count": sum(int(v.get("trial_count") or 0) for v in varieties),
                     "sources": sources, "sites": sites,
                     "years": [years[0], years[-1]] if years else None},
        "assumptions": assumptions,
    }
