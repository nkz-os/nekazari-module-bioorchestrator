"""Pure rules of the crop recommendation contract (no I/O).

Kept apart from the DAO so ranking and the evidence shape can be tested and
reasoned about without Neo4j, and so the same output feeds the UI expert mode
and the agent unchanged.

Evidence policy contract (additions of the evidence-policy change; the rules live in
``app.graph.evidence_policy``, mirrored in ``src/types/recommend.ts``):

- Response: ``evidence_policy`` (policy version) and ``conditions.purpose``.
- Request ``purpose`` = ``main`` (default: each crop's main harvested product — grain, fruit,
  kernel, tuber; records positively classified as forage are left out) or ``forage``
  (forage records only). It is part of the cache key and of the recommendation id when not
  ``main``. Evidence endpoint: same ``purpose``, plus ``tier``.
- ``evidence.tier``: ``field`` (numbers from field trials at analog field sites) or
  ``regional`` (the crop has no numeric field evidence; the numbers come from aggregate
  national/regional pseudo-sites of the same climate — never mixed with field numbers).
  A regional recommendation has trust capped at ``low``, the data gaps
  ``regional_evidence_only`` and ``regional_not_comparable`` (no relative yield: a registry
  average is not comparable to the field-trial reference), ``fit.reference.scope`` =
  ``regional`` and a null median. Ranking: ``field`` before ``regional``, then the former
  order (blockers, relative yield, trials, crop code).
- Presence-only recommendation (main mode): a crop with no numeric evidence at either tier, whose
  only evidence is trials of an excluded source (BSL) at the climate's aggregate sites, is
  returned with ``evidence.tier`` ``regional``, ``yield.expected_kg_ha`` null (the excluded kg/ha
  are never read), ``yield.n_trials`` = distinct such trials, no ``varieties``, trust ``low`` and
  the data gaps ``no_measured_yield`` (new) + ``regional_evidence_only`` + ``no_expected_yield``.
  It ranks after the regional recommendations that have a number.
- Reference of ``fit.relative_yield_pct`` (field recommendations): the median kg/ha of the crop's
  distinct, policy-eligible trials at the SAME analog field sites that back the recommendation
  and in the same irrigation regime (a trial whose source states the opposite regime never counts; one
  whose source states none stays and is reported as unknown, see ``evidence.irrigation_unknown_trials``);
  ``fit.reference.n_trials`` is how many trials it rests on and ``fit.reference.scope`` names the
  set: ``analog_sites:<climate>:<regime>`` (e.g. ``analog_sites:Csa:secano``; climate = the Köppen
  class, ``vector_v2`` when the vector-similarity fallback supplied the sites, ``any`` without a
  class; regime = ``secano`` | ``regadio`` | ``any``; ``:forage`` appended in forage mode).
  Fewer than ``MIN_REFERENCE_TRIALS`` trials: ``reference_too_small`` and a null relative yield.
  Regional recommendations keep a null relative yield and scope ``regional``.
- ``evidence.regional_trial_count``: distinct numeric regional trials of a ``field`` crop at
  the climate's aggregate sites (supplementary; in no number). null when not computed.
- ``evidence.irrigation_unknown_trials``: with a requested regime, the trials of the listed varieties
  whose source states no regime (kept, never pooled against a stated opposite regime) and the data
  gap ``irrigation_regime_unknown`` when there is any; null when no regime was requested.
- ``evidence.other_purpose_trials``: main mode ``{"forage": N}`` — distinct forage trials of
  the crop at the same analog field sites, counted and never averaged; ``{}`` otherwise.
- ``evidence.purpose``: the purpose the answer was computed for.
- Forage mode: ``yield.basis`` is ``dry_matter`` (yields are kg dry matter/ha, from records
  whose basis is known and convertible); a crop whose forage trials all have an unknown basis
  returns ``expected_kg_ha`` null, ``yield.n_trials`` = those trials, and the data gap
  ``forage_basis_unknown``. ``evidence.unknown_basis_trials`` counts the best variety's
  forage trials with a kg value that add no number. Reference medians use the same purpose.
- ``yield.n_trials`` / ``yield.n_sites`` / ``evidence.trial_count`` count distinct trials
  (content-identical re-ingested copies count once).
"""

from __future__ import annotations

import hashlib
import json

from app.graph import evidence_policy as ep
from app.services import ggcmi_calendar
from app.services.crop_reference import get_season_slots

RESISTANT_THRESHOLD = 0.7
# A reference median of fewer trials than this is not a reference: no relative yield (controller
# ruling 2026-10-04, raised from 3 now that the reference is local to the analog sites).
MIN_REFERENCE_TRIALS = 5
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


def sowing_info(eppo: str, koppen: str | None, table_rows: list[dict], country: str | None = None,
                *, lat: float | None = None, lon: float | None = None,
                irrigation: str | None = None) -> dict:
    """Sowing calendar of a crop: table row > GGCMI calendar > season slot.

    1. First matching table row (a month range). A row with ``countries``
       applies only when ``country`` is one of them (an unknown country matches
       no scoped row); a row without it applies anywhere.
    2. With a point (``lat``/``lon``), the GGCMI crop calendar cell: a typical
       sowing and maturity day, never a range, so ``sowing_window`` stays null.
       ``irrigation`` ``"regadío"`` reads the irrigated calendar, else the
       rainfed one (``typical_rainfed_fallback`` True).
    3. The coarse season slot of the crop.
    """
    for row in table_rows:
        if "countries" in row and country not in row["countries"]:
            continue
        if row["eppo"] == eppo and (koppen is None or koppen in row.get("koppen", [])):
            return {
                "sowing_type": row["sowing_type"],
                "sowing_window": {"start_month": row["start_month"], "end_month": row["end_month"]},
                "cycle_days": row.get("cycle_days"),
                "source": row["source"],
                "typical_sowing_doy": None,
                "typical_maturity_doy": None,
                "typical_rainfed_fallback": None,
            }
    if lat is not None and lon is not None:
        cal = ggcmi_calendar.lookup(eppo, lat, lon, irrigation=irrigation)
        if cal is not None:
            return {
                "sowing_type": ggcmi_calendar.sowing_type_from_doy(cal["planting_doy"]),
                "sowing_window": None,
                "cycle_days": cal["cycle_days"],
                "source": cal["source"],
                "typical_sowing_doy": cal["planting_doy"],
                "typical_maturity_doy": cal["maturity_doy"],
                "typical_rainfed_fallback": cal["rainfed_fallback"],
            }
    slots = get_season_slots(eppo)
    sowing_type = _SLOT_TO_SOWING[next(iter(slots))] if len(slots) == 1 else None
    return {"sowing_type": sowing_type, "sowing_window": None, "cycle_days": None,
            "source": "crop_season_slot", "typical_sowing_doy": None, "typical_maturity_doy": None,
            "typical_rainfed_fallback": None}


def count_blockers(rec: dict) -> int:
    s = rec["suitability"]
    return int(s["soil"]["level"] == "unsuitable") + int(s["frost"]["level"] == "risk")


def rank_recommendations(recs: list[dict]) -> list[dict]:
    def key(rec: dict):
        rel = rec["fit"]["relative_yield_pct"]
        # Field evidence first, then regional; within a tier the former order.
        regional = rec.get("evidence", {}).get("tier") == ep.EVIDENCE_TIER_REGIONAL
        # ... and a regional recommendation with a measured yield before one without (its trial
        # count says how often the crop was tested, not how well it did).
        no_number = regional and rec["yield"]["expected_kg_ha"] is None
        return (regional, no_number, count_blockers(rec), rel is None, -(rel or 0.0),
                -rec["yield"]["n_trials"], rec["crop"]["eppo"])
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
                         water, frost_level, sowing, data_gaps_extra, assumptions,
                         tier: str = ep.EVIDENCE_TIER_FIELD, purpose: str = ep.MODE_MAIN,
                         regional_trial_count: int | None = None) -> dict | None:
    if not varieties:
        return None
    best = varieties[0]
    regional = tier == ep.EVIDENCE_TIER_REGIONAL
    forage = purpose == ep.MODE_FORAGE
    # Presence only: the crop's regional evidence is trials of an excluded source (BSL), counted
    # and never read for a number.
    presence_only = bool(best.get("presence_only"))
    # Numeric trials only: a trial without a yield value backs no yield figure.
    n_numeric = int(best.get("numeric_yield_count") or 0)
    unknown_basis = int(best.get("unknown_basis_trial_count") or 0)
    # Forage trials whose basis is unknown carry kg but no comparable number: report how many.
    n_trials = n_numeric if (n_numeric or not forage) else unknown_basis
    if presence_only:
        n_trials = int(best.get("trial_count") or 0)
    expected = best.get("mean_yield_kg_ha")
    if regional:
        # A registry average is not comparable to the field-trial reference median.
        rel, rel_gap = None, ("no_expected_yield" if expected is None else "regional_not_comparable")
        reference = {"median_kg_ha": None, "n_trials": 0, "scope": ep.EVIDENCE_TIER_REGIONAL}
    else:
        rel, rel_gap = relative_yield(expected, reference.get("median_kg_ha"),
                                      int(reference.get("n_trials") or 0))
    cv, cv_gap = stability_cv(expected, best.get("stddev_yield_kg_ha"), n_numeric)
    gaps = [g for g in (rel_gap, cv_gap) if g] + list(data_gaps_extra)
    # Trials whose source states no irrigation regime stay under a requested regime (never
    # pooled against a stated opposite one); say how many, so the answer is not read as stated.
    regime_unknown = (sum(int(v.get("irrigation_unknown_trial_count") or 0) for v in varieties)
                      if conditions.get("irrigation_regime") else None)
    if regime_unknown:
        gaps.append("irrigation_regime_unknown")
    if regional:
        gaps.append("regional_evidence_only")
    if presence_only:
        gaps.append("no_measured_yield")
    if forage and expected is None and unknown_basis:
        gaps.append("forage_basis_unknown")
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
            "basis": ep.yield_basis(purpose) if expected is not None else None,
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
                   "source": sowing["source"],
                   "typical_sowing_doy": sowing.get("typical_sowing_doy"),
                   "typical_maturity_doy": sowing.get("typical_maturity_doy"),
                   "typical_rainfed_fallback": sowing.get("typical_rainfed_fallback")},
        "trust": {"level": "low" if regional else _trust_level(n_trials, best.get("confidence")),
                  "data_gaps": gaps},
        "varieties": [
            {"variety": v.get("variety"), "variety_uri": v.get("variety_uri"),
             "expected_kg_ha": v.get("mean_yield_kg_ha"),
             "interval": [v.get("min_yield_kg_ha"), v.get("max_yield_kg_ha")],
             "n_trials": int(v.get("numeric_yield_count") or 0),
             "disease_summary": disease_summary(v.get("disease_scores") or {})}
            for v in varieties[:5] if not v.get("presence_only")
        ],
        # trial_count = all trials (numeric or not) of the listed varieties;
        # years is null when no trial year is known.
        "evidence": {"trial_count": sum(int(v.get("trial_count") or 0) for v in varieties),
                     "sources": sources, "sites": sites,
                     "years": [years[0], years[-1]] if years else None,
                     "tier": tier, "purpose": purpose,
                     "regional_trial_count": regional_trial_count,
                     "irrigation_unknown_trials": regime_unknown,
                     "other_purpose_trials": (
                         {"forage": int(best.get("crop_other_purpose_trials") or 0)}
                         if purpose == ep.MODE_MAIN and not regional else {}),
                     "unknown_basis_trials": unknown_basis if forage else None},
        "assumptions": assumptions,
    }
