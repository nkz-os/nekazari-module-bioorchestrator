"""Depth of the trial evidence behind a variety table (pure).

The level is computed from the rows that feed the table of one parcel (not written by hand per
crop): distinct sites, distinct years and the number of trials behind the page (the page cap of
``load_crop_evidence`` can cut the list; ``total`` is the count before the cap).

ASSUMPTION (owner review after the first measurement, see the ranking ledger): the thresholds come
from the 2026-11-01 census of the production graph. They are named constants, so a review changes
numbers, not code. A crop that does not reach a level is shown one level down, never promoted.
"""
from __future__ import annotations

DEEP_MIN_YEARS, DEEP_MIN_TRIALS, DEEP_MIN_SITES = 15, 2000, 50
BROAD_MIN_YEARS, BROAD_MIN_TRIALS = 10, 1000
MODERATE_MIN_YEARS, MODERATE_MIN_TRIALS = 10, 300

LEVELS = ("deep", "broad", "moderate", "pilot")


def depth_level(*, n_trials: int, n_sites: int, n_years: int) -> str:
    if n_years >= DEEP_MIN_YEARS and n_trials >= DEEP_MIN_TRIALS and n_sites >= DEEP_MIN_SITES:
        return "deep"
    if n_years >= BROAD_MIN_YEARS and n_trials >= BROAD_MIN_TRIALS:
        return "broad"
    if n_years >= MODERATE_MIN_YEARS and n_trials >= MODERATE_MIN_TRIALS:
        return "moderate"
    return "pilot"


def evidence_depth(rows: list[dict], *, total: int | None = None) -> dict:
    """``{level, n_trials, n_sites, n_years, first_year, last_year}`` of the rows (``total`` overrides
    the trial count when the page was capped)."""
    sites = {r.get("site") for r in rows if r.get("site")}
    years = sorted({r["year"] for r in rows if isinstance(r.get("year"), int)})
    n_trials = max(int(total or 0), len(rows))
    return {"level": depth_level(n_trials=n_trials, n_sites=len(sites), n_years=len(years)),
            "n_trials": n_trials, "n_sites": len(sites), "n_years": len(years),
            "first_year": years[0] if years else None, "last_year": years[-1] if years else None}
