"""Depth of the trial evidence behind a variety table (pure).

The level is computed from the rows that feed the table of one parcel (not written by hand per
crop): distinct sites, distinct years and the number of trials behind the page (the page cap of
``load_crop_evidence`` can cut the list; ``total`` is the count before the cap).

Every level asks for sites, campaigns and trials together: one campaign cannot separate a variety
from its year, and one site cannot separate it from its soil, however many plots either has. The
thresholds (owner delegation 2026-11-03, after the first measurement; see the ranking ledger) follow
how variety networks recommend: a few campaigns across several locations make a usable network
("broad"), a long series across many locations adds the year-to-year contrast ("deep"). They are
named constants, so a review changes numbers, not code. A crop that does not reach a level is shown
one level down, never promoted.
"""
from __future__ import annotations

DEEP_MIN_YEARS, DEEP_MIN_SITES, DEEP_MIN_TRIALS = 8, 15, 1000
BROAD_MIN_YEARS, BROAD_MIN_SITES, BROAD_MIN_TRIALS = 3, 8, 300
MODERATE_MIN_YEARS, MODERATE_MIN_SITES, MODERATE_MIN_TRIALS = 2, 3, 50

LEVELS = ("deep", "broad", "moderate", "pilot")


def depth_level(*, n_trials: int, n_sites: int, n_years: int) -> str:
    if n_years >= DEEP_MIN_YEARS and n_sites >= DEEP_MIN_SITES and n_trials >= DEEP_MIN_TRIALS:
        return "deep"
    if n_years >= BROAD_MIN_YEARS and n_sites >= BROAD_MIN_SITES and n_trials >= BROAD_MIN_TRIALS:
        return "broad"
    if n_years >= MODERATE_MIN_YEARS and n_sites >= MODERATE_MIN_SITES and n_trials >= MODERATE_MIN_TRIALS:
        return "moderate"
    return "pilot"


def evidence_depth(rows: list[dict], *, total: int | None = None, summary: dict | None = None) -> dict:
    """``{level, n_trials, n_sites, n_years, first_year, last_year}`` of the evidence.

    ``total`` is the trial count before the page cap; ``summary`` (``n_sites``, ``n_years``,
    ``first_year``, ``last_year`` over the whole evidence, from the dao's count query) wins over the
    page when given, so a capped page never shortens the range or drops sites.
    """
    sites = {r.get("site") for r in rows if r.get("site")}
    years = sorted({r["year"] for r in rows if isinstance(r.get("year"), int)})
    n_trials = max(int(total or 0), len(rows))
    sm = summary or {}
    n_sites = max(int(sm.get("n_sites") or 0), len(sites))
    n_years = max(int(sm.get("n_years") or 0), len(years))
    first_year = sm.get("first_year") if sm.get("first_year") is not None else (years[0] if years else None)
    last_year = sm.get("last_year") if sm.get("last_year") is not None else (years[-1] if years else None)
    return {"level": depth_level(n_trials=n_trials, n_sites=n_sites, n_years=n_years),
            "n_trials": n_trials, "n_sites": n_sites, "n_years": n_years,
            "first_year": first_year, "last_year": last_year}
