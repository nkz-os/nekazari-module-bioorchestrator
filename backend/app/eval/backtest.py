"""C.3 — Accuracy backtest for the variety-recommendation advisor.

Leave-one-site-out cross-validation over the trials sub-graph: hold out one
TrialSite, predict its per-variety yield ranking from the *rest* using the very
same `extrapolate_varieties` the API ships (so this is a valid regression gate
for C.1/C.2/C.4), and compare against what was actually observed at that site.

Honesty rules (see plan Task C.3):
  * Ground truth = MEASURED yields only: `yieldKgHa IS NOT NULL` **and**
    `yieldDerivationMethod IS NULL`. Note-derived / fabricated yields never act
    as an observation you validate against.
  * The prediction side calls `extrapolate_varieties` unchanged — the backtest
    measures the advisor as it ships, not an idealized variant.

Metrics, reported overall and broken down per crop and per climate:
  * median absolute error (kg/ha) of predicted vs observed variety yield;
  * top-3 variety rank-overlap on held-out sites;
  * coverage — fraction of held-out (site, crop) folds that got a ranking.
"""
from __future__ import annotations

import statistics
from typing import Any

# Pull a deep ranking per fold so predicted means exist for every observed
# variety; top-3 overlap still uses only the first three ranked.
_RANK_DEPTH = 500


def _mean(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 1) if values else None


def _median(values: list[float]) -> float | None:
    return round(statistics.median(values), 1) if values else None


class _Bucket:
    """Accumulates fold outcomes for one grouping (overall / crop / climate)."""

    __slots__ = ("errors", "overlaps", "folds", "covered")

    def __init__(self) -> None:
        self.errors: list[float] = []
        self.overlaps: list[float] = []
        self.folds = 0
        self.covered = 0

    def summary(self) -> dict[str, Any]:
        return {
            "median_abs_error_kg_ha": _median(self.errors),
            "top3_overlap": round(statistics.fmean(self.overlaps), 3) if self.overlaps else 0.0,
            "coverage": round(self.covered / self.folds, 3) if self.folds else 0.0,
            "folds": self.folds,
            "error_pairs": len(self.errors),
        }


class Backtester:
    """Leave-one-site-out accuracy evaluation over measured trials."""

    def __init__(self, dao) -> None:
        self._dao = dao

    async def _folds(self) -> list[dict[str, Any]]:
        """One row per (site, crop): observed per-variety mean measured yield."""
        query = """
            MATCH (v:VarietyTrial)-[:TRIAL_AT]->(t:TrialSite)
            WHERE v.yieldKgHa IS NOT NULL
              AND v.yieldDerivationMethod IS NULL
              AND coalesce(v.rankingEligible, true) = true
              AND coalesce(v.yieldMetric, '') NOT IN [
                'fresh_fruit_kg_ha', 'fresh_grape_kg_ha', 'fresh grape', 'fresh_fruit'
              ]
              AND NOT coalesce(v.yieldMetric, '') CONTAINS 'fresh'
              AND t.climateClass IS NOT NULL
              AND v.cropEppo IS NOT NULL
              AND v.varietyNormalized IS NOT NULL
            WITH t.name AS site, t.climateClass AS climate, v.cropEppo AS crop,
                 t.annualRainfallMm AS rainfall, t.annualET0Mm AS et0,
                 t.frostDaysPerYear AS frost, t.elevationM AS elevation,
                 v.varietyNormalized AS variety, avg(v.yieldKgHa) AS obs_mean
            RETURN site, climate, crop, rainfall, et0, frost, elevation,
                   collect({variety: variety, obs: obs_mean}) AS observed
        """
        async with self._dao._driver.session() as session:
            result = await session.run(query)
            return [dict(r) async for r in result]

    async def run(self, min_observed_varieties: int = 1) -> dict[str, Any]:
        folds = await self._folds()

        overall = _Bucket()
        by_crop: dict[str, _Bucket] = {}
        by_climate: dict[str, _Bucket] = {}

        eval_pool = 0
        for fold in folds:
            site, climate, crop = fold["site"], fold["climate"], fold["crop"]
            observed = {
                o["variety"]: o["obs"]
                for o in fold["observed"]
                if o["variety"] is not None and o["obs"] is not None
            }
            if len(observed) < min_observed_varieties:
                continue
            eval_pool += len(observed)

            crop_b = by_crop.setdefault(crop, _Bucket())
            clim_b = by_climate.setdefault(climate, _Bucket())
            for b in (overall, crop_b, clim_b):
                b.folds += 1

            pred = await self._dao.extrapolate_varieties(
                crop=crop,
                climate_class=climate,
                top_n=_RANK_DEPTH,
                exclude_sites=[site],
                target_features={
                    "rainfall": fold["rainfall"], "et0": fold["et0"],
                    "frost": fold["frost"], "elevation": fold["elevation"],
                },
            )
            ranked = [
                r for r in pred.get("ranked_varieties", [])
                if r.get("mean_yield_kg_ha") is not None
            ]
            if not ranked:  # coverage miss: no analog produced a numeric ranking
                continue
            for b in (overall, crop_b, clim_b):
                b.covered += 1

            pred_means = {r["variety"]: r["mean_yield_kg_ha"] for r in ranked}
            pred_top3 = [r["variety"] for r in ranked[:3]]
            obs_top3 = sorted(observed, key=lambda v: observed[v], reverse=True)[:3]
            overlap = len(set(pred_top3) & set(obs_top3)) / min(3, len(obs_top3))

            for b in (overall, crop_b, clim_b):
                b.overlaps.append(overlap)
            for variety, obs_val in observed.items():
                if variety in pred_means:
                    err = abs(pred_means[variety] - obs_val)
                    for b in (overall, crop_b, clim_b):
                        b.errors.append(err)

        return {
            "strategy": "leave_one_site_out",
            "eval_pool_observations": eval_pool,
            "overall": overall.summary(),
            "by_crop": {c: b.summary() for c, b in sorted(by_crop.items())},
            "by_climate": {c: b.summary() for c, b in sorted(by_climate.items())},
        }
