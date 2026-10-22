"""C.3 — Accuracy backtest for the variety-recommendation advisor.

Leave-one-site-out cross-validation over the trials sub-graph: hold out one
TrialSite, predict its per-variety yield ranking from the *rest* using the very
same `extrapolate_varieties` the API ships (so this is a valid regression gate
for C.1/C.2/C.4), and compare against what was actually observed at that site.

Honesty rules (see plan Task C.3):
  * Ground truth = MEASURED yields only, as the evidence policy
    (`app.graph.evidence_policy`) defines them: grain yields of grain-family crops only (no
    forage/fresh records, no kg/ha from excluded sources such as BSL note × constant, no
    note-derived / fabricated kg/ha); only field evidence (located trials at real
    sites — pseudo-sites are never held out); content-identical trials count once.
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

from app.graph import agroclimatic, evidence_policy

# Pull a deep ranking per fold so predicted means exist for every observed
# variety; top-3 overlap still uses only the first three ranked.
_RANK_DEPTH = 500


def _mean(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 1) if values else None


def _median(values: list[float]) -> float | None:
    return round(statistics.median(values), 1) if values else None


class _Bucket:
    """Accumulates fold outcomes for one grouping (overall / crop / climate)."""

    __slots__ = ("covered", "errors", "folds", "overlaps")

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


_STRATEGIES = ("koppen", "v1", "v2", "hybrid")


def _has_numeric_ranking(pred: dict[str, Any]) -> bool:
    return any(r.get("mean_yield_kg_ha") is not None for r in pred.get("ranked_varieties", []))


class Backtester:
    """Leave-one-site-out accuracy evaluation over measured trials."""

    def __init__(self, dao) -> None:
        self._dao = dao

    async def _folds(self) -> list[dict[str, Any]]:
        """One row per (site, crop): observed per-variety mean measured yield."""
        query = f"""
            MATCH (v:VarietyTrial)-[:TRIAL_AT]->(t:TrialSite)
            WHERE v.yieldKgHa IS NOT NULL
              AND coalesce(v.rankingEligible, true) = true
              AND {evidence_policy.cypher_grain_yield("v")}
              AND {evidence_policy.cypher_field_evidence("v", "t")}
              AND {evidence_policy.cypher_production_match("v.productionSystem", "'conventional'")}
              AND coalesce(t.climateClassChelsa, t.climateClass) IS NOT NULL
              AND v.cropEppo IS NOT NULL
              AND v.varietyNormalized IS NOT NULL
            // One observation per distinct trial content (re-ingest twins and
            // same-name duplicate sites count once).
            WITH t.name AS site,
                 coalesce(t.climateClassChelsa, t.climateClass) AS climate, v.cropEppo AS crop,
                 coalesce(t.annualRainfallMmChelsa, t.annualRainfallMm) AS rainfall,
                 coalesce(t.annualET0MmChelsa, t.annualET0Mm) AS et0,
                 t.frostDaysPerYear AS frost, t.elevationM AS elevation,
                 t.coldestMonthMinCChelsa AS coldest_min, t.annualTempCChelsa AS annual_temp,
                 v.varietyNormalized AS variety, {evidence_policy.cypher_content_key("v")} AS ck,
                 min(v.yieldKgHa) AS kg
            WITH site, climate, crop, rainfall, et0, frost, elevation, coldest_min, annual_temp,
                 variety, avg(kg) AS obs_mean
            RETURN site, climate, crop, rainfall, et0, frost, elevation, coldest_min, annual_temp,
                   collect({{variety: variety, obs: obs_mean}}) AS observed
        """
        async with self._dao._driver.session() as session:
            result = await session.run(query)
            return [dict(r) async for r in result]

    async def _predict(
        self, strategy: str, fold: dict[str, Any], crop: str, climate: str, site: str,
    ) -> dict[str, Any]:
        base = {"crop": crop, "climate_class": climate, "top_n": _RANK_DEPTH,
                "exclude_sites": [site]}
        v2_target = {
            "rainfall": fold["rainfall"], "et0": fold["et0"],
            "coldest_min": fold.get("coldest_min"), "annual_temp": fold.get("annual_temp"),
        }
        if strategy == "v1":
            return await self._dao.extrapolate_varieties(
                **base,
                target_features={
                    "rainfall": fold["rainfall"], "et0": fold["et0"],
                    "frost": fold["frost"], "elevation": fold["elevation"],
                },
            )
        if strategy == "v2":
            return await self._dao.extrapolate_varieties(
                **base, target_features=v2_target, vector_version="v2",
            )
        pred = await self._dao.extrapolate_varieties(**base)  # Köppen path
        if strategy == "hybrid" and not _has_numeric_ranking(pred) \
                and agroclimatic.feature_vector_v2(
                    v2_target["rainfall"], v2_target["et0"],
                    v2_target["coldest_min"], v2_target["annual_temp"],
                ) is not None:
            pred = await self._dao.extrapolate_varieties(
                **base, target_features=v2_target, vector_version="v2",
            )
        return pred

    async def run(
        self, min_observed_varieties: int = 1, strategy: str = "hybrid",
    ) -> dict[str, Any]:
        """strategy: koppen (no vector) | v1 | v2 | hybrid (koppen, then v2 if uncovered).

        Default "hybrid" mirrors what the parcel API ships when enabled.
        """
        if strategy not in _STRATEGIES:
            raise ValueError(f"unknown backtest strategy: {strategy!r}")
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

            pred = await self._predict(strategy, fold, crop, climate, site)
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
            "similarity": strategy,
            "evidence_policy": evidence_policy.POLICY_VERSION,
            "eval_pool_observations": eval_pool,
            "overall": overall.summary(),
            "by_crop": {c: b.summary() for c, b in sorted(by_crop.items())},
            "by_climate": {c: b.summary() for c, b in sorted(by_climate.items())},
        }
