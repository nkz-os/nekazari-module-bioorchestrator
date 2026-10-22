"""Graph Data Access Object — Neo4j query layer.

All methods use the AsyncDriver and return plain dicts for JSON serialisation.
Business logic lives in app/services/.

Tenant model:
  The Neo4j knowledge graph holds ONLY global biological reference data
  (crop catalog, phenology, EPPO/IUCN/AGROVOC). It is a single shared graph,
  NOT partitioned by tenant — biological reality is identical for all tenants.
  Tenant isolation is enforced at the deployment level (a dedicated nkz instance
  if a customer requires it), never by a tenant axis inside this graph.

  Tenant-specific data (parcels, NDVI, soil, weather, crop assignments) lives in
  Orion-LD per-tenant stores + TimescaleDB and is read on demand with the request's
  tenant; it is never persisted into this graph.
"""

from __future__ import annotations

import asyncio
import copy
import functools
import hashlib
import json
import logging
import os
import re
import time
import weakref
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import quote, unquote

from nkz_platform_sdk.agronomy import AgronomicValue, Source
from nkz_platform_sdk.orion import OrionClient

from app.graph import agroclimatic, zone_match
from app.graph import evidence_policy as ep
from app.services.country_lookup import country_at
from app.services.crop_cycles_client import fetch_crop_cycles
from app.services.soil_client import assess_soil_suitability, get_parcel_soil_properties
from app.species_registry import (
    CATALOG_SIBLING_CODES,
    resolve_species,
)
from neo4j import AsyncDriver

# C.2 — minimum numeric trials in a (crop, climate) cell for a "direct" (robust)
# ranking. Below it the response carries lowEvidence. Owner default (B2) = 5.
EVIDENCE_THRESHOLD = 5

# Catalogue/zonal trials (rankingEligible=false) remain in the graph for metadata
# and map display but must not enter yield ranking, extrapolation, or backtest.
RANKING_ELIGIBLE_PREDICATE = "coalesce(vt.rankingEligible, true) = true"

# Same crop predicate as extrapolate_varieties.
_CROP_MATCH_PREDICATE = (
    "(vt.cropEppo = $crop OR vt.cropScientific CONTAINS $crop "
    "OR toLower(vt.cropScientific) = toLower($crop))"
)
# Row stream -> ranked varieties, shared by extrapolate_varieties and its batched variant.
# A single definition keeps both paths' filters and weights identical; ties (equal mean at the
# displayed 0.1 precision) are broken by variety name (ASC), so ranking never depends on the
# query plan. The evidence policy (``app.graph.evidence_policy``) classifies each (trial, site)
# row once (``cypher_row_policy``); nothing below re-implements a rule:
#   * trials with identical observed content are ONE observation (content-key dedup);
#   * ``ep_y`` is the policy yield (null for excluded sources, forage in main mode, ...), so
#     ``numeric_yield_count`` / means / intervals only see eligible kg/ha, and ``trial_count``
#     counts distinct in-mode trials, numeric or not;
#   * forage trials of a main-mode answer are only counted (``other_n``, deduplicated);
#   * the content key is computed once per row, in the hit map, and read by the variety
#     aggregation and by the reference median alike (``ref_median``/``ref_n``): the median of the
#     crop's deduplicated policy numbers at these same sites, in the requested irrigation regime
#     (a trial with no regime never counts for one), independent of ``top_n`` and of the
#     variety-level filters.
_EXTRAPOLATE_BODY_TEMPLATE = """
                // Count the other-purpose trials of the crop once (grouping treats nulls as
                // equal, unlike an IN test over the key lists), in the requested irrigation
                // regime like every number of the answer: the notice must not promise trials
                // that "see as forage" would not list.
                CALL (hits) {
                  UNWIND [x IN hits WHERE x.other AND @IRRIGATION_REFERENCE@] AS o
                  WITH DISTINCT o.ck AS ck
                  RETURN count(*) AS other_n
                }
                // Reference: the median of the crop's distinct policy numbers at these sites in
                // the requested regime (content-identical trials once).
                CALL (hits) {
                  UNWIND [x IN hits WHERE x.in_mode AND x.y IS NOT NULL
                          AND @IRRIGATION_REFERENCE@] AS r
                  WITH r.ck AS ck, max(r.y) AS y
                  RETURN percentileCont(y, 0.5) AS ref_median, count(y) AS ref_n
                }
                // The variety aggregation runs in the main query, NOT in a CALL subquery: a
                // subquery keeps its imported ``hits`` (every trial row of the crop) in each
                // buffered row, so a sort inside it counted that list once per row it kept and
                // memory grew with ``top_n``. Here every grouping step below drops ``hits``, and
                // the crop-level scalars ride along as grouping keys.
                UNWIND [x IN hits WHERE x.in_mode] AS h
                WITH crop, other_n, ref_median, ref_n,
                     h.vt AS vt, h.ts AS ts, h.y AS ep_y, h.unconv AS ep_unconv, h.ck AS ck
                // Collapse each trial to ONE row regardless of how many (same-name
                // duplicate) sites it links to (G2/G9 guard). The key ``ck`` makes
                // content-identical trials (re-ingest twins) ONE observation below, so every
                // count and mean sees them once.
                WITH crop, other_n, ref_median, ref_n,
                     vt.varietyNormalized AS variety, vt, ep_y, ep_unconv, ck,
                     collect(DISTINCT ts.name) AS trial_sites
                WITH crop, other_n, ref_median, ref_n, variety, ck,
                     max(ep_y) AS g_y,
                     max(CASE WHEN ep_unconv THEN 1 ELSE 0 END) AS g_unconv,
                     min(vt.year) AS g_year,
                     min(vt.irrigationRegime) AS g_regime,
                     min(vt.productionSystem) AS g_system,
                     reduce(acc = [], sl IN collect(trial_sites) | acc + [x IN sl WHERE NOT x IN acc]) AS g_sites,
                     collect(DISTINCT vt.diseaseScoresUnified) AS g_disease,
                     collect(DISTINCT vt.agronomicTraitsUnified) AS g_traits,
                     collect(DISTINCT vt.confidence) AS g_confidence,
                     collect(DISTINCT vt.source_id) AS g_sources,
                     max(CASE WHEN vt.yieldDerivationMethod IS NOT NULL THEN 1 ELSE 0 END) AS g_derived
                // A requested regime never pools a trial whose source states the opposite one
                // (no down-weighting): such a trial is dropped here. A trial whose source states
                // no regime stays and is counted as unknown (``regime_unknown_count``).
                WHERE @IRRIGATION_WEIGHT@
                // Per-observation weight = nearest analog site (C.1) x recency (C.4).
                // $site_weights are 1.0 on the legacy path, so the weighted mean collapses
                // to a flat average there.
                WITH crop, other_n, ref_median, ref_n,
                     variety, g_y, g_unconv, g_year, g_regime, g_system, g_sites, g_disease,
                     g_traits, g_confidence, g_sources, g_derived,
                     reduce(mw = 0.0, n IN g_sites |
                        CASE WHEN coalesce($site_weights[n], 0.0) > mw
                             THEN $site_weights[n] ELSE mw END) AS w_site
                WITH crop, other_n, ref_median, ref_n,
                     variety, g_y, g_unconv, g_year, g_regime, g_system, g_sites, g_disease,
                     g_traits, g_confidence, g_sources, g_derived,
                     w_site
                     * (CASE WHEN g_year IS NOT NULL AND (toFloat($now_year) - toFloat(g_year)) > 0
                             THEN 0.5 ^ ((toFloat($now_year) - toFloat(g_year)) / $half_life)
                             ELSE 1.0 END)
                     AS w
                WITH crop, other_n, ref_median, ref_n, variety,
                     collect(DISTINCT g_year) AS years,
                     collect(g_sites) AS site_lists,
                     collect(DISTINCT g_regime) AS irrigation_regimes,
                     sum(CASE WHEN @REGIME_BLANK@ THEN 1 ELSE 0 END) AS regime_unknown_count,
                     collect(DISTINCT g_system) AS production_systems,
                     collect(g_disease) AS disease_lists,
                     collect(g_traits) AS trait_lists,
                     collect(g_confidence) AS confidence_lists,
                     collect(g_sources) AS source_lists,
                     avg(g_y) AS mean_yield_flat,
                     sum(CASE WHEN g_y IS NOT NULL THEN w * g_y ELSE 0.0 END) AS wsum,
                     sum(CASE WHEN g_y IS NOT NULL THEN w ELSE 0.0 END) AS wtot,
                     min(g_y) AS min_yield,
                     max(g_y) AS max_yield,
                     stDev(g_y) AS stddev_yield,
                     count(g_y) AS numeric_yield_count,
                     count(*) AS trial_count,
                     sum(g_derived) AS derived_count,
                     sum(CASE WHEN g_y IS NULL AND g_unconv = 1 THEN 1 ELSE 0 END) AS unconverted_count
                WHERE trial_count >= 1
                WITH crop, other_n, ref_median, ref_n, variety,
                     CASE WHEN wtot > 0 THEN wsum / wtot ELSE mean_yield_flat END AS mean_yield,
                     min_yield, max_yield, stddev_yield,
                     numeric_yield_count, trial_count, derived_count, unconverted_count, years,
                     reduce(acc = [], sl IN site_lists | acc + [x IN sl WHERE NOT x IN acc]) AS sites,
                     irrigation_regimes, regime_unknown_count, production_systems,
                     reduce(acc = [], l IN disease_lists | acc + [x IN l WHERE NOT x IN acc]) AS disease_scores_list,
                     reduce(acc = [], l IN trait_lists | acc + [x IN l WHERE NOT x IN acc]) AS agronomic_traits_list,
                     reduce(acc = [], l IN confidence_lists | acc + [x IN l WHERE NOT x IN acc]) AS confidence_levels,
                     reduce(acc = [], l IN source_lists | acc + [x IN l WHERE NOT x IN acc]) AS source_ids
                // One row per variety: sort them (per crop) and keep the first $top_n. The cut
                // is a slice of the ordered collection (never a sort over rows that carry a
                // large value), so memory is the crop's variety rows once plus the returned
                // rows. The crop's numeric trial total is summed over every variety BEFORE the
                // cut, so $top_n never truncates it (a trial belongs to exactly one variety).
                ORDER BY crop, mean_yield IS NULL, round(mean_yield, 1) DESC, variety ASC
                WITH crop, other_n, ref_median, ref_n,
                     sum(numeric_yield_count) AS crop_numeric_n,
                     collect({variety: variety, mean_yield: mean_yield, min_yield: min_yield,
                              max_yield: max_yield, stddev_yield: stddev_yield,
                              numeric_yield_count: numeric_yield_count, trial_count: trial_count,
                              derived_count: derived_count, unconverted_count: unconverted_count,
                              years: years, sites: sites, irrigation_regimes: irrigation_regimes,
                              regime_unknown_count: regime_unknown_count,
                              production_systems: production_systems,
                              disease_scores_list: disease_scores_list,
                              agronomic_traits_list: agronomic_traits_list,
                              confidence_levels: confidence_levels, source_ids: source_ids})
                       AS vrows
                UNWIND vrows[0..$top_n] AS r
                RETURN crop,
                       r.variety AS variety,
                       r.mean_yield AS mean_yield,
                       r.min_yield AS min_yield,
                       r.max_yield AS max_yield,
                       r.stddev_yield AS stddev_yield,
                       r.numeric_yield_count AS numeric_yield_count,
                       r.trial_count AS trial_count,
                       r.derived_count AS derived_count,
                       r.unconverted_count AS unconverted_count,
                       r.years AS years,
                       r.sites AS sites,
                       r.irrigation_regimes AS irrigation_regimes,
                       r.regime_unknown_count AS regime_unknown_count,
                       r.production_systems AS production_systems,
                       r.disease_scores_list AS disease_scores_list,
                       r.agronomic_traits_list AS agronomic_traits_list,
                       r.confidence_levels AS confidence_levels,
                       r.source_ids AS source_ids,
                       other_n, crop_numeric_n, ref_median, ref_n
"""
# The regime tests (reference, weight, variety filter) are the policy's (rule 8): a literal
# "secano"/"regadío" counts like its URI.
_EXTRAPOLATE_BODY_CYPHER = (
    _EXTRAPOLATE_BODY_TEMPLATE
    .replace("@IRRIGATION_REFERENCE@", ep.cypher_irrigation_match("x.regime"))
    .replace("@REGIME_BLANK@", "toLower(trim(coalesce(g_regime, ''))) = ''")
    .replace("@IRRIGATION_WEIGHT@", ep.cypher_irrigation_match("g_regime", "$target_regime"))
)

# One entry of the per-crop ``hits`` list built from the policy-classified rows.
_HIT_MAP_CYPHER = (
    "{{vt: vt, ts: ts, y: ep_y, in_mode: ep_in_mode, other: ep_other, unconv: ep_unconv, "
    "regime: vt.irrigationRegime, ck: {content_key}}}"
)


# Organic and conventional units are never pooled (policy rule 10); param ``$production_class``.
_PRODUCTION_PREDICATE = ep.cypher_production_match("vt.productionSystem")


_EXCLUDED_SITES_PREDICATE = """($excluded_sites IS NULL OR NOT EXISTS {
                      MATCH (vt)-[:TRIAL_AT]->(x:TrialSite)
                      WHERE toLower(x.name) IN $excluded_sites
                  })"""


# Zone pool of the regional tier (parcel zone matching, ``app.graph.zone_match``): ``matched`` keeps the
# units whose ``zoneKey`` is the parcel's zone; ``fallback`` keeps the units whose zone cannot be told
# apart for this parcel (no ``zoneKey``: no published climatic definition covers them, or the parcel lacks
# a class), never a unit decidably in another zone. Params: ``$zone_pool``, ``$zone_allow``, ``$zone_deny``.
_ZONE_POOL_PREDICATE = """(CASE $zone_pool
                    WHEN 'matched' THEN coalesce(vt.zoneKey IN $zone_allow, false)
                    WHEN 'fallback' THEN NOT coalesce(vt.zoneKey IN $zone_allow, false)
                                         AND NOT coalesce(vt.zoneKey IN $zone_deny, false)
                    ELSE true END)"""


def _zone_term(zone: bool) -> str:
    return f"AND {_ZONE_POOL_PREDICATE}" if zone else ""


def _zone_params(pool: str | None, ctx: Any) -> dict[str, Any]:
    """Query parameters of ``_ZONE_POOL_PREDICATE`` (all null/empty: the predicate keeps every row)."""
    if pool is None or ctx is None:
        return {"zone_pool": None, "zone_allow": [], "zone_deny": []}
    return {"zone_pool": pool, "zone_allow": list(ctx.allow), "zone_deny": list(ctx.deny)}


def _extrapolate_single_query(mode: str, tier: str, zone: bool = False) -> str:
    """Ranked varieties of ONE crop at ``$site_names`` (params: see extrapolate_varieties)."""
    ck = ep.cypher_content_key("vt")
    return (
        f"""
                MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite)
                WHERE ts.name IN $site_names
                  AND (vt.yieldKgHa IS NOT NULL OR vt.yieldNoteS1 IS NOT NULL)
                  AND coalesce(vt.rankingEligible, true) = true
                  AND {_CROP_MATCH_PREDICATE}
                  AND {_EXCLUDED_SITES_PREDICATE}
                  AND {_PRODUCTION_PREDICATE}
                  {_zone_term(zone)}
                  {ep.cypher_tier_prefilter(tier)}
                """
        + ep.cypher_row_policy(mode)
        + ep.cypher_tier_gate(tier)
        + "WITH $crop AS crop, collect(" + _HIT_MAP_CYPHER.format(content_key=ck) + ") AS hits\n"
        + _EXTRAPOLATE_BODY_CYPHER
    )


def _extrapolate_batch_query(mode: str, tier: str, zone: bool = False) -> str:
    """Ranked varieties of every crop in ``$crops`` at ``$site_names``, in one scan."""
    ck = ep.cypher_content_key("vt")
    return (
        f"""
                MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite)
                WHERE ts.name IN $site_names
                  AND (vt.yieldKgHa IS NOT NULL OR vt.yieldNoteS1 IS NOT NULL)
                  AND coalesce(vt.rankingEligible, true) = true
                  AND {_EXCLUDED_SITES_PREDICATE}
                  AND {_PRODUCTION_PREDICATE}
                  {_zone_term(zone)}
                  {ep.cypher_tier_prefilter(tier)}
                // Same crop predicate as extrapolate_varieties, evaluated once per
                // trial row for every requested crop; rows of other crops stop here.
                WITH vt, ts, [c IN $crops WHERE
                      vt.cropEppo = c
                      OR vt.cropScientific CONTAINS c
                      OR toLower(vt.cropScientific) = toLower(c)] AS matched
                WHERE size(matched) > 0
                """
        + ep.cypher_row_policy(mode, carry=("matched",))
        + ep.cypher_tier_gate(tier)
        + "UNWIND matched AS crop\n"
        + "WITH crop, collect(" + _HIT_MAP_CYPHER.format(content_key=ck) + ") AS hits\n"
        + _EXTRAPOLATE_BODY_CYPHER
    )


# Whole-response cache for recommend_for_conditions: the answer depends only on the
# request conditions and on graph data that changes through ingestion, so bounded
# staleness is acceptable. Insertion-ordered; the oldest entry is evicted when full.
_RECOMMEND_CACHE: dict[str, tuple[float, dict]] = {}
_RECOMMEND_TTL = 3600.0
_RECOMMEND_CACHE_MAX = 256
# Concurrent crop evaluations per request; Neo4j contention dominates beyond this.
RECOMMEND_CONCURRENCY = 4
# Process-wide bound on cold (cache-miss) recommend computations: the endpoint is
# public and each cold computation fans out into many Neo4j queries. Identical
# concurrent requests share one in-flight computation (single-flight).
RECOMMEND_MAX_CONCURRENT_REQUESTS = 2


class _ColdGuard:
    def __init__(self) -> None:
        self.sem = asyncio.Semaphore(RECOMMEND_MAX_CONCURRENT_REQUESTS)
        self.inflight: dict[str, asyncio.Future] = {}


# One guard per event loop: asyncio primitives are bound to the loop they run on.
_COLD_GUARDS: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def _cold_guard() -> _ColdGuard:
    loop = asyncio.get_running_loop()
    guard = _COLD_GUARDS.get(loop)
    if guard is None:
        guard = _COLD_GUARDS[loop] = _ColdGuard()
    return guard


def _inflight_done(inflight: dict[str, asyncio.Future], key: str, task: asyncio.Future) -> None:
    if inflight.get(key) is task:
        del inflight[key]
    if not task.cancelled():
        task.exception()  # retrieved: every awaiter re-raises it; avoid "never retrieved"


# Crops evaluated per request, counted after the analog-trial prefilter.
_RECOMMEND_MAX_CROPS = 30
# Crop cycle used for water demand when the crop reference has none.
_DEFAULT_GROWING_SEASON_DAYS = 180


def _assess_evidence(ranked: list[dict]) -> dict:
    """C.2 evidence gate: how much trial evidence backs this ranking."""
    n_numeric = sum(int(v.get("numeric_yield_count") or 0) for v in ranked)
    if n_numeric == 0:
        basis = "none"
    elif n_numeric < EVIDENCE_THRESHOLD:
        basis = "sparse"
    else:
        basis = "direct"
    return {
        "basis": basis,
        "lowEvidence": n_numeric < EVIDENCE_THRESHOLD,
        "numericTrials": n_numeric,
        "threshold": EVIDENCE_THRESHOLD,
    }

logger = logging.getLogger(__name__)


def _regime_label(irrigation_uri: str | None) -> str:
    """Stable ASCII label of the irrigation regime of a reference: secano | regadio | any."""
    if irrigation_uri is None:
        return "any"
    return ep.irrigation_regime(irrigation_uri) or "other"


def _reference_scope(climate: str | None, irrigation_uri: str | None, purpose: str) -> str:
    """``fit.reference.scope`` of a field recommendation: the trials the reference median is over.

    ``analog_sites:<climate>:<regime>`` (``analog_sites:Csa:secano``): the same analog field
    sites as the recommendation (``<climate>`` = the Köppen class, ``vector_v2`` for the
    vector-similarity fallback, ``any`` when the request has no class) and the same irrigation
    regime (``any`` = no regime requested). The forage purpose appends ``:forage``.
    """
    scope = f"analog_sites:{climate or 'any'}:{_regime_label(irrigation_uri)}"
    return scope if purpose == ep.MODE_MAIN else f"{scope}:{purpose}"


def _analog_reference(rows: list[dict], scope: str) -> dict:
    """The reference of a recommendation from its ranked rows (the median is crop level, so
    every row carries the same one). No rows or no trials: null median, n 0 (a data gap, not 0)."""
    row = rows[0] if rows else {}
    return {"median_kg_ha": row.get("crop_reference_median_kg_ha"),
            "n_trials": int(row.get("crop_reference_n") or 0), "scope": scope}


def _irrigation_uri(regime: str | None) -> str | None:
    """Map a human-readable irrigation regime to the AGROVOC URI stored on trials."""
    return ep.irrigation_uri(regime)


# Canonical order of the phenological stages (FAO-56: initial, development, mid-season, late-season).
# A phenology lookup that names no stage and gives no GDD returns the parameters of
# PHENOLOGY_DEFAULT_STAGE (FAO-56 reference stage: Kc_mid characterises the crop, Ky peaks at
# flowering), then the first stage in this order that has any; a stage outside it (species-specific,
# e.g. pit_hardening) follows, by name. Ties among parameters of one stage resolve by cultivar,
# management and climate zone, so the choice never depends on the physical order of the rows.
PHENOLOGY_STAGE_ORDER: tuple[str, ...] = ("initial", "development", "mid-season", "late-season")
PHENOLOGY_DEFAULT_STAGE = "mid-season"


def _phenology_stage_rank_cypher(stage_var: str = "st") -> str:
    """Cypher rank of ``stage_var.name``: ``PHENOLOGY_DEFAULT_STAGE``, then ``PHENOLOGY_STAGE_ORDER`` (others last)."""
    order = (PHENOLOGY_DEFAULT_STAGE, *(n for n in PHENOLOGY_STAGE_ORDER if n != PHENOLOGY_DEFAULT_STAGE))
    whens = " ".join(f"WHEN '{n}' THEN {i}" for i, n in enumerate(order))
    return f"CASE toLower(trim(coalesce({stage_var}.name, ''))) {whens} ELSE {len(PHENOLOGY_STAGE_ORDER)} END"


def _agroclimatic_mode() -> str:
    """Read the AGROCLIMATIC_VECTOR kill switch; invalid values fail safe to v1."""
    global _INVALID_VECTOR_LOGGED
    mode = os.environ.get("AGROCLIMATIC_VECTOR", "v1")
    if mode not in ("v1", "v2", "hybrid"):
        if not _INVALID_VECTOR_LOGGED:
            _INVALID_VECTOR_LOGGED = True
            logger.critical("invalid AGROCLIMATIC_VECTOR %r (expected v1|v2|hybrid); using v1", mode)
        mode = "v1"
    return mode


_CLIMATE_KEYS = ("annual_rainfall_mm", "annual_et0_mm", "coldest_month_min_c", "annual_temp_c")


def _numeric_trials(v: dict) -> int:
    return int(v.get("numeric_yield_count") or 0)


def _crop_matches_label(crop: str, eppo: str | None, scientific: str | None) -> bool:
    """``_CROP_MATCH_PREDICATE`` over one distinct (cropEppo, cropScientific) label."""
    return eppo == crop or (scientific is not None
                            and (crop in scientific or scientific.lower() == crop.lower()))


def _presence_variety(info: dict) -> dict:
    """The single ranked-variety row of a crop whose evidence is presence only: trials, no number."""
    return {
        "variety": None, "variety_uri": None, "presence_only": True,
        "mean_yield_kg_ha": None, "min_yield_kg_ha": None, "max_yield_kg_ha": None,
        "stddev_yield_kg_ha": None, "numeric_yield_count": 0, "unknown_basis_trial_count": 0,
        "trial_count": int(info["trial_count"]), "crop_other_purpose_trials": 0,
        "crop_numeric_trial_count": 0, "trial_years": list(info["years"]),
        "trial_sites": list(info["sites"]), "source_ids": list(info["sources"]),
        "irrigation_regimes": [], "production_systems": [], "disease_scores": {},
        "agronomic_traits": {}, "confidence": None,
    }


def _has_numeric_mean(rows: list[dict]) -> bool:
    return any(v.get("mean_yield_kg_ha") is not None for v in rows)


def _crop_numeric_trials(rows: list[dict]) -> int:
    """Distinct numeric trials of the crop behind ranked ``rows``. The query sums them over every
    variety before its ``top_n`` cut (``crop_numeric_trial_count``), so a capped list does not
    truncate the count; rows without that field fall back to the sum of the listed rows."""
    if not rows:
        return 0
    total = rows[0].get("crop_numeric_trial_count")
    return int(total) if total is not None else sum(_numeric_trials(v) for v in rows)


def _forage_basis_unknown_only(rows: list[dict], purpose: str) -> bool:
    """Forage mode: the field rows hold forage trials but no number, because every kg value has an
    unknown basis. Such a crop keeps its field recommendation (null yield, gap
    ``forage_basis_unknown``) instead of being replaced by regional evidence."""
    return (purpose == ep.MODE_FORAGE and not _has_numeric_mean(rows)
            and any(int(v.get("unknown_basis_trial_count") or 0) for v in rows))


# Log an invalid AGROCLIMATIC_VECTOR once per process, not per request.
_INVALID_VECTOR_LOGGED = False


TIMESERIES_READER_URL = os.getenv("TIMESERIES_READER_URL", "http://timeseries-reader-service:5000")


# Strong references to in-flight background climate reads, keyed by grid cell.
_climate_tasks: dict[str, asyncio.Task] = {}

# Negative cache: cell key -> monotonic() expiry. A cell whose read came back empty is not
# retried by background lookups (wait=False) until the entry expires; wait=True ignores it.
CLIMATE_NEGATIVE_TTL_S = 600.0
_climate_negative: dict[str, float] = {}

# At most this many CHELSA cell reads run at once (background tasks and wait=True alike).
MAX_CONCURRENT_CELL_READS = 2
_climate_sem: asyncio.Semaphore | None = None
_climate_sem_loop: asyncio.AbstractEventLoop | None = None


def _get_climate_sem() -> asyncio.Semaphore:
    """Semaphore bound to the running loop, created lazily (a new loop gets a new one)."""
    global _climate_sem, _climate_sem_loop
    loop = asyncio.get_running_loop()
    if _climate_sem is None or _climate_sem_loop is not loop:
        _climate_sem = asyncio.Semaphore(MAX_CONCURRENT_CELL_READS)
        _climate_sem_loop = loop
    return _climate_sem


def _chelsa_parcel_climate_enabled() -> bool:
    """Feature flag, read at call time. Default OFF: only 1/true/yes (any case) enable it."""
    return os.getenv("CHELSA_PARCEL_CLIMATE_ENABLED", "").strip().lower() in ("1", "true", "yes")


def _monthly_or_none(values: list | None) -> list | None:
    """Neo4j list properties cannot hold null: a series with any missing month is omitted."""
    if values is None or any(v is None for v in values):
        return None
    return values


def _climate_task_done(key: str, task: asyncio.Task) -> None:
    _climate_tasks.pop(key, None)
    if task.cancelled():
        return
    exc = task.exception()  # retrieves it: no "never retrieved" warning
    if exc is not None:
        logger.warning("background climate cell %s failed: %s", key, type(exc).__name__)


def _cap_field_sites(sites: list[dict], limit: int | None) -> list[dict]:
    """Keep the first ``limit`` field sites (all of them when ``limit`` is None); aggregate
    sites, present only when the caller asked for them, are never cut."""
    if limit is None:
        return sites
    kept = 0
    out: list[dict] = []
    for site in sites:
        if site["site_kind"] == ep.SITE_KIND_AGGREGATE:
            out.append(site)
        elif kept < limit:
            out.append(site)
            kept += 1
    return out


class GraphDAO:
    def __init__(self, driver: AsyncDriver) -> None:
        self._driver = driver

    # ── Health ────────────────────────────────────────────────────────────────

    async def health_check(self) -> dict:
        """Verify Neo4j connectivity with a minimal query."""
        try:
            async with self._driver.session() as session:
                result = await session.run("RETURN 1 AS alive")
                record = await result.single()
                return {"neo4j": "connected", "alive": record["alive"]}
        except Exception as exc:  # noqa: BLE001
            return {"neo4j": "error", "detail": str(exc)}

    # ── ClimateCell cache (global, keyed by CHELSA 30" grid cell) ─────────────

    async def get_climate_cell(self, key: str) -> dict | None:
        """Return the cached climate normals for a grid cell, or None."""
        async with self._driver.session() as session:
            result = await session.run(
                "MATCH (c:ClimateCell {key: $key}) RETURN c {.*} AS c", key=key
            )
            record = await result.single()
        node = record["c"] if record else None
        if not node:
            return None
        return {
            "koppen": node.get("koppen"),
            "annual_temp_c": node.get("annualTempC"),
            "annual_rainfall_mm": node.get("annualRainfallMm"),
            "annual_et0_mm": node.get("annualET0Mm"),
            "coldest_month_min_c": node.get("coldestMonthMinC"),
            "monthly_tas_c": node.get("monthlyTasC"),
            "monthly_pr_mm": node.get("monthlyPrMm"),
            "source": node.get("source"),
        }

    async def save_climate_cell(self, key: str, data: dict) -> None:
        """Upsert the climate normals for a grid cell (idempotent MERGE on key)."""
        async with self._driver.session() as session:
            await session.run(
                "MERGE (c:ClimateCell {key: $key}) "
                "SET c.koppen = $koppen, c.annualTempC = $annualTempC, "
                "c.annualRainfallMm = $annualRainfallMm, c.annualET0Mm = $annualET0Mm, "
                "c.coldestMonthMinC = $coldestMonthMinC, c.monthlyTasC = $monthlyTasC, "
                "c.monthlyPrMm = $monthlyPrMm, c.source = $source, "
                "c.computedAt = $computedAt",
                key=key,
                koppen=data.get("koppen"),
                annualTempC=data.get("annual_temp_c"),
                annualRainfallMm=data.get("annual_rainfall_mm"),
                annualET0Mm=data.get("annual_et0_mm"),
                coldestMonthMinC=data.get("coldest_month_min_c"),
                monthlyTasC=_monthly_or_none(data.get("monthly_tas_c")),
                monthlyPrMm=_monthly_or_none(data.get("monthly_pr_mm")),
                source=data.get("source"),
                computedAt=datetime.now(timezone.utc).isoformat(),
            )

    async def parcel_climate(
        self, lat: float, lon: float, *, wait: bool = False, timeout_s: float = 30.0
    ) -> dict | None:
        """Climate normals for a point: graph cache first, CHELSA read on miss.

        On a miss with wait=False the read+save runs in a background task (one per
        cell) and None is returned so the caller can fall back. With wait=True the
        read is awaited. A failed read (None) is never stored as a cell; it is remembered
        for CLIMATE_NEGATIVE_TTL_S so background lookups do not re-read it, while
        wait=True always retries. timeout_s bounds one CHELSA read.
        """
        from app.services import chelsa_climate

        key = chelsa_climate.cell_key(lat, lon)
        cached = await self.get_climate_cell(key)
        if cached is not None:
            return cached
        if wait:
            return await self._compute_climate_cell(key, lat, lon, timeout_s)
        if _climate_negative.get(key, 0.0) > time.monotonic():
            return None
        if key not in _climate_tasks:
            task = asyncio.create_task(self._compute_climate_cell(key, lat, lon, timeout_s))
            _climate_tasks[key] = task
            task.add_done_callback(lambda t, k=key: _climate_task_done(k, t))
        return None

    async def _compute_climate_cell(
        self, key: str, lat: float, lon: float, timeout_s: float = 30.0
    ) -> dict | None:
        from app.services import chelsa_climate

        async with _get_climate_sem():
            data = await chelsa_climate.read_cell(lat, lon, timeout_s=timeout_s)
        if data is None:
            _climate_negative[key] = time.monotonic() + CLIMATE_NEGATIVE_TTL_S
            return None
        _climate_negative.pop(key, None)
        await self.save_climate_cell(key, data)
        return data

    # ── Global stats (not tenant-filtered — counts all reference data) ────────

    async def get_stats(self) -> dict[str, Any]:
        """Return node count, relationship count, and per-label counts.

        Counts all nodes and relationships in the shared global graph.
        No tenant filtering applies — the graph holds only biological
        reference data that is identical for all tenants.
        """
        async with self._driver.session() as session:
            totals_result = await session.run(
                "MATCH (n) RETURN count(n) AS node_count "
                "UNION ALL "
                "MATCH ()-[r]->() RETURN count(r) AS node_count"
            )
            records = await totals_result.values()
            node_count = records[0][0] if records else 0
            rel_count = records[1][0] if len(records) > 1 else 0

            label_result = await session.run(
                "CALL db.labels() YIELD label "
                "RETURN label"
            )
            labels = [r["label"] async for r in label_result]

            label_counts = {}
            for lbl in labels:
                # Defense-in-depth: labels come from db.labels() (app-set), never user input
                if not re.match(r'^[A-Za-z][A-Za-z0-9_]*$', lbl):
                    continue
                cnt = await session.run(
                    "MATCH (n:" + lbl + ") RETURN count(n) AS c"
                )
                row = await cnt.single()
                if row:
                    label_counts[lbl] = row["c"]

            # Sort by count desc, limit to 30
            label_counts = dict(
                sorted(label_counts.items(), key=lambda x: -x[1])[:30]
            )

        return {
            "node_count": node_count,
            "relationship_count": rel_count,
            "label_counts": label_counts,
        }

    async def graph_quality_stats(self) -> dict[str, Any]:
        """Quality-metrics snapshot of the trials sub-graph — the regression gate.

        Run before/after any hygiene/canonicalization mutation and diff. Every
        per-trial count is DISTINCT so a trial multi-linked to duplicate sites is
        counted once (protects the metric while G2/G9 duplicates still exist).
        """
        async with self._driver.session() as session:
            trials_rec = await (
                await session.run(
                    "MATCH (v:VarietyTrial) "
                    "RETURN count(v) AS trials, count(v.yieldKgHa) AS with_yield"
                )
            ).single() or {}
            trials = trials_rec.get("trials") or 0
            with_yield = trials_rec.get("with_yield") or 0

            orphan_rec = await (
                await session.run(
                    "MATCH (v:VarietyTrial) WHERE NOT (v)-[:TRIAL_AT]->() "
                    "RETURN count(v) AS orphan"
                )
            ).single() or {}

            rel_rec = await (
                await session.run(
                    "MATCH (:VarietyTrial)-[r:TRIAL_AT]->(:TrialSite) "
                    "RETURN count(r) AS rels"
                )
            ).single() or {}

            sites_rec = await (
                await session.run(
                    "MATCH (t:TrialSite) "
                    "RETURN count(t) AS sites, count(t.climateClass) AS with_climate"
                )
            ).single() or {}

            dup_rec = await (
                await session.run(
                    "MATCH (t:TrialSite) "
                    "WITH toLower(trim(t.name)) AS n, count(*) AS c "
                    "WHERE c > 1 "
                    "RETURN count(*) AS dup_groups"
                )
            ).single() or {}

            climate_res = await session.run(
                "MATCH (v:VarietyTrial)-[:TRIAL_AT]->(t:TrialSite) "
                "WHERE t.climateClass IS NOT NULL "
                "RETURN t.climateClass AS climate, count(DISTINCT v) AS c "
                "ORDER BY c DESC"
            )
            trials_per_climate = {r["climate"]: r["c"] async for r in climate_res}

            src_res = await session.run(
                "MATCH (v:VarietyTrial) WHERE v.source_id IS NOT NULL "
                "RETURN DISTINCT v.source_id AS src ORDER BY src"
            )
            source_ids = [r["src"] async for r in src_res]

        sites = sites_rec.get("sites") or 0
        with_climate = sites_rec.get("with_climate") or 0
        rels = rel_rec.get("rels") or 0
        yield_pct = round(with_yield / trials * 100, 1) if trials else 0.0
        trial_at_ratio = round(rels / trials, 2) if trials else 0.0
        return {
            "trials": trials,
            "trials_with_yield": with_yield,
            "yield_pct": yield_pct,
            "orphan_trials": orphan_rec.get("orphan") or 0,
            "trial_at_ratio": trial_at_ratio,
            "trial_sites": sites,
            "dup_name_sites": dup_rec.get("dup_groups") or 0,
            "sites_without_climate": sites - with_climate,
            "trials_per_climate": trials_per_climate,
            "source_ids": source_ids,
        }

    # ── Lookup (global reference data) ────────────────────────────────────────

    async def get_all_species(self) -> list[dict]:
        """Return all species in the knowledge graph with data availability."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (s:Species)
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(st:PhenologyStage)-[:HAS_PARAMETER]->(p:PhenologyParams)
                OPTIONAL MATCH (s)-[:HAS_HEAT_TOLERANCE]->(ht:CropHeatTolerance)
                OPTIONAL MATCH (s)-[:HAS_SOIL_SUITABILITY]->(ss:CropSoilSuitability)
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(:PhenologyStage)-[:HAS_NUTRIENT_PROFILE]->(np:CropNutrientProfile)
                RETURN s.name AS name,
                       s.scientificName AS scientific_name,
                       s.eppoCode AS eppo_code,
                       s.agrovocUri AS agrovoc_uri,
                       count(DISTINCT st) AS stage_count,
                       count(DISTINCT p) AS params_count,
                       count(DISTINCT ht) AS heat_count,
                       count(DISTINCT ss) AS soil_count,
                       count(DISTINCT np) AS npk_count,
                       collect(DISTINCT p.kc) AS kc_values,
                       collect(DISTINCT p.d1) AS d1_values
                ORDER BY s.name
            """)
            from app.data.eppo_common_names import get_common_name
            species_list = []
            async for record in result:
                name = record["name"]
                kc_vals = [v for v in (record["kc_values"] or []) if v is not None]
                d1_vals = [v for v in (record["d1_values"] or []) if v is not None]
                species_list.append({
                    "name": name,
                    "scientific_name": record["scientific_name"],
                    "eppoCode": record["eppo_code"],
                    "uri": record["agrovoc_uri"],
                    "common_name": get_common_name(name) or name.capitalize(),
                    "stage_count": record["stage_count"],
                    "params_count": record["params_count"],
                    "has_phenology": record["params_count"] > 0,
                    "data_available": {
                        "kc": bool(kc_vals),
                        "d1_d2": bool(d1_vals),
                        "thermal": record["heat_count"] > 0,
                        "soil_suitability": record["soil_count"] > 0,
                        "npk": record["npk_count"] > 0,
                    },
                })
            return species_list

    # ── Reference Data ─────────────────────────────────────────────────────

    async def get_climate_classes(self) -> list[str]:
        """Return unique K\u00f6ppen climate classes from TrialSite nodes."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (ts:TrialSite)
                WHERE ts.climateClass IS NOT NULL AND ts.climateClass <> ''
                RETURN DISTINCT ts.climateClass AS climate_class
                ORDER BY climate_class
            """)
            return [r["climate_class"] async for r in result]

    async def get_soil_types(self) -> list[str]:
        """Return unique WRB soil types from TrialSite nodes."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (ts:TrialSite)
                WHERE ts.soilType IS NOT NULL AND ts.soilType <> ''
                RETURN DISTINCT ts.soilType AS soil_type
                ORDER BY soil_type
            """)
            return [r["soil_type"] async for r in result]

    async def get_phenology_params(
        self,
        species: str,
        stage: str | None = None,
        cultivar: str | None = None,
        management: str | None = None,
        climate_zone: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
        gdd: float | None = None,
    ) -> dict | None:
        """Query phenology parameters with context-aware cascade matching.

        Matching priority:
          1. Exact: species + stage + cultivar + management
          2. Management-only: species + stage + management (any cultivar)
          3. Generic: species + stage (no cultivar, no management)
          4. Species-only: any stage default

        Deterministic default: with no stage and no GDD the result is the best-scoring
        parameter row of ``PHENOLOGY_DEFAULT_STAGE`` (mid-season), then of the first stage in
        ``PHENOLOGY_STAGE_ORDER`` that has one (species-specific stages follow, by name); the
        same ranking resolves a ``stage`` that matches several stages. Remaining ties resolve by
        cultivar, management and climate zone, never by the physical order of the rows.

        When GDD (Growing Degree Days) is provided and stage is not explicitly
        given, auto-detects the phenological stage by matching GDD against
        the [gddMin, gddMax] thresholds stored in PhenologyStage nodes.

        Returns a dict with:
          - Core values: d1, d2, kc, mds_ref with confidence intervals
          - Provenance: sourceDoi, sourceShort, sourceAuthor, sourceYear,
                        sourceInstitution, sourceMethod, sourceConditions
          - Context: species, scientificName, stage, stageDescription,
                     cultivar, management, climateZone
          - Stage detection: baseTemp, gddMin, gddMax (when available)
          - Alternatives: list of {kc, sourceShort, sourceDoi, conditions}
          - Match info: matchLevel (exact/management/generic/species_only)
        """
        async with self._driver.session() as session:
            result = await session.run(
                """
                // ── Find species ────────────────────────────────────────
                MATCH (s:Species)
                WHERE s.name CONTAINS $species
                   OR s.scientificName CONTAINS $species
                   OR s.agrovocUri CONTAINS $species
                WITH s
                ORDER BY
                    CASE WHEN s.name = $species THEN 0
                         WHEN s.name CONTAINS $species THEN 1
                         ELSE 2 END,
                    s.name ASC, coalesce(s.scientificName, '') ASC
                LIMIT 1

                // ── Find best-matching stage ─────────────────────────────
                // Priority: explicit stage name > GDD range match > any stage with params
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(st:PhenologyStage)
                WHERE $stage IS NOT NULL AND st.name CONTAINS $stage

                // If no stage matched by name and GDD is provided, match by GDD range
                WITH s, st, $gdd AS gdd
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(st_gdd:PhenologyStage)
                WHERE st IS NULL
                  AND gdd IS NOT NULL
                  AND st_gdd.gddMin IS NOT NULL
                  AND st_gdd.gddMax IS NOT NULL
                  AND st_gdd.gddMin <= gdd
                  AND st_gdd.gddMax > gdd
                WITH s, st, st_gdd

                // If no stage matched yet, pick any stage that has parameters
                OPTIONAL MATCH (s)-[:HAS_STAGE]->(st_any:PhenologyStage)
                WHERE st IS NULL AND st_gdd IS NULL
                  AND (st_any)-[:HAS_PARAMETER]->()
                WITH s, COALESCE(st, st_gdd, st_any) AS st

                // ── Find best parameter by context cascade ───────────────
                OPTIONAL MATCH (st)-[:HAS_PARAMETER]->(p:PhenologyParams)

                // Score: exact context > management-only > generic default
                // Ties (every stage has a default row) resolve by the canonical stage order,
                // then by names: never by the physical order of the rows.
                WITH s, st, p
                ORDER BY
                    CASE WHEN p.cultivar = $cultivar
                          AND p.management = $mgmt THEN 0
                         WHEN p.management = $mgmt
                          AND p.cultivar IS NULL THEN 1
                         WHEN p.isDefault = true THEN 2
                         ELSE 3 END,
                    @STAGE_RANK@,
                    st.name ASC,
                    coalesce(p.cultivar, '') ASC,
                    coalesce(p.management, '') ASC,
                    coalesce(p.climateZone, '') ASC
                LIMIT 1

                // ── Fetch alternatives ───────────────────────────────────
                OPTIONAL MATCH (p)-[:HAS_ALTERNATIVE]->(alt:PhenologyAlternative)

                // ── Return full provenance ────────────────────────────────
                RETURN
                    s.name AS species,
                    s.scientificName AS scientific_name,
                    s.agrovocUri AS agrovoc_uri,
                    st.name AS stage,
                    st.description AS stage_description,
                    st.baseTemp AS stage_base_temp,
                    st.gddMin AS stage_gdd_min,
                    st.gddMax AS stage_gdd_max,
                    p.kc AS kc,
                    p.kcCiLow AS kc_ci_low,
                    p.kcCiHigh AS kc_ci_high,
                    p.ky AS ky,
                    p.d1 AS d1,
                    p.d1CiLow AS d1_ci_low,
                    p.d1CiHigh AS d1_ci_high,
                    p.d2 AS d2,
                    p.d2CiLow AS d2_ci_low,
                    p.d2CiHigh AS d2_ci_high,
                    p.mdsRef AS mds_ref,
                    p.mdsRefCiLow AS mds_ref_ci_low,
                    p.mdsRefCiHigh AS mds_ref_ci_high,
                    p.cultivar AS cultivar,
                    p.management AS management,
                    p.climateZone AS climate_zone,
                    p.isDefault AS is_default,
                    p.sourceDoi AS source_doi,
                    p.sourceShort AS source_short,
                    p.sourceAuthor AS source_author,
                    p.sourceYear AS source_year,
                    p.sourceInstitution AS source_institution,
                    p.sourceMethod AS source_method,
                    p.sourceConditions AS source_conditions,
                    CASE WHEN p.cultivar = $cultivar
                          AND p.management = $mgmt THEN 'exact'
                         WHEN p.management = $mgmt
                          AND p.cultivar IS NULL THEN 'management'
                         WHEN p.isDefault = true THEN 'generic'
                         WHEN p IS NOT NULL THEN 'species_only'
                         ELSE 'none' END AS match_level,
                    collect(
                        CASE WHEN alt IS NOT NULL THEN {
                            kc: alt.kc,
                            sourceShort: alt.sourceShort,
                            sourceDoi: alt.sourceDoi,
                            conditions: alt.conditions
                        } END
                    ) AS alternatives
                """.replace("@STAGE_RANK@", _phenology_stage_rank_cypher()),
                species=species,
                stage=stage,
                cultivar=cultivar,
                mgmt=management,
                gdd=gdd,
            )
            record = await result.single()
            if record is None or record["match_level"] == "none":
                # ── Fallback: try CropHealthAssessment from Orion-LD ─────
                return await self._fallback_phenology_from_orion(
                    species=species,
                    stage=stage,
                )

            # Filter nulls from alternatives collection
            alts = sorted(
                (a for a in (record["alternatives"] or []) if a is not None and a.get("kc") is not None),
                key=lambda a: (a["kc"], str(a.get("sourceShort") or ""), str(a.get("sourceDoi") or ""),
                               str(a.get("conditions") or "")),
            )

            return {
                "species": record["species"],
                "scientific_name": record["scientific_name"],
                "agrovoc_uri": record["agrovoc_uri"],
                "stage": record["stage"],
                "stage_description": record["stage_description"],
                "stage_base_temp": record["stage_base_temp"],
                "stage_gdd_min": record["stage_gdd_min"],
                "stage_gdd_max": record["stage_gdd_max"],
                "kc": record["kc"],
                "kc_confidence_interval": (
                    [record["kc_ci_low"], record["kc_ci_high"]]
                    if record["kc_ci_low"] is not None
                    else None
                ),
                "ky": record.get("ky"),
                "d1": record["d1"],
                "d1_confidence_interval": (
                    [record["d1_ci_low"], record["d1_ci_high"]]
                    if record["d1_ci_low"] is not None
                    else None
                ),
                "d2": record["d2"],
                "d2_confidence_interval": (
                    [record["d2_ci_low"], record["d2_ci_high"]]
                    if record["d2_ci_low"] is not None
                    else None
                ),
                "mds_ref": record["mds_ref"],
                "mds_ref_confidence_interval": (
                    [record["mds_ref_ci_low"], record["mds_ref_ci_high"]]
                    if record["mds_ref_ci_low"] is not None
                    else None
                ),
                "cultivar": record["cultivar"],
                "management": record["management"],
                "climate_zone": record["climate_zone"],
                "is_default": record["is_default"],
                "provenance": {
                    "doi": record["source_doi"],
                    "short": record["source_short"],
                    "author": record["source_author"],
                    "year": record["source_year"],
                    "institution": record["source_institution"],
                    "method": record["source_method"],
                    "conditions": record["source_conditions"],
                },
                "alternatives": alts,
                "match_level": record["match_level"],
            }

    async def get_phenology_stages(self, species: str) -> list[dict]:
        """Return the full ordered stage table for a species (ascending gddMin).

        Empty list when the species has no PhenologyStage nodes — callers
        (e.g. crop-health) fall back to their own default table.
        """
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Species)-[:HAS_STAGE]->(st:PhenologyStage)
                WHERE toLower(s.name) = toLower($species)
                   OR toLower(s.scientificName) = toLower($species)
                RETURN st.name AS stage, st.gddMin AS gddMin,
                       st.gddMax AS gddMax, st.baseTemp AS baseTemp
                """,
                species=species,
            )
            rows = await result.data()

        rows = [
            r for r in rows
            if r.get("gddMin") is not None and r.get("gddMax") is not None
        ]
        return sorted(rows, key=lambda r: r["gddMin"])

    async def contribute_phenology(
        self,
        species: str,
        stage: str,
        kc: float,
        d1: float | None = None,
        d2: float | None = None,
        mds_ref: float | None = None,
        cultivar: str | None = None,
        management: str | None = None,
        doi: str | None = None,
        author: str | None = None,
        conditions: str | None = None,
        contact_email: str | None = None,
        contributed_by: str | None = None,
        contributor_tenant: str | None = None,
    ) -> dict:
        """Submit a contributed phenology parameter for review.

        Creates nodes with status='pending_review'. An admin can later
        approve and merge into the main parameter set.
        """
        async with self._driver.session() as session:
            result = await session.run(
                """
                MERGE (s:Species {name: $species})
                MERGE (s)-[:HAS_STAGE]->(st:PhenologyStage {name: $stage})
                CREATE (st)-[:HAS_PARAMETER]->(p:PhenologyParams {
                    kc: $kc,
                    d1: $d1,
                    d2: $d2,
                    mdsRef: $mds_ref,
                    cultivar: COALESCE($cultivar, '__contributed__'),
                    management: COALESCE($management, '__contributed__'),
                    climateZone: '__contributed__',
                    isDefault: false,
                    sourceDoi: $doi,
                    sourceShort: 'Contributed: ' + COALESCE($author, 'anonymous'),
                    sourceAuthor: $author,
                    sourceYear: null,
                    sourceConditions: $conditions,
                    status: 'pending_review',
                    contactEmail: $contact_email,
                    contributedBy: $contributed_by,
                    contributorTenant: $contributor_tenant,
                    submittedAt: datetime()
                })
                RETURN p.status AS status, p.sourceShort AS source
                """,
                species=species,
                stage=stage,
                kc=kc,
                d1=d1,
                d2=d2,
                mds_ref=mds_ref,
                cultivar=cultivar,
                management=management,
                doi=doi,
                author=author,
                conditions=conditions,
                contact_email=contact_email,
                contributed_by=contributed_by,
                contributor_tenant=contributor_tenant,
            )
            record = await result.single()
            if record is None:
                return {"status": "error", "detail": "Failed to create"}
            return {"status": record["status"], "source": record["source"]}

    async def contribute_crop_parameters(
        self,
        crop_uri: str,
        params: dict,
        *,
        contributed_by: str,
        contributor_tenant: str | None,
        provenance: dict,
    ) -> bool:
        """Store contributed parameters for a crop as a pending-review node.

        Returns False, writing nothing, when no AgriCrop has ``crop_uri``.
        ``params`` is applied first and the review state, contributor identity
        and provenance are set after it, so no key in ``params`` can overwrite
        them. Callers still allow-list the keys (api/v1/catalog.py).
        """
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (c:AgriCrop {uri: $uri})
                CREATE (p:PhenologyParams)
                SET p += $params
                SET p.status = 'pending_review',
                    p.contributedBy = $contributed_by,
                    p.contributorTenant = $contributor_tenant,
                    p.contributedAt = datetime(),
                    p.sourceDoi = $doi,
                    p.sourceAuthor = $author,
                    p.sourceYear = $year,
                    p.sourceInstitution = $institution,
                    p.sourceMethod = $method,
                    p.sourceConditions = $conditions
                CREATE (c)-[:HAS_PARAMETER]->(p)
                RETURN count(p) AS created
                """,
                uri=crop_uri,
                params=params,
                contributed_by=contributed_by,
                contributor_tenant=contributor_tenant,
                doi=provenance.get("doi"),
                author=provenance.get("author"),
                year=provenance.get("year"),
                institution=provenance.get("institution"),
                method=provenance.get("method"),
                conditions=provenance.get("conditions"),
            )
            record = await result.single()
            return bool(record and record["created"])

    # ── Phenology Fallback (Orion-LD CropHealthAssessment) ──────────────────────

    async def _fallback_phenology_from_orion(
        self,
        species: str,
        stage: str | None = None,
    ) -> dict | None:
        """Fallback: fetch phenology params from CropHealthAssessment in Orion-LD.

        Called when Neo4j has no data for the requested species.
        Queries the most recent CropHealthAssessment entity for the species
        and extracts Kc, D1, D2, MDS from its attributes.
        """
        try:
            from nkz_platform_sdk.orion import OrionClient

            from app.core.config import settings

            orion = OrionClient(
                settings.catalog_tenant,
                base_url=settings.orion_ld_url,
                context_url=settings.context_url,
            )
            try:
                # NOTE: OrionClient (sdk 0.8.x) has no order_by/order_desc kwargs —
                # passing them raises TypeError and kills the whole fallback
                # (prod logs 2026-09-20). Sort client-side by assessedAt instead.
                entities = await orion.query_entities(
                    type="CropHealthAssessment",
                    limit=5,
                )
            finally:
                await orion.close()

            if not entities or not isinstance(entities, list):
                return None

            def _assessed_at(entity: dict) -> str:
                raw = entity.get("assessedAt") or {}
                if isinstance(raw, dict):
                    raw = raw.get("value") or raw.get("@value") or ""
                return str(raw or "")

            entities = sorted(entities, key=_assessed_at, reverse=True)

            # Find the first entity that matches the species
            for entity in entities:
                species_val = (
                    entity.get("species", {}).get("value")
                    or entity.get("species", {}).get("object")
                    or entity.get("species")
                )
                if not species_val:
                    continue
                if species.lower() in str(species_val).lower():
                    return self._extract_phenology_from_assessment(entity)

            # If no species match but we have any assessment, return the latest
            return self._extract_phenology_from_assessment(entities[0])

        except ImportError:
            logger.warning("nkz_platform_sdk not available, cannot fallback to Orion")
            return None
        except Exception as exc:  # noqa: BLE001
            logger.warning("Phenology fallback to Orion failed: %s", exc)
            return None

    def _extract_phenology_from_assessment(self, entity: dict) -> dict | None:
        """Extract phenology params from a CropHealthAssessment entity.

        The entity is expected in normalized NGSI-LD format (from OrionClient).
        """
        def _val(key: str) -> float | None:
            v = entity.get(key)
            if v is None:
                return None
            if isinstance(v, dict):
                return v.get("value")
            if isinstance(v, (int, float)):
                return float(v)
            return None

        kc = _val("kc")
        ky = _val("ky")
        d1 = _val("d1") or _val("nwsbBaseline") or _val("d1Baseline")
        d2 = _val("d2") or _val("maxStressBaseline") or _val("d2Baseline")
        mds_ref = _val("mdsRef") or _val("mdsReference")

        if kc is None and d1 is None and d2 is None and mds_ref is None:
            return None

        return {
            "species": entity.get("id", ""),
            "scientific_name": None,
            "agrovoc_uri": None,
            "stage": (
                entity.get("phenologyStage", {}).get("value")
                if isinstance(entity.get("phenologyStage"), dict)
                else entity.get("phenologyStage")
            ),
            "stage_description": None,
            "stage_base_temp": None,
            "stage_gdd_min": None,
            "stage_gdd_max": None,
            "kc": kc,
            "kc_confidence_interval": None,
            "ky": ky,
            "d1": d1,
            "d1_confidence_interval": None,
            "d2": d2,
            "d2_confidence_interval": None,
            "mds_ref": mds_ref,
            "mds_ref_confidence_interval": None,
            "cultivar": None,
            "management": None,
            "climate_zone": None,
            "is_default": True,
            "provenance": {
                "doi": None,
                "short": "CropHealthAssessment (fallback)",
                "author": None,
                "year": None,
                "institution": None,
                "method": "Orion-LD fallback",
                "conditions": None,
            },
            "alternatives": [],
            "match_level": "fallback_orion",
        }

    # ── Heat Tolerance ─────────────────────────────────────────────────────────

    async def get_heat_tolerance(self, species: str) -> dict | None:
        """Return heat/frost damage thresholds for a species."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Species)-[:HAS_HEAT_TOLERANCE]->(h:CropHeatTolerance)
                WHERE s.name = $species
                RETURN h.heatDamageThresholdC AS heat_damage_c,
                       h.frostDamageThresholdC AS frost_damage_c,
                       h.heatAccumHours AS heat_accum_hours,
                       h.sourceShort AS source_short,
                       h.sourceDoi AS source_doi
                LIMIT 1
                """,
                species=species,
            )
            record = await result.single()
            if record is None:
                return None
            return dict(record)

    # ── Nutrient Profile ──────────────────────────────────────────────────────

    async def get_nutrient_profile(self, species: str, stage: str | None = None) -> dict | None:
        """Return NPK uptake curve per phenological stage."""
        async with self._driver.session() as session:
            query = """
                MATCH (s:Species)-[:HAS_STAGE]->
                      (st:PhenologyStage)-[:HAS_NUTRIENT_PROFILE]->(n:CropNutrientProfile)
                WHERE toLower(s.name) = toLower($species)
                   OR toLower(s.scientificName) = toLower($species)
            """
            params: dict = {"species": species}
            if stage:
                query += " AND st.name = $stage"
                params["stage"] = stage
            query += """
                RETURN st.name AS stage, n.nitrogenUptake AS n_uptake,
                       n.phosphorusUptake AS p_uptake, n.potassiumUptake AS k_uptake,
                       n.sourceShort AS source_short, n.sourceDoi AS source_doi
                ORDER BY st.name
            """
            result = await session.run(query, params)
            records = await result.data()
            return records if records else None

    # ── Soil Suitability ──────────────────────────────────────────────────────

    async def get_soil_suitability(self, species: str) -> dict | None:
        """Return soil requirements for a crop species."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Species {name: $species})-[:HAS_SOIL_SUITABILITY]->(ss:CropSoilSuitability)
                RETURN ss.phMin AS ph_min, ss.phMax AS ph_max,
                       ss.textures AS textures, ss.drainage AS drainage
                LIMIT 1
                """,
                species=species,
            )
            record = await result.single()
            if record is None:
                return None
            return dict(record)

    async def soil_suitability_coverage(self) -> dict:
        """Report % of graph species with CropSoilSuitability tolerance data.

        Gaps here are exactly where the soil gate must return `unknown`.
        """
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Species)
                OPTIONAL MATCH (s)-[:HAS_SOIL_SUITABILITY]->(ss:CropSoilSuitability)
                RETURN count(DISTINCT s) AS total,
                       count(DISTINCT CASE WHEN ss IS NOT NULL THEN s END) AS with_tol
                """
            )
            record = await result.single()
            total = record["total"] if record else 0
            with_tol = record["with_tol"] if record else 0
            pct = round(100.0 * with_tol / total, 1) if total else 0.0
            return {
                "species_total": total,
                "species_with_tolerance": with_tol,
                "coverage_pct": pct,
            }

    # ── Rotation Constraints ──────────────────────────────────────────────────

    async def get_rotation_constraints(self, crop: str) -> list[dict]:
        """Return rotation constraints for a crop."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (rc:RotationConstraint)
                WHERE rc.cropA = $crop
                RETURN rc.cropB AS crop_b, rc.intervalYears AS interval_years,
                       rc.reason AS reason, rc.sourceShort AS source_short,
                       coalesce(rc.effect, 'restriction') AS effect
                """,
                crop=crop,
            )
            return [dict(r) for r in await result.data()]

    async def recommend_fertilizer(
        self, species: str, stage: str, soil_n: float = 0, soil_p: float = 0, soil_k: float = 0
    ) -> dict | None:
        """Return NPK fertilizer needs based on soil levels and crop demand."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (s:Species)-[:HAS_STAGE]->(st:PhenologyStage {name: $stage})
                     -[:HAS_NUTRIENT_PROFILE]->(n:CropNutrientProfile)
                WHERE toLower(s.name) = toLower($species)
                   OR toLower(s.scientificName) = toLower($species)
                RETURN n.element AS element, n.uptakeKgHaDay AS uptake
                """,
                species=species, stage=stage,
            )
            records = await result.data()
            if not records:
                return None

            soil = {"nitrogen": soil_n, "phosphorus": soil_p, "potassium": soil_k}
            recommendations = []
            for r in records:
                element = r["element"]
                uptake = float(r["uptake"] or 0)
                level = soil.get(element, 0)
                if level < uptake * 0.5:
                    status, action = "deficient", f"Increase {element}"
                elif level < uptake:
                    status, action = "adequate", f"Maintain {element}"
                else:
                    status, action = "sufficient", f"Sufficient {element}"
                recommendations.append({
                    "element": element, "uptake_kg_ha_day": uptake,
                    "soil_level": level, "status": status, "action": action,
                })

            return {"species": species, "stage": stage, "recommendations": recommendations}

    # ── AgriCrop Catalog (Orion-LD integration) ───────────────────────────────

    async def merge_agri_crop(self, entity: dict) -> None:
        """MERGE an AgriCrop from Orion-LD into Neo4j. Idempotent."""
        async with self._driver.session() as session:
            await session.run("""
                MERGE (c:AgriCrop {uri: $uri})
                SET c.name = $name,
                    c.scientificName = $scientificName,
                    c.dataProvider = $provider,
                    c.updatedAt = datetime()
            """,
                uri=entity.get("id"),
                name=self._extract_value(entity, "name"),
                scientificName=self._extract_value(entity, "scientificName"),
                provider=self._extract_value(entity, "dataProvider"),
            )

            # Sync hasSubCrop relationships
            sub_crops = entity.get("hasSubCrop", {})
            if isinstance(sub_crops, dict) and sub_crops.get("type") == "Relationship":
                variety_uris = sub_crops.get("object", [])
                if isinstance(variety_uris, str):
                    variety_uris = [variety_uris]
                for var_uri in variety_uris:
                    await session.run("""
                        MERGE (v:AgriCropVariety {uri: $var_uri})
                        WITH v
                        MATCH (c:AgriCrop {uri: $uri})
                        MERGE (c)-[:HAS_VARIETY]->(v)
                    """, uri=entity.get("id"), var_uri=var_uri)

            # Sync Kc values -> PhenologyParams (default)
            kc_ini = self._extract_value(entity, "kcIni")
            if kc_ini is not None:
                await session.run("""
                    MATCH (c:AgriCrop {uri: $uri})
                    MERGE (c)-[r:HAS_PARAMETER]->(p:PhenologyParams {isDefault: true})
                    SET p.kc = $kc_ini,
                        p.kcMid = $kc_mid,
                        p.kcEnd = $kc_end,
                        p.sourceShort = $source,
                        p.updatedAt = datetime()
                """,
                    uri=entity.get("id"),
                    kc_ini=float(kc_ini),
                    kc_mid=float(self._extract_value(entity, "kcMid") or kc_ini),
                    kc_end=float(self._extract_value(entity, "kcEnd") or kc_ini),
                    source=self._extract_value(entity, "kcSource") or "Unknown",
                )

            # Sync heat tolerance
            heat = self._extract_value(entity, "heatDamageThresholdC")
            frost = self._extract_value(entity, "frostDamageThresholdC")
            if heat is not None or frost is not None:
                await session.run("""
                    MATCH (c:AgriCrop {uri: $uri})
                    MERGE (c)-[:HAS_HEAT_TOLERANCE]->(ht:CropHeatTolerance)
                    SET ht.heatDamageThresholdC = $heat,
                        ht.frostDamageThresholdC = $frost,
                        ht.sourceType = $source_type,
                        ht.updatedAt = datetime()
                """,
                    uri=entity.get("id"),
                    heat=float(heat) if heat else None,
                    frost=float(frost) if frost else None,
                    source_type=self._extract_value(entity, "thermalSource") or "derived_from_ecocrop",
                )

    async def get_crop_catalog(self, source: str | None = None,
                                search: str | None = None) -> list[dict]:
        """List AgriCrop entities from Neo4j with variety counts."""
        query = """
            MATCH (c:AgriCrop)
            WHERE c.uri CONTAINS ':AgriCrop:' AND NOT c.uri CONTAINS ':AgriCrop:.*:'
        """
        if search:
            query += " AND (toLower(c.name) CONTAINS toLower($search) OR toLower(c.scientificName) CONTAINS toLower($search))"
        query += """
            OPTIONAL MATCH (c)-[:HAS_VARIETY]->(v:AgriCropVariety)
            OPTIONAL MATCH (c)-[:HAS_PARAMETER]->(p:PhenologyParams)
            OPTIONAL MATCH (c)-[:HAS_HEAT_TOLERANCE]->(ht:CropHeatTolerance)
            RETURN c, count(DISTINCT v) as variety_count,
                   count(DISTINCT p) > 0 as has_kc,
                   count(DISTINCT ht) > 0 as has_thermal
            ORDER BY c.name
        """
        async with self._driver.session() as session:
            result = await session.run(query, search=search)
            crops = []
            async for record in result:
                c = record["c"]
                crops.append({
                    "uri": c.get("uri"),
                    "name": c.get("name"),
                    "scientificName": c.get("scientificName"),
                    "dataProvider": c.get("dataProvider"),
                    "variety_count": record["variety_count"],
                    "has_kc": record["has_kc"],
                    "has_thermal": record["has_thermal"],
                })
            return crops

    # ── Agriculture: Variety Trials ──────────────────────────────────────

    async def get_variety_trials(
        self,
        crop: str | None = None,
        climate_class: str | None = None,
        soil_type: str | None = None,
        soil_texture: str | None = None,
        irrigation_regime: str | None = None,
        min_yield_kg_ha: float | None = None,
        min_rainfall_mm: float | None = None,
        max_rainfall_mm: float | None = None,
        limit: int = 50,
        variety: str | None = None,
        tier: str | None = ep.EVIDENCE_TIER_FIELD,
        management: str | None = None,
    ) -> list[dict]:
        """Ranked variety trial results with environmental filters.

        Returns trials sorted by yield (descending) with their TrialSite
        environmental context. Supports filtering by crop, variety (case-insensitive
        substring), climate, soil, irrigation regime, and rainfall range.

        The numbers follow the evidence policy (``app.graph.evidence_policy``), the same one the
        recommender uses: ``yield_kg_ha`` is the policy yield of the main purpose (null for a BSL
        or note-derived kg/ha, and for forage, which this listing leaves out), content-identical
        trials are one row (``site_names`` lists every site of the observation), organic units
        are listed only for ``management="organic"`` (and then only they are; rule 10), and only
        ``tier`` rows are listed: ``field`` (default) never mixes in national or regional
        records; ``None`` lists every tier (``evidence_tier`` tells them apart). A trial with a
        note and no number stays listed, with a null yield.

        This is the primary endpoint for:
          - "What wheat varieties perform best in BSk climate?"
          - "Show me tomato trials on calcareous soils under rainfed conditions"
        """
        if tier is not None:
            ep.check_tier(tier)
        where_clauses = [
            "(vt.yieldKgHa IS NOT NULL OR vt.yieldNoteS1 IS NOT NULL)",
            RANKING_ELIGIBLE_PREDICATE,
            _PRODUCTION_PREDICATE,
        ]
        params: dict[str, Any] = {"limit": limit, "production_class": ep.production_class(management)}

        if crop:
            where_clauses.append("(vt.cropEppo = $crop OR vt.cropScientific CONTAINS $crop OR toLower(vt.variety) CONTAINS toLower($crop))")
            params["crop"] = crop

        if variety:
            where_clauses.append("toUpper(coalesce(vt.variety, '')) CONTAINS toUpper($variety)")
            params["variety"] = variety

        if climate_class:
            where_clauses.append("ts.climateClass = $climate")
            params["climate"] = climate_class

        if soil_type:
            where_clauses.append("ts.soilType CONTAINS $soil")
            params["soil"] = soil_type

        if soil_texture:
            where_clauses.append("ts.soilTexture CONTAINS $texture")
            params["texture"] = soil_texture

        if irrigation_regime:
            irrigation_uri = ep.irrigation_uri(irrigation_regime)
            if irrigation_uri:
                # URI or literal spelling of the regime (evidence policy rule 8)
                where_clauses.append(ep.cypher_irrigation_match("vt.irrigationRegime"))
                params["irrigation_uri"] = irrigation_uri
            else:  # not a known regime name: the caller's own substring of the stored value
                where_clauses.append("vt.irrigationRegime CONTAINS $irrigation")
                params["irrigation"] = irrigation_regime

        min_yield_term = ""
        if min_yield_kg_ha is not None:
            # on the policy yield: a BSL or derived kg/ha never satisfies a minimum yield
            min_yield_term = "AND ep_y >= $min_yield\n"
            params["min_yield"] = min_yield_kg_ha

        if min_rainfall_mm is not None:
            where_clauses.append("ts.annualRainfallMm >= $min_rain")
            params["min_rain"] = min_rainfall_mm

        if max_rainfall_mm is not None:
            where_clauses.append("ts.annualRainfallMm <= $max_rain")
            params["max_rain"] = max_rainfall_mm

        where_str = " AND ".join(where_clauses)
        gate = ep.cypher_tier_gate_expr(tier, with_other=False) if tier else "ep_in_mode"
        # Rows are classified by the policy once; the content key (a per-trial expand) is built only
        # for the best ``fetch`` candidates, then content-identical trials collapse to one row.
        params["fetch"] = max(limit, 1) * 3

        query = (
            f"""
            MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite)
            WHERE {where_str}
            """
            + ep.cypher_row_policy(ep.MODE_MAIN)
            + f"""WHERE {gate}
            {min_yield_term}WITH vt, ts, ep_y, ep_tier
            ORDER BY ep_y IS NULL, ep_y DESC, vt.mergeKey ASC, ts.name ASC
            LIMIT $fetch
            WITH vt, ts, ep_y, ep_tier, {ep.cypher_content_key("vt")} AS ck
            WITH ck, collect({{vt: vt, ts: ts, y: ep_y, tier: ep_tier}}) AS rows
            WITH rows[0] AS h,
                 reduce(acc = [], r IN rows | CASE WHEN r.ts.name IN acc THEN acc ELSE acc + r.ts.name END)
                   AS site_names
            WITH h.vt AS vt, h.ts AS ts, h.y AS ep_y, h.tier AS ep_tier, site_names
            ORDER BY ep_y IS NULL, ep_y DESC, vt.mergeKey ASC, ts.name ASC
            LIMIT $limit
            OPTIONAL MATCH (vt)-[:SOURCED_FROM]->(as_article:ArticleSource)
            RETURN vt.cropEppo AS crop_eppo,
                   vt.cropScientific AS crop_scientific,
                   vt.variety AS variety,
                   ep_y AS yield_kg_ha,
                   ep_tier AS evidence_tier,
                   site_names AS site_names,
                   vt.yieldNoteS1 AS yield_note_s1,
                   vt.yieldRelativePct AS yield_relative_pct,
                   vt.qualityParams AS quality_params,
                   vt.diseaseScores AS disease_scores,
                   vt.diseaseScoresUnified AS disease_scores_unified,
                   vt.agronomicTraitsUnified AS agronomic_traits_unified,
                   vt.irrigationRegime AS irrigation_regime,
                   vt.year AS year,
                   vt.confidence AS confidence,
                   vt.mergeKey AS trial_id,
                   vt.source_id AS source_id,
                   vt.rootstock AS rootstock,
                   vt.trainingSystem AS training_system,
                   vt.plantingYear AS planting_year,
                   CASE WHEN vt.plantingYear IS NOT NULL AND vt.year IS NOT NULL
                        THEN vt.year - vt.plantingYear ELSE null END AS orchard_age_years,
                   ts.name AS site_name,
                   ts.climateClass AS climate_class,
                   ts.soilType AS soil_type,
                   ts.soilTexture AS soil_texture,
                   ts.soilPh AS soil_ph,
                   ts.annualRainfallMm AS annual_rainfall_mm,
                   ts.elevationM AS elevation_m,
                   ts.frostDaysPerYear AS frost_days,
                   ts.photoperiodSummerHours AS photoperiod_hours,
                   as_article.articleTitle AS source_title,
                   as_article.issueNumber AS source_issue,
                   as_article.year AS source_year
            ORDER BY ep_y IS NULL, ep_y DESC, vt.mergeKey ASC, ts.name ASC
            """
        )

        async with self._driver.session() as session:
            result = await session.run(query, params)
            trials = []
            async for record in result:
                trials.append(dict(record))
            return trials

    async def get_similar_sites(
        self,
        reference_site: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
        climate_class: str | None = None,
        soil_type: str | None = None,
        rainfall_min: float | None = None,
        rainfall_max: float | None = None,
        limit: int | None = 10,
        target_features: dict[str, float | None] | None = None,
        vector_version: str = "v1",
        include_aggregate: bool = False,
        country: str | None = None,
    ) -> list[dict]:
        """Find TrialSites agro-climatically similar to a target (C.1).

        Each site carries ``site_kind`` (``field`` | ``aggregate``, see ``evidence_policy``).
        Only field sites are returned by default: an aggregate pseudo-site (national or regional
        registry, "average of N locations") is not a place, so it never enters a field analog
        set. ``include_aggregate`` also returns the aggregate sites of the same climate class
        (they carry no soil or rainfall, so those filters do not apply to them) for the
        regional evidence tier: an aggregate that declares a country (``TrialSite.country``, e.g. a
        national trial network's zone means, which have no coordinates and so no climate) is in scope
        for a target of that ISO country and only for it; an aggregate without a country keeps the
        climate-class match. ``country`` is ignored for field sites. ``limit`` caps the field sites only (``None`` = no cap, the
        Köppen path's contract: every matching field site, ordered by name).

        ``vector_version`` "v1" uses rainfall/et0/frost/elevation; "v2" uses the
        CHELSA vector (rainfall/et0/coldest_min/annual_temp) on both sides.

        When a target agro-climatic vector is available (``target_features`` =
        rainfall/et0/frost/elevation, or derived from ``reference_site``), sites are
        ranked by **ascending agro-climatic distance** (aridity, rainfall, frost,
        elevation — see ``agroclimatic``), and the ``distance`` is exposed. Köppen is
        a **soft prior**, not a hard gate: a same-Köppen site lacking the numeric
        vector is still returned, with a penalty distance, so coverage is preserved.

        Without a target vector (only a climate label) it falls back to the legacy
        Köppen/soil/rainfall filter (``distance`` = None).
        """
        if vector_version not in ("v1", "v2"):
            raise ValueError(f"unknown agro-climatic vector_version: {vector_version!r}")
        is_v2 = vector_version == "v2"

        # Resolve a reference site's own vector + climate as the target.
        if reference_site:
            async with self._driver.session() as session:
                ref_result = await session.run(
                    """
                    MATCH (ts:TrialSite)
                    WHERE toLower(ts.name) = toLower($name)
                       OR toLower(ts.municipality) = toLower($name)
                    RETURN coalesce(ts.climateClassChelsa, ts.climateClass) AS climate,
                           ts.soilType AS soil,
                           coalesce(ts.annualRainfallMmChelsa, ts.annualRainfallMm) AS rainfall,
                           coalesce(ts.annualET0MmChelsa, ts.annualET0Mm) AS et0,
                           ts.frostDaysPerYear AS frost,
                           ts.elevationM AS elevation,
                           ts.coldestMonthMinCChelsa AS coldest_min,
                           ts.annualTempCChelsa AS annual_temp
                    LIMIT 1
                    """,
                    name=reference_site,
                )
                ref = await ref_result.single()
                if not ref:
                    return []
                climate_class = ref["climate"]
                soil_type = soil_type or ref["soil"]
                if rainfall_min is None and ref["rainfall"] is not None:
                    rainfall_min = ref["rainfall"] - 200
                    rainfall_max = ref["rainfall"] + 200
                if target_features is None:
                    if is_v2:
                        target_features = {
                            "rainfall": ref["rainfall"], "et0": ref["et0"],
                            "coldest_min": ref["coldest_min"],
                            "annual_temp": ref["annual_temp"],
                        }
                    else:
                        target_features = {
                            "rainfall": ref["rainfall"], "et0": ref["et0"],
                            "frost": ref["frost"], "elevation": ref["elevation"],
                        }

        target_vec = None
        if target_features:
            if is_v2:
                target_vec = agroclimatic.feature_vector_v2(
                    target_features.get("rainfall"), target_features.get("et0"),
                    target_features.get("coldest_min"), target_features.get("annual_temp"),
                )
            else:
                target_vec = agroclimatic.feature_vector(
                    target_features.get("rainfall"), target_features.get("et0"),
                    target_features.get("frost"), target_features.get("elevation"),
                )

        # Pull every site once; scoring/filtering happens in Python (≤ a few hundred).
        query = """
            MATCH (ts:TrialSite)
            WHERE ts.name IS NOT NULL
            RETURN ts.name AS name,
                   ts.siteKind AS declared_kind,
                   ts.country AS country,
                   ts.municipality AS municipality,
                   ts.agroclimaticZone AS agroclimatic_zone,
                   ts.latitude AS latitude,
                   ts.longitude AS longitude,
                   coalesce(ts.climateClassChelsa, ts.climateClass) AS climate_class,
                   ts.soilType AS soil_type,
                   ts.soilTexture AS soil_texture,
                   ts.soilPh AS soil_ph,
                   ts.soilOrganicMatterPct AS soil_organic_matter_pct,
                   coalesce(ts.annualRainfallMmChelsa, ts.annualRainfallMm) AS annual_rainfall_mm,
                   coalesce(ts.annualET0MmChelsa, ts.annualET0Mm) AS annual_et0_mm,
                   ts.frostDaysPerYear AS frost_days,
                   ts.elevationM AS elevation_m,
                   ts.coldestMonthMinCChelsa AS coldest_min,
                   ts.annualTempCChelsa AS annual_temp,
                   ts.photoperiodSummerHours AS photoperiod_hours
        """
        async with self._driver.session() as session:
            result = await session.run(query)
            rows = [dict(r) async for r in result]
        for r in rows:
            r["site_kind"] = ep.site_kind(r["name"], r.pop("declared_kind", None))
        target_country = (country or "").strip().upper() or None

        def aggregate_in_scope(r: dict) -> bool:
            """Regional tier: the aggregate's own country when it has one, else its climate class."""
            site_country = (r.get("country") or "").strip().upper()
            if site_country and target_country:
                if site_country == target_country:
                    r["matched_by"] = "country"  # the gap ``regional_country_level`` reads this
                    return True
                return False
            return not climate_class or r["climate_class"] == climate_class

        # ── Distance path (C.1): rank by agro-climatic distance ─────────────
        if target_vec is not None:
            if is_v2:
                features = agroclimatic.FEATURES_V2
                weights = agroclimatic.DEFAULT_WEIGHTS_V2
                vectors = [
                    agroclimatic.feature_vector_v2(
                        r["annual_rainfall_mm"], r["annual_et0_mm"],
                        r.get("coldest_min"), r.get("annual_temp"),
                    )
                    for r in rows
                ]
            else:
                features = agroclimatic.FEATURES
                weights = agroclimatic.DEFAULT_WEIGHTS
                vectors = [
                    agroclimatic.feature_vector(
                        r["annual_rainfall_mm"], r["annual_et0_mm"],
                        r["frost_days"], r["elevation_m"],
                    )
                    for r in rows
                ]
            bounds = agroclimatic.normalize_bounds(vectors + [target_vec], features=features)
            scored: list[dict] = []
            for r, vec in zip(rows, vectors):
                if r["site_kind"] == ep.SITE_KIND_AGGREGATE and not include_aggregate:
                    continue
                if r["site_kind"] == ep.SITE_KIND_AGGREGATE and r.get("country") and target_country:
                    if not aggregate_in_scope(r):
                        continue
                    d = agroclimatic.KOPPEN_FALLBACK_DISTANCE  # a country is no vector: ranked after real analogs
                elif vec is not None:
                    d = agroclimatic.distance(
                        target_vec, vec, bounds, weights=weights, features=features,
                    )
                elif climate_class and r["climate_class"] == climate_class:
                    # Same Köppen but no numeric vector → keep, ranked after real
                    # analogs (soft prior, not a hard gate). Coverage preserved.
                    d = agroclimatic.KOPPEN_FALLBACK_DISTANCE
                else:
                    continue  # can't judge (no vector, no matching climate)
                r["distance"] = round(d, 4)
                scored.append(r)
            scored.sort(key=lambda s: (s["distance"], s["name"]))
            return _cap_field_sites(scored, limit)

        # ── Legacy path: Köppen / soil / rainfall filter, no distance ───────
        filtered: list[dict] = []
        for r in rows:
            is_aggregate = r["site_kind"] == ep.SITE_KIND_AGGREGATE
            if is_aggregate and not include_aggregate:
                continue
            if is_aggregate:
                if not aggregate_in_scope(r):
                    continue
            elif climate_class and r["climate_class"] != climate_class:
                continue
            if not is_aggregate:  # aggregate sites carry no soil or rainfall to filter on
                if soil_type and (not r["soil_type"] or soil_type not in r["soil_type"]):
                    continue
                rain = r["annual_rainfall_mm"]
                if rainfall_min is not None and (rain is None or rain < rainfall_min):
                    continue
                if rainfall_max is not None and (rain is None or rain > rainfall_max):
                    continue
            r["distance"] = None
            filtered.append(r)
        filtered.sort(key=lambda s: s["name"])
        return _cap_field_sites(filtered, limit)

    async def _crops_with_analog_trials(
        self,
        eppos: list[str],
        site_names: list[str],
        irrigation_uri: str | None = None,
        exclude_sites: list[str] | None = None,
        purpose: str = ep.MODE_MAIN,
        tier: str = ep.EVIDENCE_TIER_FIELD,
        zone_pool: str | None = None,
        zone_ctx: Any = None,
        management: str | None = None,
    ) -> set[str]:
        """EPPO codes for which ``extrapolate_varieties`` would return >= 1 variety.

        Mirrors extrapolate's eligibility exactly: same site/crop predicates,
        ranking-eligible trials with a yield value (numeric or note), the same
        held-out-site rule, the same evidence-policy row gate (``purpose`` and
        ``tier``), and, when ``irrigation_uri`` is given, at least one such
        trial in that regime (extrapolate keeps a variety only if its regimes include
        it). One round trip for all crops, so an
        empty result means extrapolate would return nothing for every crop.
        """
        excluded_lower = [x.lower() for x in exclude_sites] if exclude_sites else None
        if excluded_lower:
            site_names = [n for n in site_names if n.lower() not in set(excluded_lower)]
        if not eppos or not site_names:
            return set()
        # Site-first, single scan: anchor on ts.name (indexed) and walk only the trials
        # of the analog sites, collapsing to the distinct crop labels they carry. Crop
        # matching is then done here with extrapolate's predicate; testing each crop
        # inside the database rescans the trials once per crop that has none.
        t0 = time.monotonic()
        async with self._driver.session() as session:
            result = await session.run(
                f"""
                MATCH (ts:TrialSite)
                WHERE ts.name IN $site_names
                MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts)
                WHERE (vt.yieldKgHa IS NOT NULL OR vt.yieldNoteS1 IS NOT NULL)
                  AND {RANKING_ELIGIBLE_PREDICATE}
                  AND {ep.cypher_irrigation_match("vt.irrigationRegime")}
                  AND {_PRODUCTION_PREDICATE}
                  AND ($excluded_sites IS NULL OR NOT EXISTS {{
                      MATCH (vt)-[:TRIAL_AT]->(x:TrialSite)
                      WHERE toLower(x.name) IN $excluded_sites
                  }})
                  {_zone_term(zone_pool is not None)}
                  {ep.cypher_tier_prefilter(tier)}
                {ep.cypher_row_policy(purpose)}
                {ep.cypher_tier_gate(tier, with_other=False).rstrip()}
                RETURN DISTINCT vt.cropEppo AS eppo, vt.cropScientific AS sci
                """,
                site_names=list(site_names),
                irrigation_uri=irrigation_uri,
                production_class=ep.production_class(management),
                excluded_sites=excluded_lower,
                **_zone_params(zone_pool, zone_ctx),
            )
            labels = [(r["eppo"], r["sci"]) async for r in result]
        logger.debug("analog prefilter labels=%d elapsed_s=%.3f", len(labels), time.monotonic() - t0)

        return {c for c in eppos if any(_crop_matches_label(c, eppo, sci) for eppo, sci in labels)}

    async def regional_zone_keys(
        self, crops: list[str], site_names: list[str], zone_ctx: Any, irrigation_uri: str | None = None,
        purpose: str = ep.MODE_MAIN, management: str | None = None,
    ) -> dict[str, list[str]]:
        """Zone keys of the numeric trials of the parcel's own zone (``matched`` pool), per crop.

        What the answer names as the matched zone: the units that carry a kg/ha value in the parcel's zone,
        in the requested irrigation regime, under the same row policy and regional tier gate of the
        ``purpose`` as the evidence that feeds the answer (a unit the policy excludes is never named).
        """
        if not crops or not site_names or zone_ctx is None or not zone_ctx.ready:
            return {}
        async with self._driver.session() as session:
            result = await session.run(
                f"""
                MATCH (ts:TrialSite)
                WHERE ts.name IN $site_names
                MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts)
                WHERE (vt.yieldKgHa IS NOT NULL OR vt.yieldNoteS1 IS NOT NULL)
                  AND {RANKING_ELIGIBLE_PREDICATE}
                  AND {_ZONE_POOL_PREDICATE}
                  AND {_PRODUCTION_PREDICATE}
                  {ep.cypher_tier_prefilter(ep.EVIDENCE_TIER_REGIONAL)}
                """
                + ep.cypher_row_policy(purpose)
                + ep.cypher_tier_gate(ep.EVIDENCE_TIER_REGIONAL)
                + f"""  AND {ep.cypher_irrigation_match("vt.irrigationRegime")}
                RETURN vt.cropEppo AS eppo, vt.cropScientific AS sci, collect(DISTINCT vt.zoneKey) AS keys
                """,
                site_names=list(site_names),
                irrigation_uri=irrigation_uri,
                production_class=ep.production_class(management),
                **_zone_params(zone_match.POOL_MATCHED, zone_ctx),
            )
            rows = [dict(r) async for r in result]
        out: dict[str, list[str]] = {}
        for crop in dict.fromkeys(crops):
            keys = sorted({k for r in rows if _crop_matches_label(crop, r["eppo"], r["sci"]) for k in r["keys"]})
            if keys:
                out[crop] = keys
        return out

    async def resolve_zone_context(
        self, country: str | None, lat: Any, lon: Any, irrigation_regime: str | None,
    ) -> zone_match.ZoneContext | None:
        """The parcel's zone context, or None when zone matching does not apply (not a Spanish parcel
        with a point). The parcel's CHELSA cell comes from the graph cache; a miss is read once
        (bounded) only when the parcel-climate flag is on. Not ready (never an error): the answer stays
        at the country level and says why."""
        if not zone_match.applies(country, lat, lon):
            return None
        from app.services import chelsa_climate

        key = chelsa_climate.cell_key(float(lat), float(lon))
        try:
            cell = await self.get_climate_cell(key)
            if cell is None and _chelsa_parcel_climate_enabled():
                cell = await self.parcel_climate(float(lat), float(lon), wait=True, timeout_s=10.0)
        except Exception as exc:  # noqa: BLE001
            logger.warning("zone context: parcel climate lookup failed: %s", type(exc).__name__)
            cell = None
        return zone_match.build_context(cell, irrigation_regime, key)

    async def regional_presence_trials(
        self,
        crops: list[str],
        site_names: list[str],
        irrigation_uri: str | None = None,
        purpose: str = ep.MODE_MAIN,
        management: str | None = None,
    ) -> dict[str, dict]:
        """Crops whose evidence at the aggregate ``site_names`` is presence only (policy rule 7).

        One scan for every crop: the trials of an excluded source (BSL) at the aggregate sites,
        ranking-eligible, with a yield value or note (extrapolate's eligibility), in the same
        irrigation regime when ``irrigation_uri`` is given. Per crop (same crop predicate as
        extrapolate): ``{"trial_count": distinct trials (content key), "years", "sites",
        "sources"}``. The excluded kg/ha are never read. Crops without such trials are absent;
        any purpose but ``main`` returns ``{}`` (``evidence_policy.presence_only_applies``).
        """
        purpose = ep.check_mode(purpose)
        if not ep.presence_only_applies(purpose) or not crops or not site_names:
            return {}
        t0 = time.monotonic()
        async with self._driver.session() as session:
            result = await session.run(
                f"""
                MATCH (ts:TrialSite)
                WHERE ts.name IN $site_names
                MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts)
                WHERE (vt.yieldKgHa IS NOT NULL OR vt.yieldNoteS1 IS NOT NULL)
                  AND {RANKING_ELIGIBLE_PREDICATE}
                  AND {ep.cypher_irrigation_match("vt.irrigationRegime")}
                  AND {_PRODUCTION_PREDICATE}
                  AND any(c IN $crops WHERE vt.cropEppo = c OR vt.cropScientific CONTAINS c
                          OR toLower(vt.cropScientific) = toLower(c))
                  {ep.cypher_presence_prefilter()}
                {ep.cypher_row_policy(purpose)}
                {ep.cypher_presence_gate().rstrip()}
                WITH vt, collect(DISTINCT ts.name) AS sites
                WITH vt.cropEppo AS eppo, vt.cropScientific AS sci, vt.year AS year,
                     vt.source_id AS source, sites, {ep.cypher_content_key("vt")} AS ck
                RETURN eppo, sci, count(DISTINCT ck) AS n, collect(DISTINCT year) AS years,
                       collect(DISTINCT source) AS sources, collect(sites) AS site_lists
                """,
                site_names=list(site_names),
                crops=list(crops),
                irrigation_uri=irrigation_uri,
                production_class=ep.production_class(management),
            )
            labels = [dict(r) async for r in result]
        out: dict[str, dict] = {}
        for crop in dict.fromkeys(crops):
            matched = [r for r in labels if _crop_matches_label(crop, r["eppo"], r["sci"])]
            if not matched:
                continue
            out[crop] = {
                "trial_count": sum(int(r["n"]) for r in matched),
                "years": sorted({y for r in matched for y in r["years"] if y is not None}),
                "sites": sorted({n for r in matched for sl in r["site_lists"] for n in sl}),
                "sources": sorted({x for r in matched for x in r["sources"] if x}),
            }
        logger.debug("presence labels=%d crops=%d elapsed_s=%.3f", len(labels), len(out),
                     time.monotonic() - t0)
        return out

    async def organic_units_excluded(
        self,
        crops: list[str],
        site_names: list[str],
        irrigation_uri: str | None = None,
        purpose: str = ep.MODE_MAIN,
    ) -> dict[str, int]:
        """Organic units a non-organic request leaves out (policy rule 10), per crop.

        Distinct (content key) organic units that carry a policy number of the ``purpose`` at
        ``site_names`` in the requested irrigation regime, under the same eligibility as the
        evidence the answer reads. Crops without such units are absent. Informational: it feeds the
        ``organic_units_excluded`` gap and never changes a number.
        """
        purpose = ep.check_mode(purpose)
        if not crops or not site_names:
            return {}
        async with self._driver.session() as session:
            result = await session.run(
                f"""
                MATCH (ts:TrialSite)
                WHERE ts.name IN $site_names
                MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts)
                WHERE vt.yieldKgHa IS NOT NULL
                  AND {RANKING_ELIGIBLE_PREDICATE}
                  AND {ep.cypher_irrigation_match("vt.irrigationRegime")}
                  AND NOT {ep.cypher_production_match("vt.productionSystem", "'conventional'")}
                  AND any(c IN $crops WHERE vt.cropEppo = c OR vt.cropScientific CONTAINS c
                          OR toLower(vt.cropScientific) = toLower(c))
                  {ep.cypher_tier_prefilter(ep.EVIDENCE_TIER_REGIONAL)}
                {ep.cypher_row_policy(purpose)}
                WHERE ep_in_mode AND ep_y IS NOT NULL
                WITH DISTINCT vt
                WITH vt.cropEppo AS eppo, vt.cropScientific AS sci, {ep.cypher_content_key("vt")} AS ck
                RETURN eppo, sci, count(DISTINCT ck) AS n
                """,
                site_names=list(site_names),
                crops=list(crops),
                irrigation_uri=irrigation_uri,
            )
            labels = [dict(r) async for r in result]
        out: dict[str, int] = {}
        for crop in dict.fromkeys(crops):
            n = sum(int(r["n"]) for r in labels if _crop_matches_label(crop, r["eppo"], r["sci"]))
            if n:
                out[crop] = n
        return out

    async def _soil_gate(self, crop: str, parcel_id: str | None, tenant_id: str) -> dict:
        """Grade a crop against a parcel's REAL soil (C.5 soil-suitability gate).

        crop tolerance ← CropSoilSuitability (graph, keyed by species slug);
        parcel soil ← Soil module (live, HMAC). `crop` may be an EPPO code →
        resolve to the canonical slug before reading tolerance. Emits the
        verdict in the shared AgronomicValue envelope for downstream consumers.
        """
        species = resolve_species(crop) or crop
        crop_tolerance = await self.get_soil_suitability(species)
        parcel_soil = (
            await get_parcel_soil_properties(parcel_id, tenant_id) if parcel_id else None
        )
        gate = assess_soil_suitability(crop_tolerance, parcel_soil)
        gate["agronomic"] = AgronomicValue(
            value=gate["verdict"],
            source=Source(short=gate["source"]),
            confidence=gate["confidence"],
            notes=[gate["reason"]],
        ).model_dump()
        if parcel_soil and parcel_soil.get("data_available"):
            gate["parcel_soil"] = {
                "ph": parcel_soil.get("ph"), "texture": parcel_soil.get("texture"),
            }
        return gate

    async def extrapolate_varieties(
        self,
        crop: str,
        reference_site: str | None = None,
        climate_class: str | None = None,
        soil_type: str | None = None,
        irrigation_regime: str | None = None,
        rainfall_min: float | None = None,
        rainfall_max: float | None = None,
        top_n: int = 10,
        filter_soil_suitability: bool = False,
        parcel_id: str | None = None,
        tenant_id: str = "",
        exclude_sites: list[str] | None = None,
        target_features: dict[str, float | None] | None = None,
        recency_half_life: float = 8.0,
        vector_version: str = "v1",
        similar_sites_override: list[dict] | None = None,
        purpose: str = ep.MODE_MAIN,
        tier: str = ep.EVIDENCE_TIER_FIELD,
        country: str | None = None,
        management: str | None = None,
    ) -> dict:
        """Extrapolate best varieties for a target environment.

        ``management`` (``organic`` | ``conventional`` | ``any``/None): organic and conventional
        units are never pooled (policy rule 10); None and ``any`` read the non-organic units.

        ``country`` (ISO 3166 alpha-2) scopes the aggregate sites of the regional tier (see
        ``get_similar_sites``); it has no effect on the field tier.

        ``similar_sites_override``: precomputed ``get_similar_sites`` output (same
        shape) used instead of the internal lookup, so a caller evaluating many
        crops against identical inputs pays for the site scan once.

        ``purpose`` (``main`` | ``forage``) and ``tier`` (``field`` | ``regional``) select the
        evidence under the policy of ``app.graph.evidence_policy``: duplicate trials count once,
        excluded sources (BSL) add no kg/ha, and the analog sites are field sites (default) or
        the aggregate sites of the climate (``regional``, numeric evidence only). The result
        names both (``purpose``, ``evidence_tier``) and each ranked variety carries
        ``crop_other_purpose_trials`` (main mode: forage trials of the crop at these sites).

        This is the combined "killer endpoint" that:
          1. Finds TrialSites similar to the target environment
          2. Aggregates VarietyTrial results from those sites
          3. Ranks varieties by mean yield (with min/max/stddev)
          4. Returns per-site breakdown for transparency

        The target environment can be specified either by:
          - reference_site: name of a known TrialSite to emulate
          - explicit climate/soil/rainfall filters

        Returns:
            {
              "target_environment": {...},
              "similar_sites": ["site1", "site2", ...],
              "ranked_varieties": [
                {
                  "variety": "AUBUSSON",
                  "mean_yield_kg_ha": 9121.0,
                  "min_yield": 8500.0,
                  "max_yield": 9800.0,
                  "stddev_yield": 350.0,
                  "trial_count": 5,
                  "years": [2007, 2008, ...],
                  "sites": ["Cadreita", "Olite"]
                }, ...
              ],
              "data_quality": {"total_trials_analyzed": N, "unique_varieties": M}
            }
        """
        purpose = ep.check_mode(purpose)
        tier = ep.check_tier(tier)
        production_class = ep.production_class(management)
        # ── Step 1: resolve target environment ──────────────────────────
        target_env: dict[str, Any] = {
            "crop": crop,
            "climate_class": climate_class,
            "soil_type": soil_type,
            "irrigation_regime": irrigation_regime,
            "rainfall_min": rainfall_min,
            "rainfall_max": rainfall_max,
        }

        if reference_site:
            async with self._driver.session() as session:
                ref_result = await session.run(
                    """
                    MATCH (ts:TrialSite)
                    WHERE toLower(ts.name) = toLower($name)
                       OR toLower(ts.municipality) = toLower($name)
                    RETURN coalesce(ts.climateClassChelsa, ts.climateClass) AS climate,
                           ts.soilType AS soil,
                           coalesce(ts.annualRainfallMmChelsa, ts.annualRainfallMm) AS rainfall,
                           coalesce(ts.annualET0MmChelsa, ts.annualET0Mm) AS et0,
                           ts.frostDaysPerYear AS frost,
                           ts.name AS name,
                           ts.latitude AS lat,
                           ts.longitude AS lon,
                           ts.elevationM AS elevation,
                           ts.coldestMonthMinCChelsa AS coldest_min,
                           ts.annualTempCChelsa AS annual_temp
                    LIMIT 1
                    """,
                    name=reference_site,
                )
                ref = await ref_result.single()
                if not ref:
                    return {"error": f"Reference site '{reference_site}' not found", "similar_sites": [], "ranked_varieties": []}

                target_env["climate_class"] = ref["climate"]
                target_env["soil_type"] = ref["soil"]
                target_env["rainfall_min"] = (ref["rainfall"] or 500) - 200
                target_env["rainfall_max"] = (ref["rainfall"] or 500) + 200
                target_env["reference_site_name"] = ref["name"]
                target_env["reference_lat"] = ref["lat"]
                target_env["reference_lon"] = ref["lon"]
                target_env["reference_elevation"] = ref["elevation"]
                # Derive the target agro-climatic vector for distance weighting (C.1).
                if target_features is None and vector_version == "v2":
                    target_features = {
                        "rainfall": ref["rainfall"], "et0": ref["et0"],
                        "coldest_min": ref["coldest_min"],
                        "annual_temp": ref["annual_temp"],
                    }
                elif target_features is None:
                    target_features = {
                        "rainfall": ref["rainfall"], "et0": ref["et0"],
                        "frost": ref["frost"], "elevation": ref["elevation"],
                    }

        # Map human-readable irrigation regime to AGROVOC URIs stored in DB
        irrigation_uri = _irrigation_uri(irrigation_regime)

        # ── Step 2: find similar sites ──────────────────────────────────
        if similar_sites_override is not None:
            if not isinstance(similar_sites_override, list):
                raise TypeError("similar_sites_override must be a list of site dicts")
            similar_sites_result = similar_sites_override
        else:
            regional = tier == ep.EVIDENCE_TIER_REGIONAL
            # The regional tier reads every site of the climate (its rows sit at the aggregate
            # pseudo-sites and, for aggregate sources, at field-named ones: policy rule 9), so
            # the soil and rainfall analog filters of the field tier do not apply to it.
            similar_sites_result = await self.get_similar_sites(
                climate_class=target_env.get("climate_class"),
                soil_type=None if regional else target_env.get("soil_type"),
                rainfall_min=None if regional else target_env.get("rainfall_min"),
                rainfall_max=None if regional else target_env.get("rainfall_max"),
                # The Köppen path takes every matching field site (no alphabetical cut);
                # the distance path keeps the 50 nearest.
                limit=None if target_features is None else 50,
                target_features=target_features,
                vector_version=vector_version,
                include_aggregate=regional,
                country=country,
            )
            similar_sites_result = [
                s for s in similar_sites_result
                if regional or s.get("site_kind", ep.SITE_KIND_FIELD) != ep.SITE_KIND_AGGREGATE
            ]
        similar_site_names = [s["name"] for s in similar_sites_result]

        # Per-site weight for distance-weighted aggregation (C.1): nearer analog →
        # higher weight. When there is no agro-climatic vector (legacy path,
        # distance=None) every weight is 1.0, so the weighted mean below collapses
        # exactly to a flat average — no behaviour change.
        site_weights = {
            s["name"]: (1.0 / (1.0 + s["distance"]) if s.get("distance") is not None else 1.0)
            for s in similar_sites_result
        }

        # Hold out sites from the training pool (leave-one-site-out backtest, C.3).
        # Drop them from the analog list AND exclude every trial *observed at* them
        # (even one also linked to an analog site) — otherwise a multi-linked trial
        # leaks the held-out observation back into the prediction.
        excluded_lower: list[str] | None = None
        if exclude_sites:
            excluded_lower = [s.lower() for s in exclude_sites]
            _excluded = set(excluded_lower)
            similar_site_names = [n for n in similar_site_names if n.lower() not in _excluded]

        if not similar_site_names:
            return {
                "target_environment": target_env,
                "similar_sites": [],
                "ranked_varieties": [],
                "purpose": purpose,
                "evidence_tier": tier,
                "evidence": _assess_evidence([]),
                "data_quality": {"total_trials_analyzed": 0, "unique_varieties": 0},
            }

        # ── Step 3: aggregate variety trials from similar sites ─────────
        async with self._driver.session() as session:
            result = await session.run(
                _extrapolate_single_query(purpose, tier),
                site_names=similar_site_names,
                crop=crop,
                irrigation_uri=irrigation_uri,
                production_class=production_class,
                top_n=top_n,
                excluded_sites=excluded_lower,
                site_weights=site_weights,
                now_year=datetime.now(tz=timezone.utc).date().year,
                half_life=recency_half_life,
                target_regime=irrigation_uri,
            )

            ranked = []
            async for record in result:
                ranked.append(_ranked_variety(record, crop))

        # ── Soil-suitability gate (C.5) ────────────────────────────
        # Gate = crop STANDARD tolerance (CropSoilSuitability, EcoCrop) × the
        # parcel's REAL soil (read live from the Soil module). `unsuitable`
        # drops the crop's varieties; `marginal`/`unknown` keep + flag.
        excluded_by_soil: list = []
        soil_gate: dict | None = None
        if filter_soil_suitability:
            soil_gate = await self._soil_gate(crop, parcel_id, tenant_id)
            for v in ranked:
                v["soil_suitability"] = soil_gate["agronomic"]
            if soil_gate["verdict"] == "unsuitable":
                excluded_by_soil = [
                    {"variety": v["variety"], "reason": soil_gate["reason"],
                     "soil_requirement": soil_gate["ph"]}
                    for v in ranked
                ]
                ranked = []

        # ── Weather-based scoring adjustment ─────────────────────
        weather_stats = None
        penalties_applied: dict = {}
        if parcel_id:
            weather_stats = await self.fetch_parcel_weather_stats(parcel_id, tenant_id)
            if weather_stats:
                from app.graph.recommendation import apply_weather_penalties
                ranked, weather_stats, penalties_applied = await apply_weather_penalties(
                    weather_stats=weather_stats,
                    ranked_varieties=ranked,
                    crop=crop,
                    dao=self,
                )

        return {
            "target_environment": target_env,
            "similar_sites": [s["name"] for s in similar_sites_result],
            "similar_sites_detail": similar_sites_result[:5],
            "ranked_varieties": ranked,
            "purpose": purpose,
            "evidence_tier": tier,
            "excluded_by_soil": excluded_by_soil,
            "soil_gate": soil_gate,
            "soil_filter_applied": bool(soil_gate and soil_gate["verdict"] != "unknown"),
            "target_soil": (soil_gate or {}).get("parcel_soil") if soil_gate else None,
            "irrigation_filter_applied": irrigation_regime is not None,
            "weather_stats": weather_stats,
            "weather_penalties": penalties_applied if weather_stats else None,
            "evidence": _assess_evidence(ranked),
            "data_quality": {
                "total_trials_analyzed": sum(v["trial_count"] for v in ranked),
                "unique_varieties": len(ranked),
                "similar_sites_count": len(similar_site_names),
            },
        }

    async def extrapolate_varieties_batch(
        self,
        crops: list[str],
        similar_sites: list[dict],
        irrigation_regime: str | None = None,
        top_n: int = 10,
        exclude_sites: list[str] | None = None,
        recency_half_life: float = 8.0,
        purpose: str = ep.MODE_MAIN,
        tier: str = ep.EVIDENCE_TIER_FIELD,
        zone_pool: str | None = None,
        zone_ctx: Any = None,
        management: str | None = None,
    ) -> dict[str, list[dict]]:
        """``ranked_varieties`` of ``extrapolate_varieties`` for many crops in one query.

        ``zone_pool`` (``matched`` | ``fallback``, with ``zone_ctx``) restricts the regional tier to the
        parcel's GENVCE zone or to the units whose zone cannot be told (see ``_ZONE_POOL_PREDICATE``).

        For each crop, the list equals ``extrapolate_varieties(crop,
        similar_sites_override=similar_sites, irrigation_regime=..., top_n=...,
        exclude_sites=..., recency_half_life=..., purpose=..., tier=...)["ranked_varieties"]``
        (no soil gate, no parcel weather). The analog sites' trials are expanded once, classified
        by the evidence policy once per row, and split by crop with the same predicate; each
        crop's rows then go through the same aggregation (``_EXTRAPOLATE_BODY_CYPHER``) and the
        same per-crop ORDER BY/LIMIT. Every requested crop is a key; a crop without trials
        maps to ``[]``.
        """
        purpose = ep.check_mode(purpose)
        tier = ep.check_tier(tier)
        production_class = ep.production_class(management)
        if not isinstance(similar_sites, list):
            raise TypeError("similar_sites must be a list of site dicts")
        crops = list(dict.fromkeys(crops))
        out: dict[str, list[dict]] = {c: [] for c in crops}
        irrigation_uri = _irrigation_uri(irrigation_regime)
        site_names = [s["name"] for s in similar_sites]
        site_weights = {
            s["name"]: (1.0 / (1.0 + s["distance"]) if s.get("distance") is not None else 1.0)
            for s in similar_sites
        }
        excluded_lower: list[str] | None = None
        if exclude_sites:
            excluded_lower = [s.lower() for s in exclude_sites]
            _excluded = set(excluded_lower)
            site_names = [n for n in site_names if n.lower() not in _excluded]
        if not crops or not site_names:
            return out

        t0 = time.monotonic()
        rows = 0
        async with self._driver.session() as session:
            result = await session.run(
                _extrapolate_batch_query(purpose, tier, zone=zone_pool is not None),
                **_zone_params(zone_pool, zone_ctx),
                site_names=site_names,
                crops=crops,
                irrigation_uri=irrigation_uri,
                production_class=production_class,
                top_n=top_n,
                excluded_sites=excluded_lower,
                site_weights=site_weights,
                now_year=datetime.now(tz=timezone.utc).date().year,
                half_life=recency_half_life,
                target_regime=irrigation_uri,
            )
            async for record in result:
                rows += 1
                crop = record["crop"]
                out[crop].append(_ranked_variety(record, crop))
        logger.debug("extrapolate batch crops=%d sites=%d rows=%d elapsed_s=%.3f",
                     len(crops), len(site_names), rows, time.monotonic() - t0)
        return out

    async def get_trial_sites_summary(self) -> list[dict]:
        """Return all TrialSites with trial count summaries."""
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (ts:TrialSite)
                OPTIONAL MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts)
                OPTIONAL MATCH (mt:ManagementTrial)-[:TRIAL_AT]->(ts)
                WITH ts,
                     count(DISTINCT vt) AS variety_trial_count,
                     count(DISTINCT mt) AS mgmt_trial_count,
                     [s IN collect(DISTINCT vt.source_id) WHERE s IS NOT NULL] AS source_ids
                RETURN ts.name AS name,
                       ts.municipality AS municipality,
                       ts.agroclimaticZone AS agroclimatic_zone,
                       ts.climateClass AS climate_class,
                       ts.soilType AS soil_type,
                       ts.soilTexture AS soil_texture,
                       ts.soilPh AS soil_ph,
                       ts.annualRainfallMm AS annual_rainfall_mm,
                       ts.elevationM AS elevation_m,
                       ts.frostDaysPerYear AS frost_days,
                       ts.latitude AS latitude,
                       ts.longitude AS longitude,
                       variety_trial_count,
                       mgmt_trial_count,
                       source_ids
                ORDER BY variety_trial_count DESC
            """)
            sites = []
            async for record in result:
                site = dict(record)
                site["source_ids"] = sorted(site.get("source_ids") or [])
                sites.append(site)
            return sites

    async def list_trial_evidence(
        self,
        *,
        crop: str,
        similar_sites: list[str],
        variety: str | None,
        irrigation_uri: str | None,
        page: int,
        page_size: int,
        purpose: str = ep.MODE_MAIN,
        tier: str = ep.EVIDENCE_TIER_FIELD,
        zone_ctx: Any = None,
        management: str | None = None,
    ) -> dict:
        """Paginated trials behind a recommendation (count query first, then page).

        ``zone_ctx`` (regional tier of a Spanish parcel): the list is the parcel's own GENVCE zone when
        that has numeric trials of the crop, else the units whose zone cannot be told (the same pool
        rule as the recommendation); the result names it in ``zone_pool``.

        The page lists the distinct trials of the evidence policy that carry a number in the
        recommendation: content-identical trials appear once, excluded sources (BSL) and
        off-purpose records never appear, and ``tier`` selects field or regional (aggregate
        site) evidence. Each item names its ``tier``; in forage mode ``yield_kg_ha`` is kg dry
        matter/ha and ``basis`` says so.
        """
        purpose = ep.check_mode(purpose)
        tier = ep.check_tier(tier)
        zone_on = tier == ep.EVIDENCE_TIER_REGIONAL and zone_ctx is not None and zone_ctx.ready
        where = f"""
            ts.name IN $sites
            {_zone_term(zone_on)}
            AND {ep.cypher_numeric_candidate("vt")}
            AND {RANKING_ELIGIBLE_PREDICATE}
            AND {_CROP_MATCH_PREDICATE}
            AND ($variety IS NULL OR vt.varietyNormalized = $variety)
            AND {ep.cypher_irrigation_match("vt.irrigationRegime")}
            AND {_PRODUCTION_PREDICATE}
        """
        gate = ep.cypher_numeric_tier_gate(tier).rstrip()
        params: dict[str, Any] = {
            "crop": crop,
            "sites": similar_sites,
            "variety": variety,
            "irrigation_uri": irrigation_uri,
            "production_class": ep.production_class(management),
        }
        basis = ep.yield_basis(purpose)
        ck = ep.cypher_content_key("vt")

        page = max(1, page)

        def _s(v: Any) -> Any:
            return v[:200] if isinstance(v, str) else v

        count_query = f"""
                MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite)
                WHERE {where}
                {ep.cypher_row_policy(purpose)}
                {gate}
                WITH DISTINCT vt
                WITH {ck} AS ck
                RETURN count(DISTINCT ck) AS total
                """
        zone_pool: str | None = None
        async with self._driver.session() as session:
            total = 0
            for pool in ((zone_match.POOL_MATCHED, zone_match.POOL_FALLBACK) if zone_on else (None,)):
                zone_pool = pool
                params.update(_zone_params(pool, zone_ctx))
                count_res = await session.run(count_query, **params)
                count_row = await count_res.single()
                total = int(count_row["total"]) if count_row and count_row["total"] else 0
                if total:
                    break
            items_res = await session.run(
                f"""
                MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite)
                WHERE {where}
                {ep.cypher_row_policy(purpose)}
                {gate}
                WITH vt, ts, ep_y
                ORDER BY ts.name
                WITH vt, ep_y, head(collect(DISTINCT ts.name)) AS site
                WITH {ck} AS ck,
                     max(ep_y) AS yield_kg_ha,
                     min(coalesce(vt.mergeKey, elementId(vt))) AS trial_id,
                     min(vt.varietyNormalized) AS variety,
                     min(site) AS site,
                     min(vt.year) AS year,
                     min(vt.irrigationRegime) AS irrigation_regime,
                     min(vt.productionSystem) AS production_system,
                     min(vt.source_id) AS source_id,
                     min(vt.confidence) AS confidence
                RETURN trial_id, variety, site, year, yield_kg_ha, irrigation_regime,
                       production_system, source_id, confidence
                ORDER BY year DESC, variety, site, trial_id
                SKIP $skip LIMIT $limit
                """,
                skip=(page - 1) * page_size,
                limit=page_size,
                **params,
            )
            items = []
            async for rec in items_res:
                item = {k: _s(v) for k, v in dict(rec).items()}
                items.append({**item, "tier": tier, "basis": basis,
                              # None: no regime requested; stated | unknown otherwise
                              "irrigation_status": ep.irrigation_status(
                                  item.get("irrigation_regime"), irrigation_uri)})
        result = {"items": items, "total": total, "page": page, "page_size": page_size,
                  "purpose": purpose, "tier": tier}
        if zone_on:  # which pool the list is: the parcel's own zone or the country level
            result["zone_pool"] = zone_pool
        return result

    async def get_available_crops(self) -> list[dict]:
        """Return distinct crops (one per EPPO code) available in VarietyTrial data.

        Scientific names are localized in the source data, so grouping by name
        duplicates crops. Rows are grouped by EPPO only; the name is the most
        frequent non-"(unknown)" one by trial count. Exact sibling codes of one species
        (``CATALOG_SIBLING_CODES``: ZEAMA/ZEAMX) are one entry under the listed code, with
        distinct varieties counted across both; no other codes are merged.
        """
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (vt:VarietyTrial)
                WHERE vt.cropEppo IS NOT NULL AND vt.cropEppo <> ''
                WITH coalesce($catalog_codes[vt.cropEppo], vt.cropEppo) AS eppo_code, vt
                RETURN eppo_code,
                       count(DISTINCT vt.variety) AS variety_count,
                       count(*) AS trial_count,
                       min(vt.year) AS first_year,
                       max(vt.year) AS last_year,
                       collect(COALESCE(vt.cropScientific, '(unknown)')) AS names,
                       [s IN collect(DISTINCT vt.source_id) WHERE s IS NOT NULL] AS source_ids
            """, catalog_codes=dict(CATALOG_SIBLING_CODES))
            rows = [dict(record) async for record in result]

        merged: dict[str, dict] = {}
        for r in rows:
            eppo = r.get("eppo_code")
            if not eppo:
                continue
            counts: dict[str, int] = {}
            for n in r.get("names") or []:
                if n and n != "(unknown)":
                    counts[n] = counts.get(n, 0) + 1
            name = max(counts.items(), key=lambda kv: (kv[1], kv[0]))[0] if counts else "(unknown)"
            merged[eppo] = {
                "eppo_code": eppo,
                "scientific_name": name,
                "variety_count": r.get("variety_count") or 0,
                "trial_count": r.get("trial_count") or 0,
                "first_year": r.get("first_year"),
                "last_year": r.get("last_year"),
                "source_ids": sorted(r.get("source_ids") or []),
            }
        return sorted(merged.values(), key=lambda c: c["trial_count"], reverse=True)

    # ── Regenerative Sequence Planner ────────────────────────────────────

    @staticmethod
    async def fetch_parcel_weather_stats(parcel_id: str, tenant_id: str = "") -> dict | None:
        """Fetch per-parcel weather stats from the Weather-Map module.

        Calls GET {WEATHER_MAP_URL}/api/weather-map/stats/{parcel_id}?metrics=...
        and returns the ``metrics`` sub-dict, whose keys
        (temperature_avg/water_balance/frost_risk) and nested pct fields
        (heat_stress_pct/deficit_area_pct/high_risk_pct) are exactly what
        app.graph.recommendation.apply_weather_penalties() reads via _safe_get.

        Contract (frozen 2026-06-30): BioOrch and Crop-Health MUST consume the
        same weather source so yield_potential/yield_gap stay comparable.

        Returns None on any error, non-200, or empty metrics → no weather
        penalties applied (fail-safe: ranking unchanged).
        """
        import httpx

        from app.core.config import settings
        from app.services.soil_headers import soil_module_headers
        from app.services.weather_stats_cache import weather_stats_cache

        cache_key = (tenant_id, parcel_id)
        cached = weather_stats_cache.get(cache_key)
        if cached is not None:
            return cached

        url = f"{settings.weather_map_url}/api/weather-map/stats/{parcel_id}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    url,
                    params={"metrics": "temperature_avg,water_balance,frost_risk"},
                    # weather-map verifies the tenant with the module signature.
                    headers=soil_module_headers(tenant_id),
                )
            if resp.status_code != 200:
                logger.info(
                    "weather-map stats for %s returned %d", parcel_id, resp.status_code,
                )
                return None
            metrics = (resp.json() or {}).get("metrics")
            if not metrics:
                return None
            weather_stats_cache.set(cache_key, metrics)
            return metrics
        except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPError) as exc:
            logger.warning("weather-map stats failed for parcel %s: %s", parcel_id, exc)
        except Exception as exc:  # noqa: BLE001 — never break ranking on weather failure
            logger.warning("weather-map stats unexpected error for %s: %s", parcel_id, exc)
        return None

    @staticmethod
    def _extract_value(entity: dict, attr_name: str):
        """Extract a scalar value from an NGSI-LD Property attribute."""
        attr = entity.get(attr_name, {})
        if isinstance(attr, dict):
            return attr.get("value")
        return attr

    # ═══════════════════════════════════════════════════════════════════════════
    # F4: Crop-Health Integration
    # ═══════════════════════════════════════════════════════════════════════════

    async def create_crop_plan(self, parcel_id, season, segments, tenant_id) -> dict:
        """Create one planned AgriCrop per segment.

        No segment is auto-activated (actual planting happens via advance).
        """
        import httpx

        from app.graph.crop_plan import build_segment_entity, sanity_warnings
        parcel_id = _to_parcel_urn(parcel_id)
        client = OrionClient(tenant_id=tenant_id)
        ids, warnings = [], []
        warnings.extend(sanity_warnings(segments))
        try:
            for seq, seg in enumerate(segments):
                entity = build_segment_entity(tenant_id, parcel_id, season, seq, seg)
                try:
                    try:
                        await client.create_entity(entity)
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 409:  # idempotent re-commit
                            # POST /attrs (append): PATCH skips attrs the entity lacks
                            await client.append_entity_attrs(entity["id"], {
                                k: v for k, v in entity.items() if k not in ("id", "type", "@context")
                            })
                        else:
                            warnings.append({"seq": seq, "error": str(e)[:160]})
                            continue
                except Exception as e:  # noqa: BLE001
                    # Never abort the batch on a single-segment failure
                    # (e.g. httpx.ConnectError, timeout, or any transport error).
                    warnings.append({"seq": seq, "error": str(e)[:160]})
                    continue
                ids.append(entity["id"])
            return {"status": "committed", "parcel_id": parcel_id, "season": season,
                    "segments": ids, "warnings": warnings}
        finally:
            await client.close()

    async def get_crop_plan(self, parcel_id, season, tenant_id) -> dict:
        """Return the parcel's plan segments for a season, ordered by seq.

        ``season`` is optional: when omitted the plan for every season is
        returned (the panel renders before any campaign id is known).
        """
        parcel_id = _to_parcel_urn(parcel_id)
        q = f'hasAgriParcel=="{parcel_id}"'
        if season:
            q += f';cropSeason=="{season}"'
        client = OrionClient(tenant_id=tenant_id)
        try:
            rows = await client.query_entities(
                type="AgriCrop",
                q=q,
                limit=50, options="keyValues",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "get_crop_plan: query failed for parcel=%s season=%s (returning empty plan, "
                "not necessarily an empty plan — Orion may be unreachable): %s",
                parcel_id, season, exc,
            )
            rows = []
        finally:
            await client.close()
        rows = sorted(rows, key=lambda r: r.get("seq", 0))
        # Date attrs are stored as NGSI-LD typed literals ({"@type": "Date",
        # "@value": "…"}); keyValues returns them verbatim. Unwrap to plain
        # strings at the read boundary — the frontend contract is scalars.
        date_fields = (
            "sowingWindowStart", "sowingWindowEnd", "expectedTerminationDate",
            "plantingDate", "terminationDate",
        )
        segments = [
            {**r, **{f: _extract_prop_value(r[f]) for f in date_fields if f in r}}
            for r in rows
        ]
        active = next((r["id"] for r in rows if r.get("status") == "active"), None)
        return {"parcel_id": parcel_id, "season": season, "active": active, "segments": segments}

    async def advance_segment(self, parcel_id, season, seq, planting_date, tenant_id) -> dict:
        """Mark a segment sown: set actual plantingDate + activate; demote prior active."""
        from fastapi import HTTPException

        from app.graph.crop_plan import segment_urn
        parcel_id = _to_parcel_urn(parcel_id)
        target_id = segment_urn(tenant_id, parcel_id, season, int(seq))
        _date = {"type": "Property", "value": {"@type": "Date", "@value": planting_date}}
        client = OrionClient(tenant_id=tenant_id)
        try:
            # find currently-active segment to demote
            try:
                rows = await client.query_entities(
                    type="AgriCrop",
                    q=f'hasAgriParcel=="{parcel_id}";cropSeason=="{season}";status=="active"',
                    limit=5, options="keyValues",
                )
            except Exception as exc:
                logger.warning(
                    "advance_segment: failed to read active segment for parcel=%s season=%s; "
                    "aborting to avoid dual-active corruption: %s",
                    parcel_id, season, exc,
                )
                raise HTTPException(
                    status_code=502,
                    detail="Could not read active segment to demote; advance aborted",
                ) from exc
            for prior in rows:
                if prior.get("id") == target_id:
                    continue
                method = prior.get("terminationMethod")
                final = "harvested" if method == "harvest" else "terminated"
                # POST /attrs (append): the dates are absent until now and PATCH /attrs
                # only updates EXISTING attrs, so they would silently not be written.
                await client.append_entity_attrs(prior["id"], {
                    "status": {"type": "Property", "value": final},
                    "terminationDate": _date,
                })
            # activate target with real plantingDate (append, same reason as above)
            await client.append_entity_attrs(target_id, {
                "status": {"type": "Property", "value": "active"},
                "plantingDate": _date,
                "plantingDateSource": {"type": "Property", "value": "manual"},
            })
            # project to parcel commitment (append: may be the parcel's first hasAgriCrop)
            await client.append_entity_attrs(parcel_id, {
                "hasAgriCrop": {"type": "Relationship", "object": target_id},
            })
            return {"status": "advanced", "active": target_id, "season": season}
        finally:
            await client.close()

    async def get_parcel_environment(
        self, parcel_id: str, tenant_id: str = ""
    ) -> dict:
        """Resolve parcel environmental profile WITHOUT requiring assigned crop.

        Used by CropPlanner planning phase. Contrast with get_crop_context()
        which requires AgriParcel.hasAgriCrop.

        Returns: climate_class, soil, irrigation inference, area, centroid,
        campaign status, and inputs_used provenance tags.
        """
        import httpx
        from fastapi import HTTPException

        parcel_id = _to_parcel_urn(parcel_id)
        orion = OrionClient(tenant_id)
        try:
            # ── 1. Fetch parcel entity ──────────────────────────────────
            try:
                parcel = await orion.get_entity(parcel_id)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    return {"error": f"Parcel not found: {parcel_id}"}
                raise
            except httpx.ConnectError:
                raise HTTPException(status_code=502, detail="Orion-LD unreachable")
        finally:
            await orion.close()

        # ── 2. Extract geometry / centroid ──────────────────────────────
        centroid = {"lat": None, "lon": None}
        area_ha = None
        location = parcel.get("location")
        if isinstance(location, dict):
            val = location.get("value", location)
            if isinstance(val, dict):
                point = _geometry_centroid(val.get("coordinates"))
                if point is not None:
                    centroid["lon"], centroid["lat"] = point

        area_raw = _extract_prop_value(parcel.get("area"))
        if area_raw is not None:
            try:
                area_ha = float(area_raw)
            except (TypeError, ValueError):
                pass

        # ── 3. Irrigation inference ─────────────────────────────────────
        irrigation_inferred = None
        irrigation_source = None
        # FIWARE SDM irrigationSystemType (e.g. "drip", "sprinkler")
        irrig_sys = _extract_prop_value(parcel.get("irrigationSystemType"))
        if irrig_sys:
            irrig_lower = str(irrig_sys).lower().strip()
            if any(w in irrig_lower for w in ("drip", "sprinkler", "flood", "pivot", "irrigat")):
                irrigation_inferred = "irrigated"
            else:
                irrigation_inferred = "rainfed"
            irrigation_source = "irrigationSystemType"
        else:
            # Platform custom Property
            irrig_regime = _extract_prop_value(parcel.get("irrigationRegime"))
            if irrig_regime:
                regime_lower = str(irrig_regime).lower().strip()
                if "regad" in regime_lower or "irrig" in regime_lower:
                    irrigation_inferred = "irrigated"
                else:
                    irrigation_inferred = "rainfed"
                irrigation_source = "irrigationRegime"
            else:
                irrigation_inferred = None
                irrigation_source = "unknown"

        # ── 4. Soil ─────────────────────────────────────────────────────
        from app.services.soil_client import get_parcel_soil_properties
        soil_input = "unavailable"
        try:
            soil_data = await get_parcel_soil_properties(parcel_id, tenant_id)
            if soil_data.get("data_available"):
                soil_input = "soil_module"
                wrb_type = None
                # Try to map texture to a WRB reference group via Neo4j
                if soil_data.get("texture"):
                    async with self._driver.session() as session:
                        wrb_result = await session.run(
                            "MATCH (ts:TrialSite) WHERE ts.soilTexture CONTAINS $texture "
                            "RETURN ts.soilType AS wrb LIMIT 1",
                            texture=soil_data["texture"],
                        )
                        wrb_rec = await wrb_result.single()
                        if wrb_rec and wrb_rec["wrb"]:
                            wrb_type = wrb_rec["wrb"]
                soil_data["wrb_type"] = wrb_type
            else:
                soil_data = {"texture": None, "wrb_type": None, "ph": None,
                             "data_available": False, "source": soil_data.get("source", "unavailable")}
        except Exception:  # noqa: BLE001
            soil_data = {"texture": None, "wrb_type": None, "ph": None,
                         "data_available": False, "source": "unavailable"}

        # ── 5. Climate class ────────────────────────────────────────────
        climate_class = None
        climate_detail = None
        climate_input = "unavailable"
        climate_lat = centroid["lat"]
        climate_lon = centroid["lon"]
        if climate_lat is not None and climate_lon is not None:
            # Preferred (feature-flagged): CHELSA v2.1 30-arcsec normals for the parcel's cell
            try:
                from app.services.chelsa_climate import cell_key

                cell = None
                if _chelsa_parcel_climate_enabled():
                    cell = await self.parcel_climate(float(climate_lat), float(climate_lon))
                if cell and cell.get("koppen"):
                    climate_class = cell["koppen"]
                    climate_detail = {
                        "annual_temp_c": cell.get("annual_temp_c"),
                        "annual_rainfall_mm": cell.get("annual_rainfall_mm"),
                        "annual_et0_mm": cell.get("annual_et0_mm"),
                        "coldest_month_min_c": cell.get("coldest_month_min_c"),
                        "frost_days_per_year": None,
                        "source": "chelsa_v2.1",
                        "cell": cell_key(float(climate_lat), float(climate_lon)),
                    }
                    climate_input = "chelsa_v2.1"
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "parcel climate lookup failed, using trial-site fallback: %s",
                    type(exc).__name__,
                )

            # Fallback: nearest TrialSite by haversine (capped ~50km)
            if climate_input == "unavailable":
                async with self._driver.session() as session:
                    from math import asin, cos, radians, sin, sqrt
                    lat_r, lon_r = radians(float(climate_lat)), radians(float(climate_lon))
                    result = await session.run(
                        "MATCH (ts:TrialSite) WHERE ts.latitude IS NOT NULL AND ts.longitude IS NOT NULL "
                        "AND coalesce(ts.climateClassChelsa, ts.climateClass) IS NOT NULL "
                        "RETURN coalesce(ts.climateClassChelsa, ts.climateClass) AS cc, "
                        "ts.latitude AS tlat, ts.longitude AS tlon, "
                        "ts.name AS name, "
                        "coalesce(ts.annualRainfallMmChelsa, ts.annualRainfallMm) AS rain, "
                        "coalesce(ts.annualET0MmChelsa, ts.annualET0Mm) AS et0, "
                        "ts.frostDaysPerYear AS frost "
                        "LIMIT 500"
                    )
                    best_dist = float("inf")
                    best_cc = None
                    best_rec = None
                    async for rec in result:
                        tlat, tlon = rec["tlat"], rec["tlon"]
                        if tlat is None or tlon is None:
                            continue
                        dlat = radians(float(tlat)) - lat_r
                        dlon = radians(float(tlon)) - lon_r
                        a = sin(dlat / 2) ** 2 + cos(lat_r) * cos(radians(float(tlat))) * sin(dlon / 2) ** 2
                        c = 2 * asin(sqrt(a))
                        dist_km = 6371 * c
                        if dist_km < best_dist:
                            best_dist = dist_km
                            best_cc = rec["cc"]
                            best_rec = rec
                    if best_cc and best_rec is not None and best_dist <= 50.0:
                        climate_class = best_cc
                        climate_detail = {
                            "annual_rainfall_mm": best_rec["rain"],
                            "annual_et0_mm": best_rec["et0"],
                            "frost_days_per_year": best_rec["frost"],
                            "annual_temp_c": None,
                            "coldest_month_min_c": None,
                            "source": "trial_proxy",
                            "site": best_rec["name"],
                            "distance_km": round(best_dist, 1),
                        }
                        climate_input = "trial_proxy"

        # ── 6. Campaign status ──────────────────────────────────────────
        has_crop = (
            _resolve_relationship(parcel, "hasAgriCrop")
            or _resolve_relationship(parcel, "refAgriCrop")  # legacy naming
        ) is not None

        return {
            "parcel_id": parcel_id,
            "area_ha": area_ha,
            "centroid": centroid,
            "country": country_at(centroid["lat"], centroid["lon"]),
            "climate_class": climate_class,
            "climate_detail": climate_detail,
            "soil": soil_data,
            "irrigation": {
                "inferred": irrigation_inferred,
                "source": irrigation_source or "unknown",
                "overridable": True,
            },
            "campaign": {
                "assigned": has_crop,
            },
            "inputs_used": {
                "soil": soil_input,
                "climate": climate_input,
            },
        }

    async def recommend_for_conditions(self, conditions: dict) -> dict:
        """Rank crops for explicit conditions (no parcel, no tenant data).

        Canonical ``conditions`` shape: ``climate_class``, ``soil_type``,
        ``soil_ph``, ``soil_texture``, ``irrigation_regime``
        (``secano``/``regadío``/None), ``management`` (``any``/``conventional``/
        ``organic``), ``season`` (``all``/``autumn``/``spring``/``summer``),
        ``crops`` (EPPO list or None), ``top_n``, and optional numeric climate
        inputs ``annual_rainfall_mm``, ``annual_et0_mm``, ``coldest_month_min_c``,
        ``annual_temp_c`` and ``frost_margin_c``, and ``country`` (ISO 3166
        alpha-2 or None; scopes country-specific sowing windows and is part of
        the cache key through ``agro_cond``), and ``lat``/``lon`` (parcel
        centroid or None; select the GGCMI crop calendar cell, also part of the
        cache key through ``agro_cond``). A ``climate_detail`` dict with
        the same keys is also accepted; explicit top-level keys win.

        Organic and conventional units are never pooled (policy rule 10):
        ``management="organic"`` reads ONLY organic units (no yield factor); ``any``,
        ``conventional`` and no value leave organic units out, and a crop with such units
        carries the ``organic_units_excluded`` data gap.

        ``evidence.trial_count`` of each recommendation is the number of trials
        summed over ALL returned varieties (including non-numeric ones), while
        ``yield.n_trials`` is the numeric trials of the best variety only.

        ``purpose`` (``main`` default = each crop's main harvested product, or ``forage``)
        selects the evidence under ``app.graph.evidence_policy``. Field evidence ranks first;
        a crop with no numeric field evidence but numeric aggregate (regional/national)
        evidence at the climate's aggregate sites is returned with ``evidence.tier ==
        "regional"`` (see ``app.graph.recommend`` for the contract).
        """
        from app.graph.recommend import (
            LOW_TRIAL_COUNT,
            build_recommendation,
            rank_recommendations,
            sowing_info,
        )
        from app.services import sowing_windows
        from app.services.chelsa_climate import DEFAULT_FROST_MARGIN_C
        from app.services.crop_reference import CROP_REFERENCE, get_crop_ref_sync

        cond = dict(conditions)
        purpose = ep.check_mode(cond.pop("purpose", None) or ep.MODE_MAIN)
        detail = cond.pop("climate_detail", None) or {}
        for k in _CLIMATE_KEYS:
            if cond.get(k) is None and detail.get(k) is not None:
                cond[k] = detail[k]
        rain, et0 = cond.get("annual_rainfall_mm"), cond.get("annual_et0_mm")
        cold, temp = cond.get("coldest_month_min_c"), cond.get("annual_temp_c")
        margin = cond.get("frost_margin_c")
        margin = DEFAULT_FROST_MARGIN_C if margin is None else float(margin)
        climate_class = cond.get("climate_class")
        irrigation_regime = cond.get("irrigation_regime")
        irrigation_uri = _irrigation_uri(irrigation_regime)
        management = cond.get("management") or "any"
        season = cond.get("season") or "all"
        top_n = int(cond.get("top_n") or 10)
        # Organic and conventional units are never pooled (rule 10): an organic request reads only
        # organic units; any other request leaves them out and says so.
        organic = ep.production_class(management) == ep.PRODUCTION_ORGANIC
        soil_ph, soil_texture = cond.get("soil_ph"), cond.get("soil_texture")
        parcel_soil = {
            "ph": soil_ph, "texture": soil_texture,
            "data_available": soil_ph is not None or soil_texture is not None,
            "source": "conditions",
        }
        mode = _agroclimatic_mode()
        v2_vector_ok = mode in ("v2", "hybrid") and \
            agroclimatic.feature_vector_v2(rain, et0, cold, temp) is not None
        agro_cond = {k: v for k, v in cond.items() if k not in ("top_n", "crops")}
        if purpose != ep.MODE_MAIN:  # the default keeps its recommendation ids
            agro_cond["purpose"] = purpose

        cache_key = json.dumps(
            {"c": {**agro_cond, "frost_margin_c": margin}, "top_n": top_n, "crops": cond.get("crops"), "season": season,
             "management": management, "frost_margin_c": margin, "mode": mode, "purpose": purpose},
            sort_keys=True, default=str,
        )
        cache_key = hashlib.sha256(cache_key.encode()).hexdigest()
        cached = _RECOMMEND_CACHE.get(cache_key)
        if cached is not None:
            if time.monotonic() - cached[0] < _RECOMMEND_TTL:
                logger.debug("recommend cache=hit")
                return copy.deepcopy(cached[1])
            _RECOMMEND_CACHE.pop(cache_key, None)

        # Cold path: runs under the process-wide guard; identical keys share it.
        async def _compute() -> dict:
            t_total = time.monotonic()
            degraded = False
            table_rows = sowing_windows.load_rows()

            crops = cond.get("crops")
            if crops:
                # Requested codes keep the request order; names come from the catalog
                # and a code absent from it keeps its EPPO code as the name.
                try:
                    names = {c.get("eppo_code"): c.get("scientific_name")
                             for c in await self.get_available_crops()}
                except Exception as e:  # noqa: BLE001
                    logger.warning("recommend: crop catalog failed (%s); EPPO codes as names",
                                   type(e).__name__)
                    names = {}
                    degraded = True
                crop_entries = [{"eppo_code": c, "scientific_name": names.get(c) or c} for c in crops]
            else:
                crop_entries = await self.get_available_crops()
            crop_entries = [c for c in crop_entries if c.get("eppo_code")]
            sowings = {c["eppo_code"]: sowing_info(c["eppo_code"], climate_class, table_rows,
                                                   country=cond.get("country"),
                                                   lat=cond.get("lat"), lon=cond.get("lon"),
                                                   irrigation=irrigation_regime)
                       for c in crop_entries}
            if season != "all":
                crop_entries = [c for c in crop_entries if sowings[c["eppo_code"]]["sowing_type"] == season]

            sem = asyncio.Semaphore(RECOMMEND_CONCURRENCY)
            v2_features = {"rainfall": rain, "et0": et0, "coldest_min": cold, "annual_temp": temp}

            # Similar sites depend only on the request conditions, not on the crop:
            # compute them once instead of once per extrapolate call.
            t_sites = time.monotonic()
            koppen_sites: list[dict] | None
            regional_sites: list[dict] = []  # the climate's aggregate pseudo-sites (presence scan)
            # Every site of the climate: what the regional tier reads (its rows sit at the
            # aggregate pseudo-sites and, for aggregate sources, at field-named ones, policy rule 9).
            regional_scan_sites: list[dict] = []
            regional_scan_complete = True
            try:
                # Every matching field site (no alphabetical cut), plus the climate's aggregate
                # sites for the regional tier; one site scan serves both.
                found = await self.get_similar_sites(
                    climate_class=climate_class, soil_type=cond.get("soil_type"),
                    rainfall_min=None, rainfall_max=None, limit=None,
                    target_features=None, vector_version="v1", include_aggregate=True,
                    country=cond.get("country"),
                )
                koppen_sites = [s for s in found
                                if s.get("site_kind", ep.SITE_KIND_FIELD) != ep.SITE_KIND_AGGREGATE]
                regional_sites = [s for s in found if s.get("site_kind") == ep.SITE_KIND_AGGREGATE]
                regional_scan_sites = found
                if cond.get("soil_type"):
                    # The soil analog filter belongs to the field tier: a zone average is not
                    # soil-specific, so the regional tier reads the climate's sites without it.
                    try:
                        regional_scan_sites = await self.get_similar_sites(
                            climate_class=climate_class, soil_type=None,
                            rainfall_min=None, rainfall_max=None, limit=None,
                            target_features=None, vector_version="v1", include_aggregate=True,
                            country=cond.get("country"),
                        )
                    except Exception as e:  # noqa: BLE001
                        logger.warning("recommend: regional site lookup failed (%s); aggregate sites only",
                                       type(e).__name__)
                        regional_scan_sites = regional_sites
                        regional_scan_complete = False
                        degraded = True
            except Exception as e:  # noqa: BLE001
                logger.warning("recommend: shared site lookup failed (%s); per-crop fallback",
                               type(e).__name__)
                koppen_sites = None
                degraded = True
            logger.debug("recommend stage=koppen_sites sites=%s elapsed_s=%.3f",
                         None if koppen_sites is None else len(koppen_sites),
                         time.monotonic() - t_sites)

            # Aggregate sites admitted by the request's country (their own country property).
            country_matched_sites = frozenset(
                s["name"] for s in regional_scan_sites if s.get("matched_by") == "country")

            all_eppos = [c["eppo_code"] for c in crop_entries]

            async def _prefilter(sites: list[dict] | None, stage: str,
                                 tier: str = ep.EVIDENCE_TIER_FIELD,
                                 pool: str | None = None) -> set[str] | None:
                """EPPO codes with analog trials at ``sites``; None = unknown, do not skip."""
                if sites is None:
                    return None
                t0 = time.monotonic()
                try:
                    ok = await self._crops_with_analog_trials(
                        all_eppos, [s["name"] for s in sites], irrigation_uri=irrigation_uri,
                        purpose=purpose, tier=tier, zone_pool=pool, zone_ctx=zone_ctx,
                        management=management,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning("recommend: %s prefilter failed (%s); evaluating all crops",
                                   stage, type(e).__name__)
                    nonlocal degraded
                    degraded = True
                    return None
                logger.debug("recommend stage=%s_prefilter crops=%d with_trials=%d elapsed_s=%.3f",
                             stage, len(all_eppos), len(ok), time.monotonic() - t0)
                return ok

            # Spanish parcel with a point: place it in GENVCE's own climatic zones (CHELSA climatology).
            # None = not asked; not ready = the parcel climate is missing (the answer says so).
            zone_ctx = await self.resolve_zone_context(
                cond.get("country"), cond.get("lat"), cond.get("lon"), irrigation_regime)
            if zone_ctx is not None and not zone_ctx.ready and _chelsa_parcel_climate_enabled():
                degraded = True  # a failed read is transient: never pin the country-level answer
            zone_on = zone_ctx is not None and zone_ctx.ready
            koppen_ok = await _prefilter(koppen_sites, "koppen")
            # Crops whose only numeric evidence is regional/national (aggregate sites).
            # ``regional_known``: the regional count is known (possibly 0), not merely unavailable:
            # the site lookup answered and, when the climate has aggregate sites, so did the
            # prefilter and (below) the batch. A failed pass leaves the count null.
            regional_known = koppen_sites is not None and regional_scan_complete
            regional_ok: set[str] = set()
            regional_pool: dict[str, str | None] = {}  # crop -> zone pool of its regional evidence
            if regional_scan_sites and zone_on:
                matched_pre = await _prefilter(regional_scan_sites, "regional_zone", ep.EVIDENCE_TIER_REGIONAL,
                                               zone_match.POOL_MATCHED)
                fallback_pre = await _prefilter(regional_scan_sites, "regional_country", ep.EVIDENCE_TIER_REGIONAL,
                                                zone_match.POOL_FALLBACK)
                regional_known = regional_known and matched_pre is not None and fallback_pre is not None
                regional_ok = (matched_pre or set()) | (fallback_pre or set())
                for c in regional_ok:
                    regional_pool[c] = zone_match.POOL_MATCHED if c in (matched_pre or set()) \
                        else zone_match.POOL_FALLBACK
            elif regional_scan_sites:
                regional_pre = await _prefilter(regional_scan_sites, "regional", ep.EVIDENCE_TIER_REGIONAL)
                regional_known = regional_known and regional_pre is not None
                regional_ok = regional_pre or set()

            v2_lock = asyncio.Lock()
            v2_state: dict[str, Any] = {}

            async def _v2() -> tuple[list[dict] | None, set[str] | None]:
                async with v2_lock:
                    if "sites" not in v2_state:
                        t0 = time.monotonic()
                        try:
                            v2_state["sites"] = await self.get_similar_sites(
                                climate_class=climate_class, soil_type=cond.get("soil_type"),
                                rainfall_min=None, rainfall_max=None, limit=50,
                                target_features=v2_features, vector_version="v2",
                            )
                        except Exception as e:  # noqa: BLE001
                            logger.warning("recommend: v2 site lookup failed (%s); per-crop fallback",
                                           type(e).__name__)
                            v2_state["sites"] = None
                            nonlocal degraded
                            degraded = True
                        logger.debug("recommend stage=v2_sites sites=%s elapsed_s=%.3f",
                                     None if v2_state["sites"] is None else len(v2_state["sites"]),
                                     time.monotonic() - t0)
                        v2_state["ok"] = await _prefilter(v2_state["sites"], "v2")
                    return v2_state["sites"], v2_state["ok"]

            async def _extrapolate(eppo: str, **extra: Any) -> dict:
                return await self.extrapolate_varieties(
                    crop=eppo, climate_class=climate_class, soil_type=cond.get("soil_type"),
                    irrigation_regime=irrigation_regime, top_n=5, purpose=purpose,
                    management=management, **extra,
                )

            async def _eval_one(entry: dict) -> dict | None:
                eppo = entry["eppo_code"]
                async with sem:
                    try:
                        sowing = sowings[eppo]
                        if koppen_ok is not None and eppo not in koppen_ok:
                            result = {"ranked_varieties": []}  # no analog trials: nothing to extrapolate
                        elif koppen_batch is not None:
                            result = {"ranked_varieties": koppen_batch[eppo]}
                        else:
                            t_crop = time.monotonic()
                            result = await _extrapolate(eppo, similar_sites_override=koppen_sites)
                            logger.debug("recommend stage=extrapolate_koppen crop=%s elapsed_s=%.3f",
                                         eppo, time.monotonic() - t_crop)
                        similarity = "koppen"
                        varieties = result.get("ranked_varieties", [])
                        koppen_rows = varieties
                        if v2_vector_ok and not _has_numeric_mean(varieties):
                            v2_sites, v2_ok = await _v2()
                            if v2_ok is not None and eppo not in v2_ok:
                                result = {"ranked_varieties": []}
                            else:
                                t_v2 = time.monotonic()
                                result = await _extrapolate(
                                    eppo, vector_version="v2", target_features=v2_features,
                                    similar_sites_override=v2_sites,
                                )
                                logger.debug("recommend stage=extrapolate_v2 crop=%s elapsed_s=%.3f",
                                             eppo, time.monotonic() - t_v2)
                            similarity = "vector_v2_fallback"
                            varieties = result.get("ranked_varieties", [])
                            if not _has_numeric_mean(varieties) and _forage_basis_unknown_only(koppen_rows, purpose):
                                varieties, similarity = koppen_rows, "koppen"
                        # No numeric field evidence: numeric aggregate (regional/national) evidence
                        # at the climate's aggregate sites backs the crop instead, flagged as such
                        # (unless the field rows are forage trials of unknown basis, see above).
                        tier = ep.EVIDENCE_TIER_FIELD
                        regional_rows = (regional_batch or {}).get(eppo) or []
                        regional_n = _crop_numeric_trials(regional_rows)
                        zone_block = None
                        if (regional_rows and not _has_numeric_mean(varieties)
                                and not _forage_basis_unknown_only(varieties, purpose)):
                            varieties = regional_rows[:5]
                            tier = ep.EVIDENCE_TIER_REGIONAL
                            similarity = "koppen"
                            if regional_pool.get(eppo) == zone_match.POOL_MATCHED:
                                zone_block = zone_match.zone_match_block(
                                    zone_ctx, zone_match.STATUS_MATCHED, zone_keys.get(eppo))
                            else:
                                zone_block = zone_match.zone_match_block(
                                    zone_ctx, zone_match.STATUS_COUNTRY_LEVEL)
                        if not varieties and eppo in presence:
                            varieties = [_presence_variety(presence[eppo])]
                            tier = ep.EVIDENCE_TIER_REGIONAL
                            similarity = "koppen"
                            zone_block = zone_match.zone_match_block(zone_ctx, zone_match.STATUS_COUNTRY_LEVEL)
                        if not varieties:
                            return None
                        # Best variety = highest mean among those with enough trials;
                        # a 1-2 trial variety must not set the headline yield.
                        varieties = (
                            [v for v in varieties if _numeric_trials(v) >= LOW_TRIAL_COUNT]
                            + [v for v in varieties if _numeric_trials(v) < LOW_TRIAL_COUNT]
                        )
                        # Reference for the relative yield: the median of the crop's trials at the
                        # SAME analog field sites and irrigation regime (it comes with the rows).
                        reference = _analog_reference(
                            varieties,
                            _reference_scope(climate_class if similarity == "koppen" else "vector_v2",
                                             irrigation_uri, purpose),
                        )
                        species = resolve_species(eppo) or eppo
                        heat_tol = await self.get_heat_tolerance(species)
                        soil_verdict = assess_soil_suitability(await self.get_soil_suitability(species), parcel_soil)

                        gaps: list[str] = []
                        water = None
                        gsd_default = False
                        if rain is not None and et0:
                            gsd = get_crop_ref_sync(eppo).get("growing_season_days")
                            if eppo not in CROP_REFERENCE or gsd is None:
                                gsd = _DEFAULT_GROWING_SEASON_DAYS
                                gsd_default = True
                            season_etc = (gsd / 365) * et0
                            deficit = max(0.0, season_etc - rain * 0.7)
                            ratio = deficit / 100.0
                            water = {"level": "low" if ratio < 0.5 else ("medium" if ratio < 1.5 else "high"),
                                     "etc_mm": round(season_etc, 0)}
                        else:
                            gaps.append("climate_detail_unavailable")
                        frost_tol = (heat_tol or {}).get("frost_damage_c")
                        if cold is None:
                            gaps.append("climate_detail_unavailable")
                        elif frost_tol is None:
                            gaps.append("frost_tolerance_unavailable")
                        if cold is None or frost_tol is None:
                            frost_level = "unknown"
                        else:
                            frost_level = "risk" if cold - margin <= frost_tol else "none"
                        if not parcel_soil["data_available"]:
                            gaps.append("soil_unavailable")
                        if organic_excluded.get(eppo):
                            gaps.append("organic_units_excluded")
                        if not any(v.get("source_ids") for v in varieties):
                            gaps.append("sources_unavailable")

                        # Organic request: the evidence is organic units only, so no yield factor
                        # stands in for them (rule 10).
                        assumptions: list[dict] = []
                        assumptions.append({
                            "id": "frost_margin_c", "value": margin,
                            "citation": "ASSUMPTION: conservative default, not a published standard; editable",
                        })
                        if gsd_default:
                            assumptions.append({
                                "id": "growing_season_days_default", "value": _DEFAULT_GROWING_SEASON_DAYS,
                                "citation": "ASSUMPTION: crop cycle unknown; default used for water demand",
                            })

                        rec = build_recommendation(
                            eppo=eppo, scientific_name=entry.get("scientific_name") or eppo,
                            conditions=agro_cond, varieties=varieties, reference=reference,
                            soil_verdict=soil_verdict, water=water, frost_level=frost_level,
                            sowing=sowing, data_gaps_extra=gaps, assumptions=assumptions,
                            tier=tier, purpose=purpose,
                            regional_trial_count=regional_n if tier == ep.EVIDENCE_TIER_FIELD
                            and regional_known else None,
                            zone_match=zone_block,
                            country_matched_sites=country_matched_sites,
                        )
                        if rec is None:
                            return None
                        trust = rec["trust"]
                        trust["data_gaps"] = list(dict.fromkeys(trust["data_gaps"]))
                        if "no_expected_yield" in trust["data_gaps"]:
                            trust["level"] = "low"
                        trust["similarity"] = similarity
                        return rec
                    except Exception as e:  # noqa: BLE001
                        logger.warning("recommend: failed to evaluate %s: %s", eppo, type(e).__name__)
                        nonlocal degraded
                        degraded = True
                        return None

            # Resolve the v2 candidate set up front when some crop may need the fallback,
            # so the crop cap below sees every crop that can produce a recommendation.
            if v2_vector_ok and koppen_ok is not None and any(e not in koppen_ok for e in all_eppos):
                await _v2()
            # Crops with no evidence at either tier whose only evidence is presence (excluded-source
            # trials, e.g. BSL, at the climate's aggregate sites): one batched scan, main mode only.
            # They are reported as regional recommendations with no yield (policy rule 7).
            presence: dict[str, dict] = {}
            if regional_sites and ep.presence_only_applies(purpose):
                has_evidence = (koppen_ok or set()) | (v2_state.get("ok") or set()) | regional_ok
                presence_candidates = [e for e in all_eppos if e not in has_evidence]
                if presence_candidates:
                    t_pres = time.monotonic()
                    try:
                        presence = await self.regional_presence_trials(
                            presence_candidates, [s["name"] for s in regional_sites],
                            irrigation_uri=irrigation_uri, purpose=purpose, management=management,
                        )
                    except Exception as e:  # noqa: BLE001
                        logger.warning("recommend: presence evidence failed (%s); skipped",
                                       type(e).__name__)
                        degraded = True
                    logger.debug("recommend stage=presence crops=%d with_presence=%d elapsed_s=%.3f",
                                 len(presence_candidates), len(presence), time.monotonic() - t_pres)
            # The crop cap applies to the crops that can produce a recommendation
            # (analog trials under Köppen or v2); skipped crops cost nothing. When a
            # prefilter is unavailable the cap falls back to the catalog head.
            if koppen_ok is None or ("ok" in v2_state and v2_state["ok"] is None):
                candidates = list(all_eppos)[:_RECOMMEND_MAX_CROPS]
            else:
                field_capable = [e for e in all_eppos
                                 if e in koppen_ok or e in (v2_state.get("ok") or ())]
                regional_only = [e for e in all_eppos if e in regional_ok and e not in set(field_capable)]
                presence_only = [e for e in all_eppos if e in presence]
                candidates = (field_capable + regional_only + presence_only)[:_RECOMMEND_MAX_CROPS]
            candidate_set = set(candidates)
            crop_entries = [c for c in crop_entries if c["eppo_code"] in candidate_set]

            # Köppen extrapolation for every evaluated crop in ONE query: the analog
            # sites' trials are scanned once instead of once per crop. Same result
            # per crop as extrapolate_varieties; on failure each crop falls back to it.
            koppen_batch: dict[str, list[dict]] | None = None
            batch_crops = [c["eppo_code"] for c in crop_entries
                           if koppen_ok is None or c["eppo_code"] in koppen_ok]
            if koppen_sites is not None and batch_crops:
                t_batch = time.monotonic()
                try:
                    koppen_batch = await self.extrapolate_varieties_batch(
                        batch_crops, koppen_sites, irrigation_regime=irrigation_regime, top_n=5,
                        purpose=purpose, management=management,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning("recommend: batched extrapolation failed (%s); per-crop fallback",
                                   type(e).__name__)
                logger.debug("recommend stage=extrapolate_koppen_batch crops=%d elapsed_s=%.3f",
                             len(batch_crops), time.monotonic() - t_batch)

            # Regional tier: the same batched extrapolation over every site of the climate (numeric
            # evidence only, the rows the policy classes as regional); the crop's regional trial
            # count comes from the query, uncut.
            regional_batch: dict[str, list[dict]] | None = None
            zone_keys: dict[str, list[str]] = {}
            regional_crops = [c["eppo_code"] for c in crop_entries if c["eppo_code"] in regional_ok]
            if regional_scan_sites and regional_crops:
                t_reg = time.monotonic()
                try:
                    if zone_on:
                        regional_batch = {}
                        for pool in (zone_match.POOL_MATCHED, zone_match.POOL_FALLBACK):
                            pool_crops = [c for c in regional_crops if regional_pool.get(c) == pool]
                            if pool_crops:
                                regional_batch.update(await self.extrapolate_varieties_batch(
                                    pool_crops, regional_scan_sites, irrigation_regime=irrigation_regime,
                                    top_n=5, purpose=purpose, tier=ep.EVIDENCE_TIER_REGIONAL,
                                    zone_pool=pool, zone_ctx=zone_ctx, management=management,
                                ))
                        matched_crops = [c for c in regional_crops
                                         if regional_pool.get(c) == zone_match.POOL_MATCHED]
                        zone_keys = await self.regional_zone_keys(
                            matched_crops, [s["name"] for s in regional_scan_sites], zone_ctx, irrigation_uri,
                            purpose=purpose, management=management)
                    else:
                        regional_batch = await self.extrapolate_varieties_batch(
                            regional_crops, regional_scan_sites, irrigation_regime=irrigation_regime,
                            top_n=5, purpose=purpose, tier=ep.EVIDENCE_TIER_REGIONAL,
                            management=management,
                        )
                except Exception as e:  # noqa: BLE001
                    logger.warning("recommend: regional tier failed (%s); field evidence only",
                                   type(e).__name__)
                    degraded = True
                    regional_known = False
                logger.debug("recommend stage=extrapolate_regional crops=%d elapsed_s=%.3f",
                             len(regional_crops), time.monotonic() - t_reg)

            # Organic units a non-organic request leaves out: one scan over the evaluated crops.
            organic_excluded: dict[str, int] = {}
            if not organic:
                scan_names = list(dict.fromkeys(
                    s["name"] for s in (koppen_sites or []) + regional_scan_sites))
                try:
                    organic_excluded = await self.organic_units_excluded(
                        [c["eppo_code"] for c in crop_entries], scan_names,
                        irrigation_uri=irrigation_uri, purpose=purpose)
                except Exception as e:  # noqa: BLE001
                    logger.warning("recommend: organic exclusion count failed (%s); gap omitted",
                                   type(e).__name__)
                    degraded = True

            t_eval = time.monotonic()
            results = await asyncio.gather(*(_eval_one(c) for c in crop_entries))
            recs = [r for r in results if r]
            logger.debug("recommend stage=evaluate crops=%d recs=%d elapsed_s=%.3f",
                         len(crop_entries), len(recs), time.monotonic() - t_eval)
            if koppen_ok is None or ("ok" in v2_state and v2_state["ok"] is None):
                with_analogs: int | None = None  # a prefilter was unavailable: count unknown
            else:
                with_analogs = len(koppen_ok | (v2_state.get("ok") or set()) | regional_ok | set(presence))
            echo = {**{k: v for k, v in cond.items() if k != "climate_detail"}, "purpose": purpose}
            response = {
                "status": "ok",
                "evidence_policy": ep.POLICY_VERSION,
                "conditions": echo,
                "recommendations": rank_recommendations(recs)[:top_n],
                "data_quality": {"crops_evaluated": len(crop_entries), "crops_with_trials": len(recs),
                                 "crops_with_analog_trials": with_analogs},
            }
            if not degraded:  # never pin a partial answer produced by a transient failure
                if len(_RECOMMEND_CACHE) >= _RECOMMEND_CACHE_MAX:
                    _RECOMMEND_CACHE.pop(next(iter(_RECOMMEND_CACHE)), None)
                _RECOMMEND_CACHE[cache_key] = (time.monotonic(), copy.deepcopy(response))
            logger.debug("recommend stage=total cache=miss degraded=%s elapsed_s=%.3f",
                         degraded, time.monotonic() - t_total)
            return response

        guard = _cold_guard()
        task = guard.inflight.get(cache_key)
        if task is None:
            async def _guarded() -> dict:
                async with guard.sem:
                    return await _compute()

            task = asyncio.ensure_future(_guarded())
            guard.inflight[cache_key] = task
            task.add_done_callback(functools.partial(_inflight_done, guard.inflight, cache_key))
        else:
            logger.debug("recommend cache=inflight")
        # shield: a caller that goes away must not cancel the computation others await
        return copy.deepcopy(await asyncio.shield(task))

    async def get_crop_context(
        self, parcel_id: str, tenant_id: str = "", gdd: float | None = None
    ) -> dict:
        """Return full calibrated agronomic context for a parcel."""
        import httpx
        from fastapi import HTTPException

        parcel_id = _to_parcel_urn(parcel_id)
        orion = OrionClient(tenant_id)
        try:
            # ── 1. Fetch parcel entity ──────────────────────────────────────
            try:
                parcel = await orion.get_entity(parcel_id)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    return {"error": f"Parcel not found: {parcel_id}"}
                raise
            except httpx.ConnectError:
                raise HTTPException(status_code=502, detail="Orion-LD unreachable")
            except Exception as e:  # noqa: BLE001
                return {"error": f"Failed to read parcel: {e!s}"}

            crop_uri = _resolve_relationship(parcel, "hasAgriCrop") or _resolve_relationship(parcel, "refAgriCrop")
            variety_uri = _resolve_relationship(parcel, "hasAgriCropVariety")
            management = _extract_prop_value(parcel.get("management"))
            if not crop_uri:
                return {"error": "Parcel has no crop assigned"}
            # Season window: the platform's resolved crop cycle. The parcel's legacy copy (no
            # longer written, so stale or planned) only when the platform is unreachable; when it
            # answered with no current cycle, the next one is shown with its own provenance.
            cycles = await fetch_crop_cycles(parcel_id, tenant_id)
            if cycles is None:
                season_start = _extract_prop_value(parcel.get("cropSeasonStart"))
                season_end = _extract_prop_value(parcel.get("cropSeasonEnd"))
                start_provenance = None
            else:
                cur = cycles.get("current") or cycles.get("next") or {}
                season_start = (cur.get("start") or {}).get("date")
                season_end = (cur.get("end") or {}).get("date")
                start_provenance = (cur.get("start") or {}).get("provenance")

            # ── 2. Fetch crop entity ────────────────────────────────────────
            crop_eppo = crop_uri.split(":")[-1] if crop_uri else "unknown"
            crop_name = None
            crop_scientific = None
            try:
                crop_entity = await orion.get_entity(crop_uri)
                crop_name = _extract_prop_value(crop_entity.get("name"))
                crop_scientific = _extract_prop_value(crop_entity.get("scientificName"))
                # Prefer the AgriCrop's own species (EPPO code, e.g. "SECCE") over
                # the URN segment (e.g. "2026") — the URN suffix is a season id.
                species_val = _extract_prop_value(crop_entity.get("species"))
                if species_val:
                    crop_eppo = species_val
            except Exception:  # noqa: BLE001,S110
                pass

            variety_name = unquote(variety_uri.split(":")[-1]) if variety_uri else None
            species_query = crop_name or crop_scientific or crop_eppo
            species_slug = resolve_species(crop_eppo) or resolve_species(species_query) or species_query
            phenology = await self.get_phenology_params(
                species=species_query, cultivar=variety_name, management=management, gdd=gdd,
            )
            thermal = await self.get_heat_tolerance(species_query)
            soil_req = await self.get_soil_suitability(species_slug)

            soil_actual = await get_parcel_soil_properties(parcel_id, tenant_id)
            # C.5 graded verdict (assess handles missing tolerance / unavailable soil → unknown)
            soil_suitability = assess_soil_suitability(soil_req, soil_actual)

            # ── 3. Fetch latest CropHealthAssessment (normalized NGSI-LD) ──
            soil_sensors: dict = {"available": False}
            try:
                entities = await orion.query_entities(
                    type="CropHealthAssessment",
                    q=f'hasAgriParcel=="{parcel_id}"|refAgriParcel=="{parcel_id}"',
                    limit=1,
                )
                if entities and isinstance(entities, list):
                    a = entities[0]
                    ph = _extract_prop_value(a.get("soilPh"))
                    ec = _extract_prop_value(a.get("soilEC"))
                    moisture = _extract_prop_value(a.get("soilMoisturePct"))
                    temp = _extract_prop_value(a.get("soilTemperatureC"))
                    if any(v is not None for v in (ph, ec, moisture, temp)):
                        soil_sensors = {
                            "available": True,
                            "last_reading": _extract_prop_value(a.get("assessedAt")) or "",
                            "ph": ph,
                            "ec_ds_m": ec,
                            "moisture_pct": moisture,
                            "temperature_c": temp,
                        }
            except Exception:  # noqa: BLE001,S110
                pass

        finally:
            await orion.close()

        if phenology and not phenology.get("is_default", True):
            if variety_name and management:
                phenology_source = f"bioorchestrator:variety:{variety_name}:management:{management}"
            elif variety_name:
                phenology_source = f"bioorchestrator:variety:{variety_name}"
            else:
                phenology_source = f"bioorchestrator:species:{crop_eppo}"
        else:
            phenology_source = "default"

        return {
            "parcel_id": parcel_id,
            "crop": {"eppo": crop_eppo, "name": crop_name or crop_eppo, "scientific_name": crop_scientific},
            "variety": {"name": variety_name, "uri": variety_uri} if variety_name else None,
            "management": management,
            "season": {"start": season_start, "start_provenance": start_provenance, "end": season_end, "gdd_accumulated": gdd, "current_stage": phenology.get("stage") if phenology else None},
            "phenology": {"stage": phenology.get("stage"), "kc": phenology.get("kc"), "ky": phenology.get("ky"), "d1": phenology.get("d1"), "d2": phenology.get("d2"), "mds_ref": phenology.get("mds_ref"), "base_temp": phenology.get("stage_base_temp"), "stage_gdd_min": phenology.get("stage_gdd_min"), "stage_gdd_max": phenology.get("stage_gdd_max")} if phenology else None,
            "thermal_limits": {"heat_damage_c": thermal.get("heat_damage_c"), "frost_damage_c": thermal.get("frost_damage_c"), "heat_accum_hours": thermal.get("heat_accum_hours")} if thermal else None,
            "soil": {"requirements": {"ph_min": soil_req.get("ph_min") if soil_req else None, "ph_max": soil_req.get("ph_max") if soil_req else None, "textures": soil_req.get("textures", []) if soil_req else [], "drainage": soil_req.get("drainage") if soil_req else None, "depth_min_cm": soil_req.get("depth_min_cm") if soil_req else None, "salinity_max_ds_m": soil_req.get("salinity_max_ds_m") if soil_req else None}, "actual": soil_actual, "suitability": soil_suitability},
            "soil_sensors": soil_sensors,
            "phenology_source": phenology_source,
            "match_level": phenology.get("match_level") if phenology else "none",
            "provenance": phenology.get("provenance") if phenology else None,
        }

    async def run_crop_simulation(
        self,
        parcel_id: str,
        tenant_id: str = "",
        crop_slug: str | None = None,
        sowing_date_str: str | None = None,
        irrigation: str = "rainfed",
        engine: str = "aquacrop",
        today: date | None = None,
        progress=None,
        sowing_method: str | None = None,
    ) -> dict:
        """Simulate the parcel's crop season with every input taken from real data.

        Crop: ``crop_slug`` or the parcel's ``hasAgriCrop``. Sowing date: the
        argument, else the platform's current crop cycle, else the latest completed
        sowing operation, else (only when the platform is unreachable) the parcel's
        legacy ``cropSeasonStart``; a plan never counts. Sowing method: the argument,
        else the completed sowing operation on the simulated sowing date (``sowingMethod``),
        else ``conventional`` (bare soil); see ``sim_inputs.sowing_method_from_operations``.
        Soil: Soil module summary.
        Weather: parcel daily series + self-hosted archive (see ``sim_weather``).
        Raises ``SimulationError`` (422 input, 404 parcel, 503 upstream); nothing
        is defaulted or synthesized.
        """
        import httpx

        from app.services.crop_simulation import (
            check_irrigation,
            check_sowing_method,
            get_engine,
            run_crop_simulation,
        )
        from app.services.sim_errors import SimulationError
        from app.services.sim_inputs import (
            SimInputError,
            resolve_sowing_date,
            sowing_method_from_operations,
        )

        today = today or datetime.now(tz=timezone.utc).date()
        check_irrigation(irrigation)
        sim_engine = get_engine(engine)
        check_sowing_method(sowing_method)

        # ── 1. Parcel: crop and centroid ──
        orion = OrionClient(tenant_id)
        try:
            try:
                parcel = await orion.get_entity(parcel_id)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise SimulationError("parcel_not_found", f"Parcel not found: {parcel_id}", 404) from e
                logger.error("crop_simulation_parcel_failed parcel=%s tenant=%s status=%s",
                             parcel_id, tenant_id, e.response.status_code)
                raise SimulationError("parcel_unavailable", "The parcel could not be read.", 503) from e
            except httpx.HTTPError as e:
                logger.error("crop_simulation_parcel_failed parcel=%s tenant=%s error=%s",
                             parcel_id, tenant_id, type(e).__name__)
                raise SimulationError("parcel_unavailable", "The parcel could not be read.", 503) from e
        finally:
            await orion.close()

        identifier = crop_slug
        if not identifier:
            crop_uri = _resolve_relationship(parcel, "hasAgriCrop") or _resolve_relationship(parcel, "refAgriCrop")
            if not crop_uri:
                raise SimulationError(
                    "crop_missing", "No crop is assigned to the parcel and no crop_slug was provided.")
            identifier = crop_uri.split(":")[-1]
        crop = sim_engine.resolve_crop(identifier)

        location = parcel.get("location")
        point = None
        if isinstance(location, dict):
            val = location.get("value", location)
            if isinstance(val, dict):
                point = _geometry_centroid(val.get("coordinates"))
        if point is None:
            raise SimulationError("parcel_location_missing", "The parcel has no usable geometry.")
        lon, lat = point

        # ── 2. Sowing date: explicit, else the platform's crop cycle, else a completed sowing ──
        # What happened counts, never a plan. The parcel's legacy season copy is used only when
        # the platform is unreachable: its "no current cycle" is an answer, not an outage.
        async def _read_operations() -> list:
            orion_ops = OrionClient(tenant_id)
            try:
                found = await orion_ops.query_entities(
                    type="AgriParcelOperation",
                    q=f'hasAgriParcel=="{parcel_id}"|refAgriParcel=="{parcel_id}"',
                    limit=200,
                )
            finally:
                await orion_ops.close()
            return found if isinstance(found, list) else []

        ops: list | None = None
        sowing_provenance: str | None = None
        cycle_start: dict = {}
        cycles: dict | None = None
        if not sowing_date_str:
            cycles = await fetch_crop_cycles(parcel_id, tenant_id)
            cycle_start = (((cycles or {}).get("current") or {}).get("start")) or {}
        if sowing_date_str or cycle_start.get("date"):
            raw = sowing_date_str or cycle_start["date"]
            try:
                sowing_date = date.fromisoformat(raw[:10])
            except ValueError as e:
                raise SimulationError(
                    "invalid_sowing_date", f"Invalid sowing date: {raw} (use YYYY-MM-DD).") from e
            if sowing_date_str:
                sowing_source = "request"
            else:
                sowing_source, sowing_provenance = "crop_cycle", cycle_start.get("provenance")
        else:
            try:
                ops = await _read_operations()
            except Exception as e:
                logger.error("crop_simulation_operations_failed parcel=%s tenant=%s error=%s",
                             parcel_id, tenant_id, type(e).__name__)
                raise SimulationError(
                    "operations_unavailable", "The sowing operations could not be read.", 503) from e
            try:
                sowing_date = resolve_sowing_date(ops, today)
                sowing_source, sowing_provenance = "field_operations", "actual"
            except SimInputError as e:
                legacy = _extract_prop_value(parcel.get("cropSeasonStart")) if cycles is None else None
                try:
                    sowing_date = date.fromisoformat(str(legacy)[:10]) if legacy else None
                except ValueError:
                    sowing_date = None
                if sowing_date is None:
                    raise SimulationError(
                        "sowing_date_missing",
                        "No sowing date available: provide one or record a sowing operation.") from e
                sowing_source = "legacy_parcel_season"

        # ── 3. Sowing method: the request, else the completed sowing of that day, else bare soil ──
        method_warnings: list[str] = []
        if sowing_method is not None:
            method, method_source = sowing_method, "request"
        else:
            if ops is None:
                try:
                    ops = await _read_operations()
                except Exception as e:  # noqa: BLE001 — the method is optional: degrade to bare soil + warning
                    logger.warning("crop_simulation_sowing_method_unread parcel=%s tenant=%s error=%s",
                                   parcel_id, tenant_id, type(e).__name__)
                    method_warnings.append(
                        "sowing method: field operations could not be read; bare soil (conventional) assumed")
            found, found_warns = sowing_method_from_operations(ops or [], sowing_date)
            method_warnings.extend(found_warns)
            method, method_source = (found, "field_operations") if found else ("conventional", "default")

        return await run_crop_simulation(
            engine_name=engine, parcel_id=parcel_id, tenant_id=tenant_id, crop=crop,
            sowing_date=sowing_date, sowing_source=sowing_source, irrigation=irrigation,
            lat=lat, lon=lon, today=today, sowing_provenance=sowing_provenance,
            sowing_method=method, sowing_method_source=method_source, input_warnings=method_warnings,
            progress=progress)

    async def get_organic_inputs(self, eppo: str) -> dict:
        """Get FiBL organic inputs compatible with this crop's pests. Non-blocking."""
        import csv
        import os as _os
        from pathlib import Path

        import httpx

        result: dict = {"inputs": [], "source_unavailable": False}

        # 1) Get pests for this crop from EPPO
        api_key = _os.getenv("EPPO_API_TOKEN") or _os.getenv("EPPO_API_KEY", "")
        pest_names: list[str] = []
        if api_key:
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(
                        f"https://api.eppo.int/gd/v2/taxons/taxon/{eppo}/pests",
                        headers={"X-Api-Key": api_key},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        pests = data if isinstance(data, list) else data.get("pests", [])
                        pest_names = [
                            p.get("scientificName", p.get("prefName", "")).lower()
                            for p in pests if isinstance(p, dict)
                        ]
            except Exception as e:  # noqa: BLE001
                logger.warning("EPPO pest fetch for organic inputs failed: %s", e)

        # 2) Cross-reference with FiBL
        csv_path = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "fibl_inputs.csv"
        if csv_path.exists() and pest_names:
            try:
                with csv_path.open("r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        target = (row.get("target_pests", "") or "").lower()
                        if any(p in target for p in pest_names):
                            result["inputs"].append({
                                "product": row.get("product_name", ""),
                                "active_substance": row.get("active_substance", ""),
                                "category": row.get("category", ""),
                            })
            except Exception as e:  # noqa: BLE001
                logger.warning("FiBL lookup failed: %s", e)

        return result

    # ── Action Rules (agronomist-editable, evaluated by BioOrchestrator) ─────────

    async def get_action_rules(
        self, species: str | None = None, stage: str | None = None, role: str | None = None
    ) -> list[dict]:
        """Candidate ActionRules: species-linked (active) + generic (no species link).

        Filtering by stage/role is intentionally permissive — this returns the
        candidate set; BioOrchestrator's rule_engine performs the precise
        condition match (Contract 2: bioorch evaluates, crop-health observes).
        """
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (r:ActionRule) WHERE r.active = true
                OPTIONAL MATCH (s:Species)-[:HAS_RULE]->(r)
                WITH r, collect(s.name) AS linked
                WHERE size(linked) = 0
                   OR ($species IS NOT NULL AND any(n IN linked WHERE toLower(n) = toLower($species)))
                RETURN r.id AS id, r.name AS name, r.category AS category,
                       r.priority AS priority, r.active AS active,
                       r.conditions AS conditions, r.action AS action,
                       r.source_doi AS source_doi, r.source_short AS source_short
                """,
                species=species,
            )
            rows = await result.data()
        out = []
        for r in rows:
            r = dict(r)
            try:
                r["conditions"] = json.loads(r.get("conditions") or "{}")
                r["action"] = json.loads(r.get("action") or "{}")
            except (TypeError, ValueError):
                r["conditions"], r["action"] = {}, {}
            out.append(r)
        return out

    async def get_action_rule(self, rule_id: str) -> dict | None:
        """Fetch a single ActionRule by id from the active candidate set."""
        rules = await self.get_action_rules()
        return next((r for r in rules if r["id"] == rule_id), None)

    async def create_action_rule(self, rule: dict) -> dict:
        """Create (or replace) an ActionRule node, optionally linked to species."""
        async with self._driver.session() as session:
            await session.run(
                """
                MERGE (r:ActionRule {id: $id})
                SET r.name=$name, r.category=$category, r.priority=$priority, r.active=$active,
                    r.conditions=$conditions, r.action=$action,
                    r.source_doi=$source_doi, r.source_short=$source_short, r.created_at=$created_at
                WITH r
                FOREACH (sp IN $species_links |
                  MERGE (s:Species {name: sp}) MERGE (s)-[:HAS_RULE]->(r))
                """,
                id=rule["id"], name=rule.get("name", ""), category=rule.get("category", ""),
                priority=rule.get("priority", 0), active=rule.get("active", True),
                conditions=json.dumps(rule.get("conditions", {})),
                action=json.dumps(rule.get("action", {})),
                source_doi=rule.get("source_doi"), source_short=rule.get("source_short"),
                created_at=rule.get("created_at"), species_links=rule.get("species_links", []),
            )
        return {"status": "created", "id": rule["id"]}

    async def update_action_rule(self, rule_id: str, patch: dict) -> dict:
        """Partially update an ActionRule's scalar fields and/or JSON blobs."""
        sets, params = [], {"id": rule_id}
        for k in ("name", "category", "priority", "active", "source_doi", "source_short"):
            if k in patch:
                sets.append(f"r.{k} = ${k}")
                params[k] = patch[k]
        for jk in ("conditions", "action"):
            if jk in patch:
                sets.append(f"r.{jk} = ${jk}")
                params[jk] = json.dumps(patch[jk])
        if not sets:
            return {"status": "noop", "id": rule_id}
        async with self._driver.session() as session:
            await session.run(f"MATCH (r:ActionRule {{id: $id}}) SET {', '.join(sets)}", **params)
        return {"status": "updated", "id": rule_id}


def _stable_key(x: Any) -> tuple[bool, str]:
    """Total order over collected values that may include None (None last)."""
    return (x is None, str(x))


def _ranked_variety(record: Any, crop: str) -> dict:
    """Map one aggregated extrapolation row to the ranked-variety dict."""
    # Merge disease scores across trials: take best (highest) per disease
    merged_diseases: dict[str, dict] = {}
    # collect() order follows the scan, which the query plan decides; sort the
    # order-dependent inputs so the output never depends on it.
    ds_list = sorted(record.get("disease_scores_list") or [], key=_stable_key)
    for ds_raw in ds_list:
        if not ds_raw:
            continue
        try:
            ds = json.loads(ds_raw) if isinstance(ds_raw, str) else ds_raw
            for dk, dv in ds.items():
                if isinstance(dv, dict) and dv.get("value") is not None \
                        and (dk not in merged_diseases or dv["value"] > merged_diseases[dk]["value"]):
                    merged_diseases[dk] = dv
        except (json.JSONDecodeError, TypeError):
            pass

    # Merge agronomic traits: take first non-null
    merged_traits: dict[str, dict] = {}
    at_list = sorted(record.get("agronomic_traits_list") or [], key=_stable_key)
    for at_raw in at_list:
        if not at_raw:
            continue
        try:
            at = json.loads(at_raw) if isinstance(at_raw, str) else at_raw
            for tk, tv in at.items():
                if tk not in merged_traits:
                    merged_traits[tk] = tv
        except (json.JSONDecodeError, TypeError):
            pass

    # Confidence provenance
    conf_list = [c for c in (record.get("confidence_levels") or []) if c]
    best_confidence = "high" if "high" in conf_list else ("medium" if "medium" in conf_list else (conf_list[0] if conf_list else None))

    variety_name = record["variety"]
    return {
        "variety": variety_name,
        "crop_uri": f"urn:ngsi-ld:AgriCrop:{crop}",
        "variety_uri": f"urn:ngsi-ld:AgriCrop:{crop}:{quote(str(variety_name), safe='')}",
        "mean_yield_kg_ha": round(record["mean_yield"], 1) if record["mean_yield"] else None,
        "min_yield_kg_ha": round(record["min_yield"], 1) if record["min_yield"] else None,
        "max_yield_kg_ha": round(record["max_yield"], 1) if record["max_yield"] else None,
        "stddev_yield_kg_ha": round(record["stddev_yield"], 1) if record["stddev_yield"] else None,
        "trial_count": record["trial_count"],
        "numeric_yield_count": record["numeric_yield_count"],
        # forage mode: distinct trials with a kg value but no known basis (no number from them)
        "unknown_basis_trial_count": int(record.get("unconverted_count") or 0),
        # crop level, repeated on every row: distinct trials of the other purpose (main mode:
        # forage) at the same sites, counted and never averaged
        "crop_other_purpose_trials": int(record.get("other_n") or 0),
        # crop level, repeated on every row: numeric trials summed over ALL the crop's varieties
        # (before the top_n cut), so a capped list never truncates the count
        "crop_numeric_trial_count": int(record.get("crop_numeric_n") or 0),
        # crop level, repeated on every row: median of the crop's distinct policy numbers at the
        # query's sites in the requested irrigation regime, and how many trials it rests on
        "crop_reference_median_kg_ha": (float(record["ref_median"])
                                         if record.get("ref_median") is not None else None),
        "crop_reference_n": int(record.get("ref_n") or 0),
        "derived_trial_count": record["derived_count"],
        "yield_provenance": _yield_provenance(record["derived_count"], record["trial_count"]),
        "trial_years": sorted(record["years"]),
        "trial_sites": sorted(record["sites"]),
        "irrigation_regimes": sorted(record["irrigation_regimes"], key=_stable_key),
        # distinct trials whose source states no irrigation regime (kept under a requested
        # regime, never counted as matching it)
        "irrigation_unknown_trial_count": int(record.get("regime_unknown_count") or 0),
        "production_systems": sorted(record["production_systems"], key=_stable_key),
        "disease_scores": merged_diseases,
        "agronomic_traits": merged_traits,
        "confidence": best_confidence,
        "source_ids": sorted(s for s in (record.get("source_ids") or []) if s),
    }


def _yield_provenance(derived_count: int | None, trial_count: int | None) -> str:
    """Provenance of a variety's aggregated yield: 'measured' | 'partial' | 'derived'.

    'derived' trials carry a persisted yieldKgHa estimated from BSL notes via an
    empirical per-crop factor (yieldDerivationMethod set). extrapolate ranks
    derived and measured yields identically; this flag surfaces to consumers when
    a ranking rests on approximations rather than field measurements.
    """
    d = derived_count or 0
    t = trial_count or 0
    if d <= 0:
        return "measured"
    if d >= t:
        return "derived"
    return "partial"


def _is_position(value) -> bool:
    return (
        isinstance(value, list)
        and len(value) >= 2
        and all(isinstance(c, (int, float)) and not isinstance(c, bool) for c in value[:2])
    )


def _geometry_centroid(coords) -> tuple[float, float] | None:
    """(lon, lat) of a GeoJSON Point, Polygon or MultiPolygon coordinate array.

    Polygons use the mean of the outer ring's vertices (closing vertex excluded);
    MultiPolygons use their first polygon. Each step descends one nesting level,
    so it always terminates. Returns None for anything malformed.
    """
    node = coords
    while isinstance(node, list) and node and isinstance(node[0], list):
        if _is_position(node[0]):
            ring = [p for p in node if _is_position(p)]
            if len(ring) > 1 and ring[0][:2] == ring[-1][:2]:
                ring = ring[:-1]
            if not ring:
                return None
            lon = sum(p[0] for p in ring) / len(ring)
            lat = sum(p[1] for p in ring) / len(ring)
            return (lon, lat)
        node = node[0]
    if _is_position(node):
        return (node[0], node[1])
    return None


def _extract_prop_value(prop: dict | str | None):
    """Extract value from NGSI-LD Property (dict with 'value' key) or plain scalar.

    Handles:
    - Normalized Property: {"type": "Property", "value": <scalar|dict>}
    - RDF typed literal as value: {"@value": ..., "@type": ...}
    - Plain scalar (str, int, float) — returned as-is
    - Plain dict value (e.g. weatherStats blob) — returned as-is
    """
    if prop is None:
        return None
    if isinstance(prop, dict):
        # Direct RDF typed literal (keyValues form): {"@value": ..., "@type": ...}
        if "@value" in prop:
            return prop["@value"]
        val = prop.get("value")
        if isinstance(val, dict):
            # RDF typed literal e.g. {"@value": "2026-01-01", "@type": "xsd:date"}
            if "@value" in val:
                return val["@value"]
            # Plain object value (e.g. weatherStats blob) — return as-is
            return val
        return val
    return str(prop)


def _resolve_relationship(entity: dict, rel_name: str) -> str | None:
    """Extract object URI from an NGSI-LD Relationship or string."""
    rel = entity.get(rel_name)
    if isinstance(rel, dict) and rel.get("type") == "Relationship":
        return rel.get("object")
    if isinstance(rel, str):
        return rel
    return None


def _to_parcel_urn(parcel_id: str) -> str:
    """Normalize a parcel id to a full AgriParcel URN (idempotent).

    Callers arrive with mixed conventions: the full URN
    (``urn:ngsi-ld:AgriParcel:parcela-42``), a bare short id (``parcela-42``),
    or a prefixed-but-not-URN id (``AgriParcel:parcela-42``). Orion-LD needs
    the full URN, so normalize here at the DAO boundary. Empty/None pass through.
    """
    if not parcel_id:
        return parcel_id
    if parcel_id.startswith("urn:"):
        return parcel_id
    if parcel_id.startswith("AgriParcel:"):
        return f"urn:ngsi-ld:{parcel_id}"
    return f"urn:ngsi-ld:AgriParcel:{parcel_id}"
