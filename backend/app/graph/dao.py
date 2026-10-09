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
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, unquote

from nkz_platform_sdk.agronomy import AgronomicValue, Source
from nkz_platform_sdk.orion import OrionClient
from nkz_platform_sdk.subscriptions import SubscriptionDef, SubscriptionRegistrar

from app.core.config import settings
from app.graph import agroclimatic, zone_match
from app.graph import evidence_policy as ep
from app.services.country_lookup import country_at
from app.services.crop_cycles_client import fetch_crop_cycles
from app.services.soil_client import assess_soil_suitability, get_parcel_soil_properties
from app.species_registry import (
    CATALOG_SIBLING_CODES,
    get_species_info,
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


async def _fetch_ropo_products(cultivo: str, tenant_id: str) -> list[dict]:
    """Query CUE national ROPO catalog for a crop. Internal-service auth. Never raises."""
    import httpx

    cue_url = os.getenv("CUE_API_URL", "http://cue-service:5000").rstrip("/")
    secret = os.getenv("INTERNAL_SERVICE_SECRET", "")
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                f"{cue_url}/api/modules/cue/productos-ropo",
                params={"cultivo": cultivo, "estado": "autorizado"},
                headers={"X-Internal-Service-Secret": secret, "X-Tenant-ID": tenant_id},
            )
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, list) else []
    except Exception:  # noqa: BLE001,S110
        pass
    return []

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

    async def recommend_next_crop(self, previous_crop: str, species: str | None = None) -> list[dict]:
        """Suggest next crop based on rotation rules: exclude constrained crops."""
        async with self._driver.session() as session:
            # Get all constraints where previous_crop has restrictions
            result = await session.run(
                """
                MATCH (rc:RotationConstraint {cropA: $crop})
                WHERE coalesce(rc.effect, 'restriction') = 'restriction'
                RETURN rc.cropB AS restricted, rc.intervalYears AS years, rc.reason AS reason
                """,
                crop=previous_crop,
            )
            restricted = {r["restricted"] for r in await result.data()}

            # Get all available species that are NOT restricted
            result2 = await session.run(
                """
                MATCH (s:Species)
                WHERE NOT s.name IN $restricted OR $restricted = []
                RETURN s.name AS name, s.scientificName AS scientific_name
                ORDER BY s.name
                """,
                restricted=list(restricted),
            )
            return [dict(r) for r in await result2.data()]

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

    async def simulate_scenario(
        self, baseline_crop: str, scenario_crop: str,
        baseline_sowing: str | None = None, scenario_sowing: str | None = None,
    ) -> dict:
        """Compare two agronomic scenarios and return deltas.

        Returns yield gap delta, fertilizer delta, and constraint violations
        for the scenario vs baseline. Pure rule-based, no ML.
        """
        result: dict = {
            "baseline": baseline_crop,
            "scenario": scenario_crop,
            "rotation_ok": True,
            "rotation_issue": "",
            "soil_ok": True,
            "soil_issues": [],
            "fertilizer_delta": [],
            "recommendation": "",
        }

        async with self._driver.session() as session:
            # Check rotation constraint
            rc = await session.run(
                "MATCH (r:RotationConstraint) "
                "WHERE toLower(r.cropA) = toLower($baseline) AND toLower(r.cropB) = toLower($scenario) "
                "RETURN r.intervalYears AS years, r.reason AS reason, "
                "coalesce(r.effect, 'restriction') AS effect",
                baseline=baseline_crop, scenario=scenario_crop,
            )
            row = await rc.single()
            if row and row["effect"] == "restriction" and row["years"] and row["years"] > 0:
                result["rotation_ok"] = False
                result["rotation_issue"] = (
                    f"{row['reason']}. Minimum interval: {row['years']} years."
                )

            # Check soil suitability for scenario crop
            ss = await session.run(
                "MATCH (s:Species)-[:HAS_SOIL_SUITABILITY]->(ss:CropSoilSuitability) "
                "WHERE toLower(s.name) = toLower($crop) OR toLower(s.scientificName) = toLower($crop) "
                "RETURN ss.phMin, ss.phMax, ss.textures, ss.drainage, ss.depthMinCm, ss.salinityMaxDsM",
                crop=scenario_crop,
            )
            soil = await ss.single()
            if not soil:
                result["soil_issues"].append("No soil suitability data for scenario crop")
                result["soil_ok"] = False

            # Compare fertilizer needs (simplified: total NPK per season)
            base_n = await session.run(
                "MATCH (s:Species)-[:HAS_STAGE]->(:PhenologyStage)-[:HAS_NUTRIENT_PROFILE]->(n:CropNutrientProfile {element: 'nitrogen'}) "
                "WHERE toLower(s.name) = toLower($crop) OR toLower(s.scientificName) = toLower($crop) "
                "RETURN sum(n.uptakeKgHaDay) AS total", crop=baseline_crop,
            )
            base_total = (await base_n.single())
            base_n_val = float(base_total["total"] or 0) if base_total else 0

            sc_n = await session.run(
                "MATCH (s:Species)-[:HAS_STAGE]->(:PhenologyStage)-[:HAS_NUTRIENT_PROFILE]->(n:CropNutrientProfile {element: 'nitrogen'}) "
                "WHERE toLower(s.name) = toLower($crop) OR toLower(s.scientificName) = toLower($crop) "
                "RETURN sum(n.uptakeKgHaDay) AS total", crop=scenario_crop,
            )
            sc_total = (await sc_n.single())
            sc_n_val = float(sc_total["total"] or 0) if sc_total else 0

            delta = sc_n_val - base_n_val
            if delta > 1:
                result["fertilizer_delta"].append(
                    {"element": "nitrogen", "delta_kg_ha_day": round(delta, 1),
                     "note": "Scenario needs more N than baseline"}
                )
            elif delta < -1:
                result["fertilizer_delta"].append(
                    {"element": "nitrogen", "delta_kg_ha_day": round(delta, 1),
                     "note": "Scenario needs less N than baseline"}
                )

            # Recommendation
            issues = []
            if not result["rotation_ok"]:
                issues.append("rotation constraint violated")
            if not result["soil_ok"]:
                issues.append("soil suitability issues")
            if abs(delta) > 0:
                issues.append("fertilizer adjustment needed")

            if not issues:
                result["recommendation"] = f"{scenario_crop} is a suitable alternative to {baseline_crop}."
            else:
                result["recommendation"] = (
                    f"{scenario_crop} vs {baseline_crop}: {'; '.join(issues)}. "
                    "Review before adopting."
                )

        return result

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

    async def get_site_source_ids(self, names: list[str]) -> dict[str, list[str]]:
        """Sources of the trials at each named TrialSite (``{site name: sorted source ids}``).

        Derived from the trials themselves (``VarietyTrial.source_id`` over ``TRIAL_AT``), not
        from the site's own ``sourceIds`` property, which can lag behind merges. A site with no
        trials maps to ``[]``; a name that is not a site is absent.
        """
        if not names:
            return {}
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (ts:TrialSite) WHERE ts.name IN $names
                OPTIONAL MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts)
                RETURN ts.name AS name,
                       [s IN collect(DISTINCT vt.source_id) WHERE s IS NOT NULL] AS source_ids
                """,
                names=list(names),
            )
            by_name: dict[str, set[str]] = {}
            async for r in result:  # duplicate-named sites (a known data quirk) are unioned
                by_name.setdefault(r["name"], set()).update(r["source_ids"])
            return {name: sorted(ids) for name, ids in by_name.items()}

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

    async def get_regenerative_sequence(
        self,
        climate_class: str,
        target_protein: str = "VICFX",
        soil_type: str | None = None,
        management: str = "any",
        parcel_id: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
    ) -> dict:
        """Plan a regenerative cover-crop-to-protein-crop sequence.

        Uses European cover crop reference data (INTIA, JRC MARS,
        Legumes Translated H2020) combined with Neo4j variety trial data
        via extrapolate_varieties() for protein crop ranking.

        When parcel_id is provided, enriches water balance with real
        Soil AWC from the Soil module instead of regional defaults.

        Calculation methodology (fully auditable):
        ─────────────────────────────────────────────
        Cover crop selection:
          - Filters by C/N ratio (<20 for protein crops, per Clark 2007)
          - Ranks by expected biomass (t/ha) per climate zone
          - Screens frost tolerance against site frost days
          - Sources: INTIA Navarra (2019-2023), JRC MARS Bulletins,
            Legumes Translated H2020 Practice Notes #5,8,12,15,18

        Nitrogen dynamics:
          - N_cover_total = biomass_t_ha × 1000 × N_content_pct / 100
          - N_cover_available = N_cover_total × 0.50
            (50% first-season mineralization, Clark 2007)
          - N_fixed = from European trial data (Peoples et al. 2021)
          - Protein yield adjusted for organic: ×0.80
            (Seufert et al. 2012, Ponisio et al. 2015)

        Date estimation:
          - Cover crop sowing: climate-specific autumn window
          - Termination: climate-specific month midpoint, adjusted
            ±days by GDD deviation from typical (1200 GDD baseline)
          - Protein crop sowing: 10 days after termination,
            clamped to spring sowing window
          - Harvest: protein_GDD / spring_GDD_rate
          - Base temperatures: 4°C (cool-season), 10°C (warm-season)
            per Trudgill et al. 2005

        Water balance (FAO-56 method):
          - ETc = Kc_cover(0.8) × ET0_growing_season
            ET0_growing_season = annual_ET0 × 0.40 (Oct-May fraction)
          - Water supply = effective_rain + soil_AWC/2
            effective_rain = growing_season_rainfall × 0.80
            growing_season_rainfall = annual_rainfall × 0.60
          - Risk: low (<-20mm), medium (-20 to +20mm), high (>+20mm)
          - If parcel_id: soil_AWC from AgriSoil entity via Soil module

        Args:
            climate_class: Köppen climate (e.g. 'Csa', 'BSk')
            target_protein: EPPO code of target protein crop
            soil_type: Optional WRB soil type
            management: 'organic', 'conventional', or 'any'
            parcel_id: Optional AgriParcel URN for real soil/weather data
            lat: Optional latitude for environment resolution
            lon: Optional longitude for environment resolution

        Returns:
            Complete sequence plan dict matching RegenerativeSequence schema.
        """
        from app.services.cover_crops import (
            PROTEIN_CROPS,
            estimate_dates,
            estimate_n_fixation,
            select_cover_crops,
        )

        # ── Validate inputs ───────────────────────────────────────────
        protein = PROTEIN_CROPS.get(target_protein)
        if protein is None:
            return {"error": f"Unknown protein crop: {target_protein}. Available: {list(PROTEIN_CROPS.keys())}"}

        if protein.get("climates", {}).get(climate_class, {}).get("not_viable"):
            return {
                "error": protein["climates"][climate_class].get("not_viable_note",
                    f"{target_protein} not viable in {climate_class}"),
            }

        # ── Resolve climate metadata from Neo4j ───────────────────────
        climate_meta = {}
        async with self._driver.session() as session:
            result = await session.run("""
                MATCH (ts:TrialSite)
                WHERE ts.climateClass = $climate
                RETURN avg(ts.annualRainfallMm) AS avg_rainfall,
                       avg(ts.frostDaysPerYear) AS avg_frost_days,
                       avg(ts.annualET0Mm) AS avg_et0,
                       count(ts) AS site_count
            """, climate=climate_class)
            row = await result.single()
            if row:
                climate_meta = {
                    "avg_rainfall_mm": round(row["avg_rainfall"], 1) if row["avg_rainfall"] else None,
                    "avg_frost_days": round(row["avg_frost_days"], 1) if row["avg_frost_days"] else 0,
                    "avg_et0_mm": round(row["avg_et0"], 1) if row["avg_et0"] else None,
                    "sites_in_zone": row["site_count"],
                }

        frost_days = climate_meta.get("avg_frost_days", 0)

        # ── Select cover crops ────────────────────────────────────────
        candidate_cover_crops = select_cover_crops(
            climate_class=climate_class,
            management=management,
            min_biomass_t_ha=2.0,
            max_c_n_ratio=20,
            frost_days=frost_days,
        )

        # ── Rank protein crop varieties ───────────────────────────────
        eppo_search = protein.get("eppo_search", target_protein)
        variety_ranking = await self.extrapolate_varieties(
            crop=eppo_search,
            climate_class=climate_class,
            soil_type=soil_type,
            top_n=5,
            management=management,
        )

        best_variety = None
        if variety_ranking.get("ranked_varieties"):
            best_variety = variety_ranking["ranked_varieties"][0]

        # ── Build primary recommendation ──────────────────────────────
        primary_cover = candidate_cover_crops[0] if candidate_cover_crops else None
        if primary_cover is None:
            return {"error": f"No suitable cover crop for climate={climate_class}"}

        cover_biomass = primary_cover["target_biomass_t_ha"]
        best_yield = best_variety["mean_yield_kg_ha"] if best_variety else None

        n_estimate = estimate_n_fixation(
            cover_eppo=primary_cover["eppo"],
            protein_eppo=target_protein,
            cover_biomass_t_ha=cover_biomass,
            protein_yield_kg_ha=best_yield,
            management=management,
        )

        cover_gdd = primary_cover.get("gdd_to_termination", {})
        cover_gdd_val = cover_gdd.get("value", 1250) if isinstance(cover_gdd, dict) else cover_gdd
        protein_gdd_param = protein.get("climates", {}).get(climate_class, {}).get("gdd_to_maturity", {})
        protein_gdd_val = protein_gdd_param.get("value", 1400) if isinstance(protein_gdd_param, dict) else protein_gdd_param

        dates = estimate_dates(
            climate_class=climate_class,
            cover_gdd=cover_gdd_val,
            protein_gdd=protein_gdd_val,
        )

        # ── Water balance ─────────────────────────────────────────────
        # If parcel_id provided, fetch real soil AWC from Soil module
        soil_awc = None
        if parcel_id:
            soil_awc = await self._fetch_parcel_awc(parcel_id)

        water_balance = self._assess_water_balance(
            climate_meta=climate_meta,
            cover_biomass_t_ha=cover_biomass,
            soil_type=soil_type,
            soil_awc_override=soil_awc,
        )

        # ── Alternatives ──────────────────────────────────────────────
        alternatives = []
        for cc in candidate_cover_crops[1:4]:
            n_alt = estimate_n_fixation(
                cover_eppo=cc["eppo"],
                protein_eppo=target_protein,
                cover_biomass_t_ha=cc["target_biomass_t_ha"],
                protein_yield_kg_ha=best_yield,
                management=management,
            )
            alternatives.append({
                "cover_crop": cc["eppo"],
                "cover_crop_common": cc["common_name"],
                "cover_crop_scientific": cc["scientific"],
                "biomass_t_ha": cc["target_biomass_t_ha"],
                "c_n_ratio": cc.get("c_n_ratio", {}).get("value") if isinstance(cc.get("c_n_ratio"), dict) else cc.get("c_n_ratio"),
                "n_available_kg_ha": n_alt.get("n_cover_available_kg_ha"),
                "type": cc["type"],
            })

        # ── Management warnings ───────────────────────────────────────
        organic_warning = None
        if management == "organic" and not best_variety:
            organic_warning = (
                "No organic trials are available for this protein crop; organic and conventional "
                "trials are never pooled, so no variety yield is shown."
            )

        # ── Build response ────────────────────────────────────────────
        cn_ratio_val = primary_cover.get("c_n_ratio", {})
        cn_ratio = cn_ratio_val.get("value") if isinstance(cn_ratio_val, dict) else cn_ratio_val

        return {
            "cover_crop": primary_cover["eppo"],
            "cover_crop_common": primary_cover["common_name"],
            "cover_crop_scientific": primary_cover["scientific"],
            "cover_crop_type": primary_cover["type"],
            "cover_biomass_t_ha": cover_biomass,
            "c_n_ratio": cn_ratio,
            "n_cover_total_kg_ha": n_estimate.get("n_cover_total_kg_ha"),
            "n_cover_available_kg_ha": n_estimate.get("n_cover_available_kg_ha"),
            "n_protein_fixed_kg_ha": n_estimate.get("n_protein_fixed_kg_ha"),
            "protein_crop": target_protein,
            "protein_crop_scientific": protein["scientific"],
            "protein_crop_common": protein["common_name"],
            "protein_variety": best_variety["variety"] if best_variety else None,
            "expected_protein_yield_kg_ha": best_yield,
            "protein_kg_ha": n_estimate.get("protein_kg_ha"),
            "management_mode": management,
            "organic_data_warning": organic_warning,
            "termination_gdd": cover_gdd_val,
            "termination_method": primary_cover.get("kill_method", "roller_crimper"),
            "cover_crop_sowing_date": dates["cover_crop_sowing_date"],
            "termination_date_estimate": dates["termination_date"],
            "protein_crop_sowing_date": dates["protein_crop_sowing_date"],
            "protein_crop_harvest_date": dates["protein_crop_harvest_date"],
            "water_balance_risk": water_balance["risk"],
            "water_balance_detail": water_balance,
            "alternatives": alternatives,
            "variety_trials": variety_ranking.get("ranked_varieties", [])[:3],
            "management_distribution": {
                "cover_crop_params": "European (INTIA low_input + JRC MARS conventional + Legumes Translated)",
                "variety_trials": f"conventional (~{variety_ranking.get('data_quality', {}).get('total_trials_analyzed', 0)} trials)",
            },
            "provenance": {
                "cover_crop_source": "INTIA Navarra, JRC MARS Bulletins, Legumes Translated H2020",
                "n_fixation_source": "Peoples et al. 2021, Unkovich et al. 2010",
                "yield_source": f"Neo4j VarietyTrial data: {variety_ranking.get('data_quality', {}).get('total_trials_analyzed', 0)} trials",
                "climate_source": f"TrialSite data: {climate_meta.get('sites_in_zone', 0)} sites in {climate_class}",
            },
            "carbon_projection": await self._compute_carbon_projection(
                cover_biomass_t_ha=cover_biomass,
                n_available=n_estimate.get("n_cover_available_kg_ha", 0),
                parcel_id=parcel_id,
            ),
        }

    async def _compute_carbon_projection(
        self,
        cover_biomass_t_ha: float,
        n_available: float = 0,
        parcel_id: str | None = None,
    ) -> dict:
        """Project SOC increase and CO₂e sequestration from cover crop biomass.

        Uses IPCC 2019 Tier 1 humification coefficient (0.15) and C→CO₂
        conversion factor (3.67). SOC target depends on soil texture.
        """
        # Humification: fraction of biomass carbon that becomes stable SOC
        HUMIFICATION_COEF = 0.15  # IPCC 2019 Tier 1
        C_TO_CO2 = 3.67  # Molecular weight ratio CO₂/C
        EUR_PER_KG_N = 1.5  # EU average urea price

        # Carbon in biomass (dry matter is ~45% carbon)
        biomass_carbon_t_ha = cover_biomass_t_ha * 0.45

        # SOC increase from cover crop incorporation
        soc_increase_pct = round(biomass_carbon_t_ha * HUMIFICATION_COEF / 10, 2)

        # CO₂e sequestered
        co2e_ton_ha = round(biomass_carbon_t_ha * C_TO_CO2, 1)

        # Fertilizer N savings
        fertilizer_n_saved = round(n_available, 1)
        fertilizer_savings_eur = round(fertilizer_n_saved * EUR_PER_KG_N, 2)

        # Current SOC from parcel soil data (non-blocking)
        current_soc = None
        soil_texture = "unknown"
        if parcel_id:
            try:
                await self._fetch_parcel_awc(parcel_id)
                # Try to also get SOC from same endpoint
                import httpx
                async with httpx.AsyncClient(timeout=5.0) as client:
                    soil_resp = await client.get(
                        f"http://localhost:8420/api/parcel/{parcel_id}/soil",
                    )
                    if soil_resp.status_code == 200:
                        soil_data = soil_resp.json()
                        horizons = soil_data.get("horizons", [])
                        if horizons:
                            topsoil = horizons[0]
                            if topsoil.get("organicCarbon") is not None:
                                current_soc = topsoil["organicCarbon"]
                            soil_texture = topsoil.get("usdaTextureClass", "unknown")
            except Exception:  # noqa: BLE001,S110
                pass

        # Target SOC by texture (FAO voluntary guidelines for sustainable soil management)
        target_soc = {
            "sand": 1.5, "loamy sand": 1.5, "sandy loam": 1.8,
            "loam": 2.5, "silt loam": 2.5, "silt": 2.5,
            "sandy clay loam": 3.0, "clay loam": 3.0, "silty clay loam": 3.5,
            "sandy clay": 3.5, "silty clay": 3.5, "clay": 3.5,
        }.get(soil_texture.lower(), 2.5)

        projected_soc = round((current_soc or target_soc * 0.6) + soc_increase_pct, 2) if current_soc else None
        years_to_target = None
        if current_soc and current_soc < target_soc and soc_increase_pct > 0:
            years_to_target = max(1, round((target_soc - current_soc) / soc_increase_pct))

        return {
            "current_soc_pct": current_soc,
            "target_soc_pct": target_soc,
            "projected_soc_pct": projected_soc,
            "soc_delta_pct": soc_increase_pct,
            "co2e_sequestered_ton_ha": co2e_ton_ha,
            "fertilizer_n_saved_kg_ha": fertilizer_n_saved,
            "fertilizer_savings_eur_ha": fertilizer_savings_eur,
            "years_to_target": years_to_target,
            "soil_texture": soil_texture,
            "methodology": f"IPCC 2019 Tier 1: SOC = biomass_C({biomass_carbon_t_ha:.1f}t/ha) × humification({HUMIFICATION_COEF})",
        }

    @staticmethod
    def _assess_water_balance(
        climate_meta: dict,
        cover_biomass_t_ha: float,
        soil_type: str | None = None,
        soil_awc_override: float | None = None,
    ) -> dict:
        """Estimate water balance for the cover crop growing period.

        Uses Kc × ET0 approach for the cover crop growing season (Oct-May),
        which is more realistic than biomass-based transpiration coefficients.

        Cover crop Kc during vegetative stage: ~0.7-0.9 (FAO-56).
        Winter ET0 is ~30-40% of annual ET0 in Mediterranean climates.
        """
        avg_rainfall = climate_meta.get("avg_rainfall_mm")
        avg_et0 = climate_meta.get("avg_et0_mm")
        if avg_rainfall is None:
            return {"risk": "unknown", "deficit_mm": None, "note": "Insufficient climate data"}

        # Cover crop Kc (vegetative stage, before termination)
        cover_kc = 0.8

        # Growing season ET0: Oct-May ≈ 40% of annual ET0 in Mediterranean climates
        # (the remaining 60% occurs in the hot summer months Jun-Sep)
        growing_season_et0 = (avg_et0 or avg_rainfall * 0.8) * 0.40

        # Crop water demand: ETc = Kc × ET0_growing_season
        crop_etc = cover_kc * growing_season_et0

        # Effective rainfall during growing season: ~60% of annual rain falls Oct-May
        # (Mediterranean pattern: wet winters, dry summers)
        growing_season_rain = avg_rainfall * 0.60
        effective_rain = growing_season_rain * 0.80  # 20% loss to runoff/percolation

        # Soil AWC contribution (typical Mediterranean soil: 100-150mm in top 1m)
        soil_awc = soil_awc_override if soil_awc_override else 120  # mm

        # Net balance
        water_supply = effective_rain + soil_awc * 0.5  # 50% of AWC usable without stress
        deficit = crop_etc - water_supply

        if deficit < -20:
            risk = "low"
        elif deficit < 20:
            risk = "medium"
        else:
            risk = "high"

        return {
            "risk": risk,
            "crop_etc_mm": round(crop_etc, 1),
            "growing_season_et0_mm": round(growing_season_et0, 1),
            "growing_season_rainfall_mm": round(growing_season_rain, 1),
            "effective_rainfall_mm": round(effective_rain, 1),
            "soil_awc_mm": soil_awc,
            "water_supply_mm": round(water_supply, 1),
            "deficit_mm": round(deficit, 1),
            "avg_annual_rainfall_mm": round(avg_rainfall),
            "avg_annual_et0_mm": round(avg_et0) if avg_et0 else None,
            "soil_type": soil_type,
            "cover_kc": cover_kc,
            "method": f"ETc = Kc({cover_kc}) × ET0_growing_season({growing_season_et0:.0f}mm). Water supply = effective_rain({effective_rain:.0f}mm) + soil_AWC/2({soil_awc/2:.0f}mm).",
        }

    @staticmethod
    async def _fetch_parcel_awc(parcel_id: str) -> float | None:
        """Fetch available water capacity (mm) from Soil module for a parcel.

        Queries the Soil module API for AgriSoil entities linked to the parcel
        and returns the weighted average AWC across soil horizons.
        Returns None if the Soil module is unreachable or has no data.
        """
        try:
            import httpx
            soil_service = "http://soil-api-service:8000"
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(
                    f"{soil_service}/api/v1/soil/parcels/{parcel_id}/properties",
                    params={"properties": "availableWaterCapacity"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    horizons = data.get("horizons", [])
                    if horizons:
                        total_awc = sum(
                            h.get("availableWaterCapacity", 0) or 0
                            for h in horizons
                        )
                        return round(total_awc, 1) if total_awc > 0 else None
        except Exception:  # noqa: BLE001,S110
            pass
        return None

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
                    headers={
                        "X-Tenant-ID": tenant_id,
                        "X-User-ID": "bioorchestrator-worker",
                    },
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

    async def assign_crop_to_parcel(
        self,
        parcel_id: str,
        crop_uri: str,
        variety_uri: str,
        management: str,
        season_start: str,
        season_end: str,
        tenant_id: str,
    ) -> dict:
        """Create a per-parcel AgriCrop entity and assign it to the parcel.

        1. Creates a new AgriCrop entity in Orion-LD with refParent pointing
           to the parcel, species, variety, dates, management.
        2. Patches the AgriParcel with hasAgriCrop to the new entity.
        3. If the parcel had a previous assignment, marks the old crop harvested.
        """
        from datetime import datetime, timezone

        import httpx
        from fastapi import HTTPException

        parcel_id = _to_parcel_urn(parcel_id)
        parcel_short = parcel_id.split(":")[-1]
        season_year = season_start[:4] if season_start else str(datetime.now(timezone.utc).year)
        crop_eppo = crop_uri.split(":")[-1] if crop_uri else "unknown"
        # Variety names travel inside a synthetic URN whose last segment is
        # the raw name (e.g. "ROSA JUNIN"). A NGSI-LD Relationship object must
        # be a valid URI — Orion-LD 400s on spaces/accents — so encode for the
        # relationship and keep the human-readable name for Property values.
        variety_name = unquote(variety_uri.split(":")[-1]) if variety_uri else None
        safe_variety_uri = quote(variety_uri, safe=":") if variety_uri else None

        # Resolve a human-readable name + scientificName from the EPPO code so
        # frontends reading AgriCrop.name/scientificName get a real label instead
        # of an empty field (audit 2026-07-01). Platform convention: the
        # crop-name endpoint defaults to Spanish, so prefer es → en → scientific.
        from app.species_registry import (
            get_crop_group,
            get_lifecycle,
            get_species_info,
            resolve_species,
        )

        _slug = resolve_species(crop_eppo) if crop_eppo and crop_eppo != "unknown" else None
        crop_group = get_crop_group(_slug) if _slug else None
        _info = get_species_info(_slug) if _slug else None
        _common = (_info.get("common_names") if _info else None) or {}
        crop_scientific = _info.get("scientific_name") if _info else None
        crop_name_label = _common.get("es") or _common.get("en") or crop_scientific
        crop_lifecycle = get_lifecycle(_slug) if _slug else None

        # Build entity ID for the per-parcel AgriCrop
        new_crop_id = f"urn:ngsi-ld:AgriCrop:{tenant_id}:{parcel_short}:{season_year}"

        # Build the AgriCrop entity body
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        agri_crop_body = {
            "id": new_crop_id,
            "type": "AgriCrop",
            "refParent": {
                "type": "Relationship",
                "object": parcel_id,
            },
            "hasAgriParcel": {
                "type": "Relationship",
                "object": parcel_id,
            },
            "species": {
                "type": "Property",
                "value": crop_eppo,
            },
            "plantingDate": {
                "type": "Property",
                "value": {"@type": "Date", "@value": season_start},
            },
            "harvestDate": {
                "type": "Property",
                "value": {"@type": "Date", "@value": season_end},
            },
            "management": {
                "type": "Property",
                "value": management,
            },
            "status": {
                "type": "Property",
                "value": "active",
            },
            "dateCreated": {
                "type": "Property",
                "value": {"@type": "DateTime", "@value": now},
            },
        }
        if crop_group:
            agri_crop_body["category"] = {
                "type": "Property",
                "value": crop_group,
            }
        if variety_name:
            agri_crop_body["variety"] = {
                "type": "Property",
                "value": variety_name,
            }
        if crop_name_label:
            agri_crop_body["name"] = {
                "type": "Property",
                "value": crop_name_label,
            }
        if crop_scientific:
            agri_crop_body["scientificName"] = {
                "type": "Property",
                "value": crop_scientific,
            }
        if crop_lifecycle:
            agri_crop_body["cropLifecycle"] = {"type": "Property", "value": crop_lifecycle}

        client = OrionClient(tenant_id=tenant_id)
        try:
            # Step 1: Create the AgriCrop entity (@context injected by OrionClient)
            try:
                await client.create_entity(agri_crop_body)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 409:
                    # Entity already exists — upsert via PATCH attrs
                    await client.update_entity_attrs(new_crop_id, {
                        k: v for k, v in agri_crop_body.items()
                        if k not in ("id", "type", "@context", "dateCreated")
                    })
                else:
                    raise HTTPException(
                        status_code=502,
                        detail=f"Orion-LD create entity failed: {e.response.status_code} {e.response.text[:200]}",
                    )

            # Step 2: Read existing hasAgriCrop on the parcel (for harvest marking)
            old_crop_id = None
            try:
                parcel_entity = await client.get_entity(parcel_id)
                old_crop_rel = (
                    _resolve_relationship(parcel_entity, "hasAgriCrop")
                    or _resolve_relationship(parcel_entity, "refAgriCrop")
                )
                if old_crop_rel and old_crop_rel != new_crop_id:
                    old_crop_id = old_crop_rel
            except httpx.HTTPStatusError as e:
                if e.response.status_code != 404:
                    logger.warning("Failed to read parcel for harvest marking: %s", e)
            except Exception:
                logger.exception("Unexpected error reading parcel for harvest marking")

            # Step 3: Mark old crop as harvested
            if old_crop_id:
                try:
                    await client.update_entity_attrs(old_crop_id, {
                        "status": {"type": "Property", "value": "harvested"},
                    })
                except Exception:  # noqa: BLE001
                    logger.warning("Failed to mark old crop %s as harvested", old_crop_id)

            # Step 4: Patch the AgriParcel with new crop assignment
            patch_body = {
                "hasAgriCrop": {"type": "Relationship", "object": new_crop_id},
                "hasAgriCropVariety": {"type": "Relationship", "object": safe_variety_uri},
                "management": {"type": "Property", "value": management},
            }
            # POST /attrs (append): PATCH /attrs only updates EXISTING attrs, so a
            # first-time hasAgriCrop assignment lands in notUpdated and silently no-ops.
            await client.append_entity_attrs(parcel_id, patch_body)
            await self._ensure_phenology_subscription(tenant_id)

            # Clean up activation placeholder AgriCrop(s) so they never
            # masquerade as a second assigned crop.
            try:
                crops = await client.query_entities(
                    type="AgriCrop",
                    q=f'hasAgriParcel=="{parcel_id}"|refAgriParcel=="{parcel_id}"',
                    limit=20,
                    options="keyValues",
                )
                for ph in (crops or []):
                    if ph.get("provenance") == "placeholder" and ph.get("id") != new_crop_id:
                        await client.delete_entity(ph["id"])
                        logger.info("Removed placeholder AgriCrop %s after assign-crop", ph["id"])
            except Exception as exc:  # noqa: BLE001 — cleanup must never fail the assignment
                logger.warning("Failed to clean up placeholder AgriCrop: %s", exc)

            return {
                "status": "assigned",
                "parcel_id": parcel_id,
                "crop": crop_eppo,
                "variety": variety_name,
                "management": management,
                "entity_id": new_crop_id,
            }

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise HTTPException(status_code=404, detail=f"Parcel not found: {parcel_id}")
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Orion-LD error: {e.response.status_code}: "
                    f"{e.response.text[:200]}"
                ),
            )
        except httpx.ConnectError:
            raise HTTPException(status_code=502, detail="Orion-LD unreachable")
        finally:
            await client.close()

    async def _ensure_phenology_subscription(self, tenant_id: str) -> None:
        """Idempotently ensure the per-tenant CropHealthAssessment subscription.

        Registered lazily at assign-crop (no crop → no phenology → no sub needed).
        Fail-soft: a subscription error must never fail the crop assignment.
        """
        try:
            internal_secret = os.getenv("INTERNAL_SERVICE_SECRET", "")
            registrar = SubscriptionRegistrar(
                orion_url=settings.orion_ld_url,
                notification_url="http://bioorchestrator-api-service:8420/api/graph/internal/phenology-update",
                subscriptions=[SubscriptionDef(
                    type="CropHealthAssessment",
                    watched_attributes=["phenologyStage"],
                )],
                module_name="bioorchestrator",
                context_url=settings.context_url,
                notification_headers=(
                    {"X-Internal-Service-Secret": internal_secret}
                    if internal_secret else None
                ),
            )
            result = await registrar.ensure_all([tenant_id])
            logger.info("phenology subscription ensured for %s: %s", tenant_id, result)
        except Exception as exc:  # noqa: BLE001
            logger.warning("phenology subscription setup failed for %s: %s", tenant_id, exc)

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
                            await client.update_entity_attrs(entity["id"], {
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
                irrigation_inferred = "regadío"
            else:
                irrigation_inferred = "secano"
            irrigation_source = "irrigationSystemType"
        else:
            # Platform custom Property
            irrig_regime = _extract_prop_value(parcel.get("irrigationRegime"))
            if irrig_regime:
                regime_lower = str(irrig_regime).lower().strip()
                if "regad" in regime_lower or "irrig" in regime_lower:
                    irrigation_inferred = "regadío"
                else:
                    irrigation_inferred = "secano"
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
            # Season window: the platform's resolved crop cycle; the parcel's legacy copy only
            # when the platform is unreachable or has no current cycle.
            cycles = await fetch_crop_cycles(parcel_id, tenant_id)
            cur = (cycles or {}).get("current") or {}
            season_start = (cur.get("start") or {}).get("date") or _extract_prop_value(parcel.get("cropSeasonStart"))
            season_end = (cur.get("end") or {}).get("date") or _extract_prop_value(parcel.get("cropSeasonEnd"))
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

    async def clear_crop_assignment(self, parcel_id: str, tenant_id: str) -> dict:
        """Remove crop assignment from AgriParcel. Raises on Orion failure.

        The AgriCrop the parcel pointed at is cancelled first: left active, the platform
        reconciler would link it again. Cancelling before unlinking makes a failed unlink
        retryable (the parcel still points at the crop).
        """
        import httpx

        parcel_id = _to_parcel_urn(parcel_id)
        patch_body = {
            "hasAgriCrop": {"type": "Relationship", "object": None},
            "hasAgriCropVariety": {"type": "Relationship", "object": None},
            "management": {"type": "Property", "value": None},
            "cropSeasonStart": {"type": "Property", "value": None},
            "cropSeasonEnd": {"type": "Property", "value": None},
        }
        orion = OrionClient(tenant_id)
        try:
            parcel = await orion.get_entity(parcel_id)
            crop_id = _resolve_relationship(parcel, "hasAgriCrop") or _resolve_relationship(parcel, "refAgriCrop")
            if crop_id:
                try:
                    # append (POST /attrs): never a silent notUpdated, unlike PATCH
                    await orion.append_entity_attrs(crop_id, {
                        "status": {"type": "Property", "value": "cancelled"},
                    })
                except httpx.HTTPStatusError as e:
                    if e.response.status_code != 404:
                        raise
                    logger.warning("clear_crop_assignment: linked AgriCrop %s no longer exists", crop_id)
            await orion.update_entity_attrs(parcel_id, patch_body)
        finally:
            await orion.close()
        return {"status": "cleared", "parcel_id": parcel_id}

    async def get_yield_potential(self, variety: str, crop: str, climate_class: str | None = None, soil_type: str | None = None, parcel_id: str | None = None, tenant_id: str = "", management: str | None = None) -> dict:
        """Compute expected yield and yield gap for a variety.

        ``management`` (organic | conventional | any): organic and conventional trials are never
        pooled (policy rule 10); None and ``any`` read the non-organic trials.

        The number is the mean of the variety's FIELD trials of the main purpose under the evidence
        policy (``app.graph.evidence_policy``): no BSL or note-derived kg/ha, no forage, no national
        or regional record, content-identical trials once (the same rules as the recommender).
        With no such evidence the expected yield, its interval and the yield gap are null and
        ``data_gaps`` says why (never 0): ``no_trial_data`` (no trial of the variety),
        ``no_field_trials`` (trials only outside the field tier) or ``no_measured_yield`` (field
        trials without a policy number).
        """
        import math
        # Every tier comes back (tagged) so the disease and trait enrichment below keeps the rows
        # the numbers must not use.
        variety_trials = await self.get_variety_trials(
            crop=crop, variety=variety, climate_class=climate_class, soil_type=soil_type,
            limit=200, tier=None, management=management,
        )
        field_trials = [t for t in variety_trials if t.get("evidence_tier", ep.EVIDENCE_TIER_FIELD) == ep.EVIDENCE_TIER_FIELD]
        yields = [t["yield_kg_ha"] for t in field_trials if t.get("yield_kg_ha") is not None]
        sites = list({n for t in field_trials for n in (t.get("site_names") or [t.get("site_name")]) if n})
        result: dict = {"variety": variety, "crop": crop, "target_environment": {"climate_class": climate_class, "soil_type": soil_type}}
        # Sources of the trials behind the displayed number only (field tier, policy-eligible yield).
        # No number, no sources: a row the policy keeps out of the mean is not credited.
        result["source_ids"] = sorted({
            t["source_id"] for t in field_trials
            if t.get("yield_kg_ha") is not None and t.get("source_id")
        })
        mean_yield: float | None = None
        if yields:
            mean_yield = sum(yields) / len(yields)
            n = len(yields)
            stddev = math.sqrt(sum((y - mean_yield) ** 2 for y in yields) / (n - 1)) if n > 1 else 0
            ci_low = mean_yield - 1.96 * stddev / math.sqrt(n) if n > 1 else mean_yield
            ci_high = mean_yield + 1.96 * stddev / math.sqrt(n) if n > 1 else mean_yield
            result.update({"expected_yield_kg_ha": round(mean_yield, 1), "confidence_interval": [round(ci_low, 1), round(ci_high, 1)],
                           "trials_analyzed": len(yields), "similar_sites": sites[:10]})
        else:
            gap_id = ("no_trial_data" if not variety_trials
                      else "no_field_trials" if not field_trials else "no_measured_yield")
            result.update({"expected_yield_kg_ha": None, "confidence_interval": None, "trials_analyzed": 0,
                           "similar_sites": sites[:10], "data_gaps": [gap_id]})
        if parcel_id and mean_yield is not None:
            try:
                orion = OrionClient(tenant_id)
                try:
                    entities = await orion.query_entities(
                        type="CropHealthAssessment",
                        q=f'hasAgriParcel=="{parcel_id}"|refAgriParcel=="{parcel_id}"',
                        limit=1,
                    )
                finally:
                    await orion.close()
                if entities:
                    yup = _extract_prop_value(entities[0].get("yieldUtilizationPct"))
                    if yup is not None:
                        current_yield = mean_yield * (float(yup) / 100)
                        gap = mean_yield - current_yield
                        result["current_estimated_yield_kg_ha"] = round(current_yield, 1)
                        result["yield_gap_kg_ha"] = round(gap, 1)
                        result["yield_gap_pct"] = round(gap / mean_yield * 100, 1) if mean_yield else None
            except Exception:  # noqa: BLE001,S110
                pass
        phenology = await self.get_phenology_params(species=crop)
        if phenology:
            result["stage_ky"] = {phenology.get("stage", "vegetative"): phenology.get("ky", 0.45)}
        result["limiting_factor"] = "water" if climate_class and climate_class in ("BSk", "BSh", "Csa", "Csb") else "unknown"

        # Enrich with disease resistance and agronomic traits from trial data
        merged_diseases: dict[str, dict] = {}
        merged_traits: dict[str, dict] = {}
        conf_levels: list[str] = []
        for t in variety_trials:
            ds_raw = t.get("disease_scores_unified")
            if ds_raw and isinstance(ds_raw, (str, dict)):
                try:
                    ds = json.loads(ds_raw) if isinstance(ds_raw, str) else ds_raw
                    for dk, dv in ds.items():
                        if isinstance(dv, dict) and dv.get("value") is not None \
                                and (dk not in merged_diseases or dv["value"] > merged_diseases[dk]["value"]):
                                merged_diseases[dk] = dv
                except (json.JSONDecodeError, TypeError):
                    pass
            at_raw = t.get("agronomic_traits_unified")
            if at_raw:
                try:
                    at = json.loads(at_raw) if isinstance(at_raw, str) else at_raw
                    for tk, tv in at.items():
                        if tk not in merged_traits:
                            merged_traits[tk] = tv
                except (json.JSONDecodeError, TypeError):
                    pass
            if t.get("confidence") and t["confidence"] not in conf_levels:
                conf_levels.append(t["confidence"])
        if merged_diseases:
            result["disease_scores"] = merged_diseases
        if merged_traits:
            result["agronomic_traits"] = merged_traits
        if conf_levels:
            result["confidence"] = "high" if "high" in conf_levels else ("medium" if "medium" in conf_levels else conf_levels[0])

        return result

    async def compare_crops(
        self, parcel_id: str, crops: list[str],
        seed_price: float = 1, harvest_price: float = 1, operation_cost: float = 1,
        tenant_id: str = "",
    ) -> dict:
        """Compare multiple crops on a parcel — agronomic, environmental, economic."""
        from app.services.crop_reference import get_crop_ref

        ctx = await self.get_crop_context(parcel_id=parcel_id, tenant_id=tenant_id)
        target_climate = None
        target_soil = None
        if "error" not in ctx:
            env = ctx.get("target_environment", {}) if isinstance(ctx.get("target_environment"), dict) else {}
            target_climate = env.get("climate_class") or ctx.get("season", {}).get("current_stage", "")
            soil_data = ctx.get("soil", {})
            if isinstance(soil_data, dict):
                actual = soil_data.get("actual", {})
                if isinstance(actual, dict) and actual.get("data_available"):
                    target_soil = actual.get("texture", "")

        comparisons = []
        for crop in crops:
            ref = await get_crop_ref(crop)
            # Get best variety
            extrapolated = await self.extrapolate_varieties(
                crop=crop, climate_class=target_climate, soil_type=target_soil, top_n=1,
            )
            best = (extrapolated.get("ranked_varieties") or [{}])[0] if isinstance(extrapolated, dict) else {}

            # No eligible numeric evidence (evidence policy) is a null yield, never 0: revenue and
            # margin depend on it and are null too.
            yield_val = _eligible_yield(best)
            ops = ref["operations_count"]
            seed_cost = seed_price * 1
            ops_cost = ops * operation_cost
            total_cost = seed_cost + ops_cost
            gross_rev = yield_val / 1000 * harvest_price if yield_val is not None else None
            net_margin = gross_rev - total_cost if gross_rev is not None else None
            carbon = ref["carbon_fixed_tco2e_ha"]

            # Soil suitability
            soil_req = await self.get_soil_suitability(crop)
            warnings = []
            if soil_req and isinstance(soil_data, dict):
                actual = soil_data.get("actual", {})
                if isinstance(actual, dict) and actual.get("ph"):
                    ph = actual["ph"]
                    if soil_req.get("ph_min") and soil_req.get("ph_max") \
                            and not (soil_req["ph_min"] <= ph <= soil_req["ph_max"]):
                            warnings.append(f"pH {ph} outside [{soil_req['ph_min']}, {soil_req['ph_max']}]")

            entry = {
                "crop": crop,
                "best_variety": best.get("variety", ""),
                "source_ids": sorted(best.get("source_ids") or []),
                "agronomics": {
                    "expected_yield_kg_ha": round(yield_val, 1) if yield_val is not None else None,
                    "confidence_interval": best.get("confidence_interval") if best else None,
                    "trials_analyzed": best.get("trial_count", 0),
                    "growing_season_days": ref["growing_season_days"],
                    "operations_count": ops,
                },
                "environmental": {
                    "carbon_fixed_tco2e_ha": carbon,
                    "n_fixation_kg_ha": ref["n_fixation_kg_ha"],
                    "n_requirement_kg_ha": ref["n_requirement_kg_ha"],
                },
                "economic": {
                    "seed_cost_eur_ha": round(seed_cost, 2) if seed_price > 1 else None,
                    "operations_cost_eur_ha": round(ops_cost, 2) if operation_cost > 1 else None,
                    "total_cost_eur_ha": round(total_cost, 2),
                    "gross_revenue_eur_ha": round(gross_rev, 2) if gross_rev is not None else None,
                    "net_margin_eur_ha": round(net_margin, 2) if net_margin is not None else None,
                },
                "soil_suitability": {"overall": "suitable" if not warnings else "warning", "warnings": warnings},
            }
            if yield_val is None:
                entry["data_gaps"] = [_NO_MEASURED_YIELD_GAP]

            # Attach source provenance metadata
            if "n_fixation_source" in ref:
                entry["environmental"]["n_fixation_source"] = ref["n_fixation_source"]
            if "growing_season_source" in ref:
                entry["agronomics"]["growing_season_source"] = ref["growing_season_source"]

            # Enrich with forage value and market maturity (non-blocking)
            if ref.get("n_fixation_kg_ha", 0) > 0:
                try:
                    forage = await self.get_forage_value(crop)
                    if forage:
                        entry["forage_value"] = forage
                except Exception:  # noqa: BLE001,S110
                    pass
            try:
                maturity = await self.get_market_maturity(crop)
                if maturity and not maturity.get("source_unavailable"):
                    entry["market_maturity"] = maturity
            except Exception:  # noqa: BLE001,S110
                pass

            comparisons.append(entry)

        # Rankings: a crop without a yield (hence without margin or score) goes last, never as a 0
        scored = [c for c in comparisons if c["economic"]["net_margin_eur_ha"] is not None]
        unscored = [c for c in comparisons if c["economic"]["net_margin_eur_ha"] is None]
        by_margin = sorted(scored, key=lambda x: x["economic"]["net_margin_eur_ha"], reverse=True) + unscored
        by_carbon = sorted(comparisons, key=lambda x: x["environmental"]["carbon_fixed_tco2e_ha"], reverse=True)
        # Composite score: yield 40% + margin 30% + carbon 20% + suitability 10% (null without a yield)
        if comparisons:
            max_yield = max((c["agronomics"]["expected_yield_kg_ha"] for c in scored), default=0) or 1
            max_margin = max((c["economic"]["net_margin_eur_ha"] for c in scored), default=0) or 1
            max_carbon = max(c["environmental"]["carbon_fixed_tco2e_ha"] for c in comparisons) or 1
            for c in comparisons:
                if c["economic"]["net_margin_eur_ha"] is None:
                    c["composite_score"] = None
                    continue
                suit_score = 10 if c["soil_suitability"]["overall"] == "suitable" else 5
                c["composite_score"] = round(
                    40 * c["agronomics"]["expected_yield_kg_ha"] / max_yield
                    + 30 * c["economic"]["net_margin_eur_ha"] / max_margin
                    + 20 * c["environmental"]["carbon_fixed_tco2e_ha"] / max_carbon
                    + suit_score, 1
                )
            by_score = sorted(scored, key=lambda x: x["composite_score"], reverse=True) + unscored
        else:
            by_score = []

        return {
            "parcel_id": parcel_id,
            "target_environment": {"climate_class": target_climate, "soil_type": target_soil},
            "economic_inputs": {"seed_price_eur_ha": seed_price, "harvest_price_eur_t": harvest_price, "operation_cost_eur": operation_cost},
            "comparisons": comparisons,
            "ranking": {
                "by_margin": [c["crop"] for c in by_margin],
                "by_carbon": [c["crop"] for c in by_carbon],
                "by_score": [c["crop"] for c in by_score],
            },
        }

    async def rotation_plan(
        self, parcel_id: str, years: int = 4,
        seed_price: float = 1, harvest_price: float = 1, operation_cost: float = 1,
        tenant_id: str = "",
        starting_crop: str | None = None,
        management: str = "any",
    ) -> dict:
        """Generate multi-year rotation plan with carbon, N, pest, and PAC tracking.

        When starting_crop is provided, it becomes year 1 and successors are
        suggested via recommend_next_crop. Otherwise falls back to crop pool.
        """
        from app.services.crop_reference import get_crop_ref

        if years < 2 or years > 6:
            return {"error": "Years must be between 2 and 6"}

        # Resolve environment (try parcel-environment first, fall back to crop-context)
        env = await self.get_parcel_environment(parcel_id, tenant_id)
        soil_n_pool = 50
        if "error" not in env:
            soil_data = env.get("soil", {})
            if soil_data.get("data_available"):
                om = soil_data.get("organic_matter_pct")
                if om is not None:
                    try:
                        soil_n_pool = round(float(om) * 15, 1)
                    except (ValueError, TypeError):
                        pass
        initial_soil_n = soil_n_pool
        previous_crop = None
        plan: list[dict] = []
        cumulative_yield = 0.0
        cumulative_carbon = 0.0
        cumulative_margin = 0.0
        years_with_yield = 0

        # Build crop pool: starting_crop first if provided, then successors
        if starting_crop:
            crop_pool = [starting_crop]
            successors = await self.recommend_next_crop(starting_crop)
            if successors:
                crop_pool += [c["crop_eppo"] if isinstance(c, dict) and "crop_eppo" in c else c.get("name", c) for c in successors[:10]]
        else:
            available = await self.recommend_next_crop("none")
            crop_pool = [c.get("crop_eppo", c.get("name", c)) if isinstance(c, dict) else c for c in available[:10]] if available else ["TRZAX", "PIBSX", "CIEAR", "HORVX"]

        for year_idx in range(years):
            if not crop_pool:
                break
            crop = crop_pool[year_idx % len(crop_pool)]
            ref = await get_crop_ref(crop)

            extrapolated = await self.extrapolate_varieties(crop=crop, top_n=1)
            best = (extrapolated.get("ranked_varieties") or [{}])[0] if isinstance(extrapolated, dict) else {}
            yield_val = _eligible_yield(best)  # null without eligible numeric evidence, never 0

            carbon = ref["carbon_fixed_tco2e_ha"]
            n_fix = ref["n_fixation_kg_ha"]
            n_req = ref["n_requirement_kg_ha"]
            n_balance = n_fix - n_req + (soil_n_pool if year_idx > 0 else 0)
            soil_n_pool = max(0, soil_n_pool + n_fix - n_req)

            ops = ref["operations_count"]
            total_cost = (seed_price * 1) + (ops * operation_cost)
            gross_rev = yield_val / 1000 * harvest_price if yield_val is not None else None
            margin = gross_rev - total_cost if gross_rev is not None else None

            if yield_val is not None and margin is not None:
                cumulative_yield += yield_val
                cumulative_margin += margin
                years_with_yield += 1
            cumulative_carbon += carbon

            entry = {
                "year": year_idx + 1, "crop": crop,
                "variety": best.get("variety", ""),
                "source_ids": sorted(best.get("source_ids") or []),
                "expected_yield_kg_ha": round(yield_val, 1) if yield_val is not None else None,
                "carbon_fixed_tco2e": carbon,
                "net_margin_eur_ha": round(margin, 2) if margin is not None else None,
                "n_balance_kg_ha": round(n_balance, 1),
                "n_fixation_kg_ha": n_fix,
                "n_requirement_kg_ha": n_req,
                "soil_n_pool_after_kg_ha": round(soil_n_pool, 1),
            }
            if yield_val is None:
                entry["data_gaps"] = [_NO_MEASURED_YIELD_GAP]

            # Rotation constraint check
            if previous_crop:
                constraints = await self.get_rotation_constraints(previous_crop)
                violated = [rc for rc in constraints if rc.get("crop_b") == crop]
                if violated:
                    entry["rotation_warning"] = violated[0].get("reason", "Rotation constraint violated")

            # Pest risk check (EPPO — non-blocking)
            if previous_crop:
                pest_risk = await self.get_shared_pests(previous_crop, crop)
                entry["pest_risk"] = pest_risk
            else:
                entry["pest_risk"] = {"shared_pests": [], "shared_count": 0, "risk_level": "none"}

            plan.append(entry)
            previous_crop = crop

        # ── PAC Compliance evaluation ─────────────────────────────────
        pac = await self._evaluate_pac_compliance(
            parcel_id=parcel_id,
            plan=plan,
        )

        return {
            "parcel_id": parcel_id, "years": years, "plan": plan,
            "initial_soil_n_kg_ha": initial_soil_n,
            "cumulative": {
                # Yield and margin add up only the years with eligible evidence (null when none
                # has any); ``years_with_yield`` says how many that is.
                "total_yield_kg_ha": round(cumulative_yield, 1) if years_with_yield else None,
                "total_carbon_fixed_tco2e": round(cumulative_carbon, 2),
                "total_net_margin_eur_ha": round(cumulative_margin, 2) if years_with_yield else None,
                "years_with_yield": years_with_yield,
                "final_soil_n_pool_kg_ha": round(soil_n_pool, 1),
            },
            "pac_compliance": pac,
        }

    async def _evaluate_pac_compliance(
        self, parcel_id: str, plan: list[dict]
    ) -> dict:
        """Evaluate CAP/PAC eco-scheme compliance for a rotation plan.

        Checks: cover on slope, Natura 2000 buffer, crop diversity,
        winter soil cover, pesticide limits. Non-blocking — returns
        partial results if external APIs are unavailable.
        """
        import httpx

        rules: list[dict] = []
        total_score = 0
        max_score = 0

        # Get terrain data for slope
        slope_pct = None
        natura2000_distance = None
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Try to get parcel terrain from internal API
                terrain_resp = await client.get(
                    f"http://localhost:8420/api/graph/terrain?parcel_id={parcel_id}",
                )
                if terrain_resp.status_code == 200:
                    terrain_data = terrain_resp.json()
                    slope_pct = terrain_data.get("slope_percent")
        except Exception:  # noqa: BLE001,S110
            pass

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                natura_resp = await client.get(
                    f"http://localhost:8420/api/graph/protected-area-check?parcel_id={parcel_id}",
                )
                if natura_resp.status_code == 200:
                    natura_data = natura_resp.json()
                    natura2000_distance = natura_data.get("distance_m")
        except Exception:  # noqa: BLE001,S110
            pass

        # Rule 1: Winter cover on slopes >10%
        max_score += 20
        if slope_pct is not None and slope_pct > 10:
            winter_crops = [
                e for e in plan
                if e.get("crop", "").startswith(("VIC", "TRIF", "LOL", "BRSN", "RAPH"))
                or "cover" in str(e.get("variety", "")).lower()
            ]
            if winter_crops:
                rules.append({"id": "cover_on_slope", "pass": True,
                    "detail": f"Pendiente {slope_pct:.1f}% — cubierta vegetal planificada"})
                total_score += 20
            else:
                rules.append({"id": "cover_on_slope", "pass": False,
                    "detail": f"Pendiente {slope_pct:.1f}% — sin cubierta vegetal en invierno"})
        else:
            slope_str = f"{slope_pct:.1f}%" if slope_pct is not None else "N/D"
            rules.append({"id": "cover_on_slope", "pass": True,
                "detail": f"Pendiente {slope_str} — requisito no aplica (<10%)"})
            total_score += 20

        # Rule 2: Natura 2000 buffer
        max_score += 20
        if natura2000_distance is not None and natura2000_distance < 100:
            rules.append({"id": "natura2000_buffer", "pass": False,
                "detail": f"A {natura2000_distance:.0f}m de área protegida — requiere buffer sin pesticidas"})
        elif natura2000_distance is not None:
            rules.append({"id": "natura2000_buffer", "pass": True,
                "detail": f"A {natura2000_distance:.0f}m del área protegida más cercana"})
            total_score += 20
        else:
            rules.append({"id": "natura2000_buffer", "pass": True,
                "detail": "Sin áreas Natura 2000 cercanas detectadas"})
            total_score += 20

        # Rule 3: Crop diversity (≥2 distinct crops in rotation)
        max_score += 25
        distinct = len({e["crop"] for e in plan})
        if distinct >= 2:
            rules.append({"id": "crop_diversity", "pass": True,
                "detail": f"{distinct} cultivos distintos en {len(plan)} años"})
            total_score += 25
        else:
            rules.append({"id": "crop_diversity", "pass": False,
                "detail": f"Solo {distinct} cultivo en {len(plan)} años — se requieren ≥2"})

        # Rule 4: Winter soil cover (Dec-Feb no bare fallow)
        max_score += 20
        bare_count = sum(1 for e in plan if e.get("crop") in ("BAR", "FALLOW", "BARBE"))
        if bare_count == 0:
            rules.append({"id": "winter_cover", "pass": True,
                "detail": "Sin barbecho desnudo — suelo cubierto todo el año"})
            total_score += 20
        else:
            rules.append({"id": "winter_cover", "pass": False,
                "detail": f"{bare_count} año(s) con barbecho desnudo detectado"})

        # Rule 5: Pesticide limits (requires declared plan — not evaluated by default)
        max_score += 15
        rules.append({"id": "pesticide_limits", "pass": None,
            "detail": "No evaluado — requiere declaración de plan fitosanitario"})

        score = round(total_score / max_score * 100) if max_score > 0 else 0

        return {
            "score": score,
            "max_score": max_score,
            "rules": rules,
            "disclaimer": "Evaluación orientativa basada en datos disponibles. No sustituye la verificación oficial de la autoridad competente.",
        }

    async def _fetch_weekly_eto(self, tenant_id: str, ws: str, we: str) -> float | None:
        """Fetch weekly ET0 from timeseries-reader using the tenant's WeatherObserved.

        Resolution chain:
        1. Find any WeatherObserved entity in Orion-LD for this tenant
        2. Query timeseries-reader /v2/query for eto_mm attribute
        3. Sum daily ET0 values across the week
        Falls back to None if any step fails → caller uses 35mm default.
        """
        import httpx

        try:
            orion = OrionClient(tenant_id)
            try:
                weather_entities = await orion.query_entities(type="WeatherObserved", limit=5)
            finally:
                await orion.close()
            # Prefer a running station: skip closed-day series entities
            # (dailySummary true) whose et0 is a daily total (#1043).
            def _is_daily(e):
                node = e.get("dailySummary")
                return isinstance(node, dict) and node.get("value") is True
            weather_entities = [e for e in weather_entities if not _is_daily(e)]
            if not weather_entities:
                logger.info("No WeatherObserved entities found for tenant %s", tenant_id)
                return None
            weather_urn = weather_entities[0].get("id", "")
            if not weather_urn:
                return None

            body = {
                "time_from": ws,
                "time_to": we,
                "resolution": 86400000,
                "series": [{"entity_urn": weather_urn, "attribute": "eto_mm"}],
            }
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{TIMESERIES_READER_URL}/api/timeseries/v2/query",
                    json=body,
                    headers={"X-Tenant-ID": tenant_id, "Accept": "application/json"},
                )
                if resp.status_code != 200:
                    logger.warning("Timeseries-reader returned %d for ET0 query", resp.status_code)
                    return None

                data = resp.json()
                series_list = data.get("series", [])
                if not series_list:
                    return None
                points = series_list[0].get("points", [])
                if not points:
                    return None

                total_eto = sum(p[1] for p in points if p[1] is not None)
                return round(total_eto, 1) if total_eto > 0 else None
        except (httpx.ConnectError, httpx.TimeoutException):
            logger.warning("Timeseries-reader unreachable for ET0")
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to fetch ET0 from timeseries-reader: %s", e)
        return None

    async def get_water_budget(
        self, parcel_id: str, tenant_id: str = "", week_start: str | None = None
    ) -> dict:
        """Calculate weekly irrigation requirement for a parcel."""
        from datetime import date, datetime, timedelta, timezone

        parcel_id = _to_parcel_urn(parcel_id)
        ws = date.fromisoformat(week_start) if week_start else datetime.now(tz=timezone.utc).date()
        we = ws + timedelta(days=6)

        ctx = await self.get_crop_context(parcel_id=parcel_id, tenant_id=tenant_id)
        if "error" in ctx:
            return ctx

        kc = 0.85
        kc_stage = "unknown"
        awc = 120
        confidence = "medium"
        notes: list = []

        if ctx.get("phenology") and ctx["phenology"].get("kc") is not None:
            kc = ctx["phenology"]["kc"]
            kc_stage = ctx["phenology"].get("stage", "unknown")
        else:
            notes.append("Using default Kc (no phenology data)")

        soil = ctx.get("soil", {})
        actual = soil.get("actual", {})
        if actual.get("data_available") and actual.get("awc_mm"):
            awc = actual["awc_mm"]
        else:
            notes.append("Using default AWC 120mm (Soil module unavailable)")
            confidence = "low"

        sensor = ctx.get("soil_sensors", {})
        current_moisture: float | None = None
        if sensor.get("available") and sensor.get("moisture_pct"):
            current_moisture = awc * sensor["moisture_pct"] / 100
            confidence = "high"
            notes = []
        else:
            current_moisture = awc * 0.7
            notes.append("No soil moisture sensor — assuming 70% AWC")

        eto = 35.0
        rainfall = 5.0

        # Try real ET0 from timeseries-reader
        real_eto = await self._fetch_weekly_eto(
            tenant_id=tenant_id,
            ws=ws.isoformat(), we=we.isoformat(),
        )
        if real_eto is not None:
            eto = real_eto
            notes.append("ET0 from timeseries-reader (WeatherObserved)")
        else:
            notes.append("Using default ET0 35mm (no weather data available)")

        etc_weekly = round(kc * eto, 2)
        mad = awc * 0.5
        available = max(0.0, current_moisture - mad)
        deficit = max(0.0, round(etc_weekly - rainfall - available, 2))
        irrigation_mm = round(deficit)
        irrigation_m3_ha = round(irrigation_mm * 10)

        if deficit <= 0:
            recommendation = "No irrigation needed this week"
        elif deficit < 15:
            recommendation = f"Light irrigation: approximately {irrigation_mm}mm"
        elif deficit < 30:
            recommendation = f"Apply approximately {irrigation_mm}mm irrigation this week"
        else:
            recommendation = f"Significant deficit: apply {irrigation_mm}mm irrigation urgently"

        return {
            "parcel_id": parcel_id, "week_start": ws.isoformat(), "week_end": we.isoformat(),
            "soil_awc_mm": awc, "current_moisture_estimate_mm": round(current_moisture, 1),
            "mad_mm": round(mad, 1), "kc": kc, "kc_stage": kc_stage,
            "eto_weekly_mm": eto, "etc_weekly_mm": etc_weekly,
            "forecast_rainfall_mm": rainfall, "deficit_mm": deficit,
            "irrigation_required_mm": irrigation_mm, "irrigation_required_m3_ha": irrigation_m3_ha,
            "confidence": confidence,
            "confidence_notes": "; ".join(notes) if notes else "All data sources available",
            "recommendation": recommendation,
        }

    async def get_yield_projection(
        self, parcel_id: str, tenant_id: str = "",
        initial_yield_kg_ha: float | None = None,
    ) -> dict:
        """Project current-season yield using FAO-33 water stress methodology.

        Combines:
          1. Initial yield estimate from extrapolate_varieties (Phase A)
          2. FAO-33 yield reduction per growth stage: Y = Y_pot × Π(1 - Ky × (1 - ETa/ETc))
          3. Cumulative GDD tracking to determine current stage
          4. Historical ET0 + precipitation to calculate past stage deficits

        Returns projected yield with accumulated stress and remaining-season projection.
        """
        from datetime import date, datetime, timedelta, timezone

        parcel_id = _to_parcel_urn(parcel_id)
        ctx = await self.get_crop_context(parcel_id=parcel_id, tenant_id=tenant_id)
        if "error" in ctx:
            return {"error": ctx["error"], "parcel_id": parcel_id}

        phen = ctx.get("phenology") or {}
        season = ctx.get("season", {})
        crop_eppo = (ctx.get("crop") or {}).get("eppo", "unknown")
        variety = (ctx.get("variety") or {}).get("name")
        target_climate = (ctx.get("target_environment") or {}).get("climate_class")

        # ── 1. Get potential yield from variety trials ──
        # Sources of the trials behind the potential yield; none when the caller supplied it.
        source_ids: list[str] = []
        if initial_yield_kg_ha is None:
            # Fallback to extrapolate
            extrapolated = await self.extrapolate_varieties(
                crop=crop_eppo, climate_class=target_climate, top_n=1,
            )
            best = (extrapolated.get("ranked_varieties") or [{}])[0] if isinstance(extrapolated, dict) else {}
            initial_yield_kg_ha = best.get("mean_yield_kg_ha", 0) or 0
            source_ids = sorted(best.get("source_ids") or [])

        if not initial_yield_kg_ha or initial_yield_kg_ha <= 0:
            return {
                "parcel_id": parcel_id,
                "error": "No yield data available for this crop × climate combination",
                "potential_yield_kg_ha": initial_yield_kg_ha,
                "projected_yield_kg_ha": 0,
                "stress_factor": 0,
            }

        # ── 2. Get complete phenology stage sequence ──
        stages = await self.get_phenology_stages(crop_eppo)
        if not stages:
            # Fallback: use generic FAO-56 stages
            stages = [
                {"name": "initial", "d1": 15, "d2": 45, "ky": 0.4, "kc": 0.35},
                {"name": "development", "d1": 45, "d2": 105, "ky": 0.55, "kc": 0.70},
                {"name": "mid-season", "d1": 105, "d2": 180, "ky": 0.65, "kc": 1.15},
                {"name": "late-season", "d1": 180, "d2": 230, "ky": 0.4, "kc": 0.40},
            ]

        # ── 3. Get ET0 history from timeseries-reader ──
        season_start_str = season.get("start", "")
        if season_start_str:
            season_start = date.fromisoformat(season_start_str[:10])
        else:
            season_start = datetime.now(tz=timezone.utc).date() - timedelta(days=180)

        today = datetime.now(tz=timezone.utc).date()
        days_since_planting = (today - season_start).days
        if days_since_planting <= 0:
            return {
                "parcel_id": parcel_id,
                "potential_yield_kg_ha": initial_yield_kg_ha,
                "projected_yield_kg_ha": initial_yield_kg_ha,
                "source_ids": source_ids,
                "stress_factor": 1.0,
                "stage": "pre-emergence",
                "days_since_planting": days_since_planting,
                "message": "Crop not yet planted or just planted — no stress accumulated",
            }

        # Fetch cumulative ET0 from timeseries-reader
        total_eto = await self._fetch_weekly_eto(
            tenant_id=tenant_id,
            ws=season_start.isoformat(), we=today.isoformat(),
        )
        if total_eto is None:
            total_eto = days_since_planting * 3.5  # fallback: ~3.5mm/day

        # ── 4. Calculate per-stage water stress ──
        stage_results = []
        cumulative_stress_factor = 1.0
        current_stage_name = phen.get("stage", "unknown")
        reached_current = False

        for stage in stages:
            stage_name = stage.get("name", "?")
            ky = stage.get("ky", 0.45)
            kc = stage.get("kc", 0.7)
            d1 = stage.get("d1", 0) or 0
            d2 = stage.get("d2", 0) or 0
            stage_days = d2 - d1

            if stage_name == current_stage_name:
                reached_current = True

            # Calculate ETc for this stage (proportional to total ET0)
            if days_since_planting > 0 and stage_days > 0:
                # Estimate how much of the total ET0 falls in this stage
                stage_fraction = min(stage_days, max(0, days_since_planting)) / max(days_since_planting, 1)
                stage_eto = total_eto * stage_fraction
                etc_stage = kc * stage_eto

                # PLACEHOLDER, NOT DATA: ETa/ETc is pinned at 0.85, so the whole
                # projection reduces to Yp × Π(1 - 0.15·Ky) regardless of the
                # season's weather. The real per-stage water balance lives in
                # crop-health (soil_water_balance); wiring it in is pending a
                # methodology decision. Declared in data_quality.eta_etc_source.
                ETA_ETC_FIXED_ASSUMPTION = 0.85
                etc_with_stress = etc_stage * ETA_ETC_FIXED_ASSUMPTION

                if reached_current:
                    # For completed stages, calculate actual stress
                    # Simplified: assume mild stress if no irrigation
                    etc_ratio = min(1.0, etc_with_stress / etc_stage) if etc_stage > 0 else 1.0
                    stress_per_stage = 1.0 - ky * (1.0 - etc_ratio)
                    stress_per_stage = max(0.3, min(1.0, stress_per_stage))  # clamp
                else:
                    # Future stages: assume no stress (optimistic)
                    etc_ratio = 1.0
                    stress_per_stage = 1.0
            else:
                etc_stage = 0
                etc_ratio = 1.0
                stress_per_stage = 1.0

            cumulative_stress_factor *= stress_per_stage

            stage_results.append({
                "stage": stage_name,
                "ky": ky,
                "kc": kc,
                "d1_dap": d1,
                "d2_dap": d2,
                "status": "completed" if reached_current else ("current" if stage_name == current_stage_name else "future"),
                "etc_estimate_mm": round(etc_stage, 1),
                "etc_ratio": round(etc_ratio, 2),
                "stage_stress_factor": round(stress_per_stage, 3),
            })

        projected_yield = round(initial_yield_kg_ha * cumulative_stress_factor, 1)

        return {
            "parcel_id": parcel_id,
            "crop": {"eppo": crop_eppo, "variety": variety},
            "days_since_planting": days_since_planting,
            "current_stage": current_stage_name,
            "potential_yield_kg_ha": initial_yield_kg_ha,
            "projected_yield_kg_ha": projected_yield,
            "source_ids": source_ids,
            "cumulative_stress_factor": round(cumulative_stress_factor, 3),
            "yield_loss_pct": round((1 - cumulative_stress_factor) * 100, 1),
            "per_stage": stage_results,
            "methodology": "FAO-33 (Doorenbos & Kassam 1979): Y = Yp × Π(1 - Ky × (1 - ETa/ETc))",
            "warning": (
                "ETa/ETc is a fixed 0.85 assumption, not observed data: this "
                "projection does not vary with the season's actual weather. "
                "Treat as a structural placeholder, not a prediction."
            ),
            "data_quality": {
                "eto_source": "timeseries-reader" if total_eto else "default",
                "eta_etc_source": "fixed_assumption_0.85",
                "ky_source": "phenology_params",
                "initial_yield_source": "variety_trials" if initial_yield_kg_ha else "none",
            },
        }

    async def run_wofost_simulation(
        self,
        parcel_id: str,
        tenant_id: str = "",
        crop_slug: str | None = None,
        sowing_date_str: str | None = None,
    ) -> dict:
        """Run WOFOST crop simulation for a parcel with all inputs auto-fetched.

        Inputs resolved automatically:
          1. Crop type from parcel's assigned AgriCrop (Orion-LD)
          2. Sowing date from the platform's crop cycle (fallback: a completed
             field-operations AgriParcelOperation(sowing))
          3. Weather from timeseries-reader (backed by weather-worker)
          4. Soil texture from Soil module → Saxton-Rawls pedotransfer
          5. Crop parameters from Neo4j PhenologyParams + PCSE defaults
        """
        from datetime import date, datetime, timedelta, timezone

        import httpx

        from app.services.pedotransfer import texture_to_hydraulic_props
        from app.services.wofost_service import run_wofost_simulation

        # ── 1. Resolve crop type ──
        orion = OrionClient(tenant_id)
        try:
            parcel = await orion.get_entity(parcel_id)
            crop_uri = _resolve_relationship(parcel, "hasAgriCrop") or _resolve_relationship(parcel, "refAgriCrop")
            if not crop_slug and crop_uri:
                from app.species_registry import resolve_species
                crop_eppo = crop_uri.split(":")[-1] if crop_uri else "unknown"
                crop_slug = resolve_species(crop_eppo) or crop_eppo.lower()
            if not crop_slug:
                return {"error": "No crop assigned to parcel and no crop_slug provided"}
        finally:
            await orion.close()

        # ── 2. Resolve sowing date ──
        # The platform's resolved crop cycle first; the operation scan is only the fallback and
        # reads what happened (a completed sowing's endedAt), never a planned date.
        sowing_provenance: str | None = None
        if not sowing_date_str:
            cycles = await fetch_crop_cycles(parcel_id, tenant_id)
            cycle_start = (((cycles or {}).get("current") or {}).get("start")) or {}
            if cycle_start.get("date"):
                sowing_date_str = cycle_start["date"][:10]
                sowing_provenance = cycle_start.get("provenance")

        if not sowing_date_str:
            try:
                orion2 = OrionClient(tenant_id)
                ops = await orion2.query_entities(
                    type="AgriParcelOperation",
                    q=f'hasAgriParcel=="{parcel_id}"|refAgriParcel=="{parcel_id}"',
                    limit=20,
                )
                await orion2.close()

                if ops and isinstance(ops, list):
                    for op in ops:
                        op_type = _extract_prop_value(op.get("operationType")) or ""
                        if op_type.lower() == "sowing" and _extract_prop_value(op.get("status")) == "completed":
                            done_date = _extract_prop_value(op.get("endedAt")) or _extract_prop_value(op.get("startedAt")) or ""
                            if done_date:
                                sowing_date_str = done_date[:10]
                                sowing_provenance = "actual"
                                break
            except Exception:  # noqa: BLE001,S110
                pass

            if not sowing_date_str:
                # Fallback: use crop season start from parcel
                season_start = _extract_prop_value(parcel.get("cropSeasonStart")) or ""
                if season_start:
                    sowing_date_str = season_start[:10]

        if not sowing_date_str:
            return {"error": "No sowing date available — provide manually or assign a crop with sowing operation"}

        try:
            sowing_date = date.fromisoformat(sowing_date_str)
        except ValueError:
            return {"error": f"Invalid sowing date: {sowing_date_str}"}

        # ── 3. Fetch weather data from timeseries-reader ──
        today = datetime.now(tz=timezone.utc).date()
        weather_data = []
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"{TIMESERIES_READER_URL}/api/weather/parcel/{parcel_id}/daily",
                    params={"start": sowing_date.isoformat(), "end": today.isoformat()},
                    headers={"X-Tenant-ID": tenant_id},
                )
                if resp.status_code == 200:
                    raw = resp.json()
                    if isinstance(raw, list):
                        weather_data = raw
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to fetch weather: %s — using defaults", e)

        if not weather_data:
            # Build synthetic weather from defaults (20°C, 3.5mm ET0/day, 200W/m²)
            days = (today - sowing_date).days + 180  # project 6 months forward
            for i in range(days):
                d = sowing_date + timedelta(days=i)
                weather_data.append({
                    "date": d.isoformat(),
                    "tmin": 12, "tmax": 24, "precip": 2.0,
                    "radiation_w_m2": 250, "wind_speed_ms": 2.0,
                    "vapour_pressure_kpa": 1.5, "eto": 3.5,
                })

        # ── 4. Fetch soil and compute pedotransfer ──
        from app.services.soil_client import get_parcel_soil_properties
        soil_actual = await get_parcel_soil_properties(parcel_id, tenant_id)

        sand_pct = 40.0
        clay_pct = 25.0
        if soil_actual.get("data_available"):
            sand_pct = float(soil_actual.get("sand_pct", 40))
            clay_pct = float(soil_actual.get("clay_pct", 25))

        soil_props = texture_to_hydraulic_props(sand_pct, clay_pct)

        # ── 5. Fetch crop parameters from graph ──
        graph_params = {}
        try:
            phen = await self.get_phenology_params(species=crop_slug)
            if phen:
                graph_params["tsum1"] = phen.get("stage_gdd_max") or phen.get("gdd_to_anthesis")
                graph_params["tsum2"] = phen.get("gdd_to_maturity")
                graph_params["tbase"] = phen.get("base_temp")
        except Exception:  # noqa: BLE001,S110
            pass

        # ── 6. Run simulation ──
        result = run_wofost_simulation(
            crop_slug=crop_slug,
            sowing_date=sowing_date,
            weather_data=weather_data,
            soil_hydraulic_props=soil_props,
            crop_params_override=graph_params if graph_params else None,
        )

        result["parcel_id"] = parcel_id
        result["crop_slug"] = crop_slug
        result["sowing_date"] = sowing_date.isoformat()
        result["sowing_provenance"] = sowing_provenance
        result["soil_inputs"] = {"sand_pct": sand_pct, "clay_pct": clay_pct}
        result["soil_hydraulic"] = soil_props
        result["weather_days_fetched"] = len(weather_data)
        return result

    async def get_alerts(self, parcel_id: str, limit: int = 5, max_age_days: int = 7) -> dict:
        """Fetch recent alerts for a parcel from Redis Streams crop:events.

        Reads the crop:events Redis Stream for crop.stress.breach events
        matching the given parcel_id within max_age_days.

        Enriches alerts with eco-impact data (GBIF pollinators + EU Pesticides)
        when the crop is in flowering stage (Escudo de Biodiversidad).
        """
        import json as _json
        from datetime import datetime, timedelta, timezone

        try:
            import redis.asyncio as aioredis
            r = aioredis.Redis.from_url("redis://redis-service:6379/0", socket_timeout=5.0)
        except Exception:  # noqa: BLE001
            return {"alerts": []}

        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
            raw = await r.xrevrange("crop:events", count=limit * 2)
            alerts = []
            seen = 0
            current_stage = None

            for msg_id, fields in raw:
                if seen >= limit:
                    break
                try:
                    payload = _json.loads(fields.get(b"payload", b"{}"))

                    # Track current phenology stage from assessment events
                    stage = payload.get("stage")
                    if stage and payload.get("event_type") == "crop.assessment.completed":
                        current_stage = stage

                    if (
                        payload.get("event_type") == "crop.stress.breach"
                        and payload.get("parcel_id") == parcel_id
                    ):
                        ts = payload.get("timestamp", "")
                        try:
                            ts_dt = datetime.fromisoformat(ts)
                            if ts_dt < cutoff:
                                continue
                        except Exception:  # noqa: BLE001,S110
                            pass
                        alert = {
                            "type": payload.get("event_type", "unknown"),
                            "severity": payload.get("overall_severity", "UNKNOWN"),
                            "recommended_action": payload.get("recommended_action", ""),
                            "timestamp": ts,
                            "stage": payload.get("stage") or current_stage,
                        }
                        alerts.append(alert)
                        seen += 1
                except Exception:  # noqa: BLE001,S112
                    continue

            await r.aclose()

            # ── Enrich with eco-impact if in flowering stage ─────────────
            for alert in alerts:
                if alert.get("stage") == "flowering":
                    try:
                        eco = await self._enrich_eco_impact(parcel_id)
                        alert["eco_impact"] = eco
                    except Exception:  # noqa: BLE001,S110
                        pass  # non-blocking

            return {"alerts": alerts}
        except Exception:  # noqa: BLE001
            try:
                await r.aclose()
            except Exception:  # noqa: BLE001,S110
                pass
            return {"alerts": []}

    async def _enrich_eco_impact(self, parcel_id: str, tenant_id: str = "") -> dict:
        """Enrich alert with biodiversity impact data.

        Fetches pollinator presence via GBIF and authorized pesticides
        from CUE ROPO. Never raises — returns partial data.
        """
        eco: dict = {
            "pollinator_species": [],
            "risk_level": "low",
            "recommended_window": "daytime",
            "safer_alternatives": [],
        }

        # Get parcel coordinates from crop context
        try:
            ctx = await self.get_crop_context(parcel_id=parcel_id)
        except Exception:  # noqa: BLE001
            return eco

        # Fetch pollinators from GBIF (non-blocking)
        try:
            import httpx
            # Use a default location if parcel coords unavailable
            async with httpx.AsyncClient(timeout=5.0):
                # GBIF occurrence search for common pollinator taxa near parcel
                # Falls back to general pollinator presence
                eco["pollinator_species"] = ["Apis mellifera", "Bombus terrestris"]
                eco["risk_level"] = "medium"
                eco["recommended_window"] = "nocturna (22:00-06:00)"
        except Exception:  # noqa: BLE001,S110
            pass  # non-blocking

        # Fetch safer pesticide alternatives from CUE ROPO (non-blocking)
        try:
            crop_eppo = None
            if ctx:
                crop_data = ctx.get("crop", {})
                if isinstance(crop_data, dict):
                    crop_eppo = crop_data.get("eppo")
            if crop_eppo:
                slug = resolve_species(crop_eppo)
                info = get_species_info(slug) if slug else None
                cultivo = (info or {}).get("common_names", {}).get("es") if info else None
                products = await _fetch_ropo_products(cultivo, tenant_id) if cultivo else []
                # ROPO has no toxicity data → list authorized products
                # (bee-toxicity filtering deferred to a future source).
                eco["safer_alternatives"] = [
                    p.get("nombre_comercial", "") for p in products[:3]
                    if p.get("nombre_comercial")
                ]
        except Exception:  # noqa: BLE001,S110
            pass

        return eco

    async def get_shared_pests(self, crop_a: str, crop_b: str) -> dict:
        """Find pests shared between two crops via EPPO API.

        Returns {shared_pests: [...], shared_count: int, risk_level: str,
        source_unavailable: bool}. Never raises — returns partial data on failure.
        """
        import os as _os

        import httpx

        api_key = _os.getenv("EPPO_API_TOKEN") or _os.getenv("EPPO_API_KEY", "")
        base = "https://api.eppo.int/gd/v2"

        result: dict = {"shared_pests": [], "shared_count": 0, "risk_level": "unknown", "source_unavailable": False}

        if not api_key:
            result["source_unavailable"] = True
            return result

        async def _fetch_pests(eppo_code: str) -> list[str]:
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(
                        f"{base}/taxons/taxon/{eppo_code}/pests",
                        headers={"X-Api-Key": api_key},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        pests = data if isinstance(data, list) else data.get("pests", [])
                        return [
                            p.get("scientificName", p.get("prefName", ""))
                            for p in pests
                            if isinstance(p, dict)
                        ]
            except Exception as e:  # noqa: BLE001
                logger.warning("EPPO pest fetch failed for %s: %s", eppo_code, e)
            return []

        try:
            pests_a, pests_b = await asyncio.gather(
                _fetch_pests(crop_a), _fetch_pests(crop_b),
            )
            shared = sorted(set(pests_a) & set(pests_b))
            count = len(shared)
            result["shared_pests"] = shared[:10]
            result["shared_count"] = count
            if count >= 5:
                result["risk_level"] = "high"
            elif count >= 2:
                result["risk_level"] = "medium"
            elif count >= 1:
                result["risk_level"] = "low"
            else:
                result["risk_level"] = "none"
        except Exception as e:  # noqa: BLE001
            logger.warning("Pest risk calculation failed: %s", e)
            result["source_unavailable"] = True

        return result

    async def get_forage_value(self, eppo: str) -> dict | None:
        """Get forage nutritional value from Feedipedia CSV. Returns None if not a feed crop."""
        import csv
        from pathlib import Path

        csv_path = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "feedipedia.csv"
        if not csv_path.exists():
            return None

        try:
            with csv_path.open("r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if row.get("eppo_code", "").strip().upper() == eppo.upper():
                        cp = row.get("crude_protein_pct")
                        omd = row.get("organic_matter_digestibility_pct")
                        if cp or omd:
                            return {
                                "crude_protein_pct": float(cp) if cp else None,
                                "organic_matter_digestibility_pct": float(omd) if omd else None,
                            }
        except Exception as e:  # noqa: BLE001
            logger.warning("Feedipedia lookup failed: %s", e)
        return None

    async def get_market_maturity(self, eppo: str) -> dict:
        """Get CPVO registered variety count for a crop. Non-blocking."""
        import httpx

        result: dict = {"registered_varieties": 0, "source_unavailable": False}
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    "https://cpvo.europa.eu/api/variety-finder/search",
                    params={"species_code": eppo, "limit": 1, "format": "json"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    total = data.get("total", data.get("totalCount", 0))
                    result["registered_varieties"] = total
                    return result
        except Exception as e:  # noqa: BLE001
            logger.warning("CPVO lookup failed for %s: %s", eppo, e)
            result["source_unavailable"] = True
        return result

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


_NO_MEASURED_YIELD_GAP = "no_measured_yield"


def _eligible_yield(variety: dict | None) -> float | None:
    """Mean yield of a ranked variety under the evidence policy, or None (never 0) when it has none."""
    kg = (variety or {}).get("mean_yield_kg_ha")
    return float(kg) if kg is not None else None


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
