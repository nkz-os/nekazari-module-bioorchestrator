"""Evidence policy — the single source of truth for what counts as numeric yield evidence.

Every reader of trial yields (extrapolation, medians, recommend, the evidence page and
the backtest) classifies trials through this module, either with the Python
classifiers or with the Cypher fragment builders below. Both implement the same rules
and the tests keep them in agreement; no rule (source, purpose, basis, site pattern,
dedup key) is written anywhere else.

Rules (owner decisions 2026-10-04, rule 9 2026-10-06):

1. **Source and derivation.** BSL ``yieldKgHa`` values are the 1–9 note times a per-crop
   constant, not measurements, and a persisted ``yieldKgHa`` that a backfill estimated from a
   note (``yieldDerivationMethod`` set) is not a measurement whatever its source. Trials from an
   excluded source, or with a derived kg/ha, never contribute kg/ha to a numeric aggregate
   (expected yield, interval, medians, relative yield, backtest); they may still count as
   presence evidence. The source is matched on ``source_id`` and ``dataSource``
   (case-insensitive, trimmed).
2. **Purpose.** ``yield_purpose`` reads only the record and returns ``grain``,
   ``forage``, ``fresh`` or ``unknown``:

   - ``forage``: forage feeding-value analysis in ``qualityParams`` (neutral-detergent
     fibre, organic-matter digestibility, UFL energy, ear share of whole-plant dry
     matter), or a ``yieldMetric`` naming forage, silage or a fresh/green-matter basis;
   - ``grain``: a ``yieldMetric`` naming grain or seed;
   - ``fresh``: a ``yieldMetric`` naming fresh produce (fruit, vegetables, grape);
   - ``unknown``: no such evidence (the large majority of records today).

   Dry matter % and starch alone are not forage markers (tuber/fruit traits too).
   Requests choose a **purpose mode**: ``main`` (default) is each crop's main harvested
   product — grain for cereals, fruit for vegetables and fruit trees, kernel for almond,
   tuber for potato — and takes every record not positively classified as forage
   (grain, fresh and unknown, for every crop family). ``forage`` takes forage records
   only. ``crop_family`` is metadata (UI labels, the grain-only backtest) and never
   drops a crop from the main mode. A crop whose records mixed two different main
   products (e.g. fresh sweet-corn ears and grain maize) would need them split; no crop
   does today (no record carries a grain or fresh ``yieldMetric``), so no split is applied.
3. **Forage basis.** ``forage_basis`` is ``dry_matter``, ``fresh_matter`` or ``unknown``,
   decided only from the record (``yieldBasis`` property, a ``yieldMetric`` unit) or a
   cited source-level rule (``FORAGE_BASIS_SOURCE_RULES``) — never from a dry-matter %
   or from magnitude. A dry-matter % in ``qualityParams`` only converts a fresh-matter
   yield to dry matter. Unknown-basis forage yields are never averaged with known-basis
   ones: numeric forage aggregates take ``is_forage_numeric_evidence`` (forage-eligible
   AND convertible to kg dry matter/ha, value ``forage_dm_yield``), never the bare
   forage-mode eligibility, which says only that the record is forage from a measured
   source and is what counts and presence use.
4. **Site kind.** A (trial, site) row is *field* evidence only when the trial's
   ``aggregationScope`` is ``'site'`` (or absent) and the site is not an aggregate
   pseudo-site (national/regional registries, "average of N locations", zones, unknown
   or empty names). Aggregate evidence is shown apart, never mixed with field evidence.
5. **Content dedup.** Trials with identical observed content — crop, normalized variety,
   year, yield (kg/ha and note), irrigation regime, production system, rootstock, scion,
   training system, planting year, planting density, crop cycle and the set of linked
   site names — are one observation, whatever their ``mergeKey``. The orchard fields keep
   perennial-crop trials that differ only in rootstock, density or cycle distinct.

6. **Evidence tier.** A (trial, site) row is ``field`` evidence when rule 4 holds and its source is
   not an aggregate source (rule 9), and ``regional`` evidence otherwise (aggregate pseudo-sites,
   regional/national scope, aggregate sources).
   Regional evidence never enters a field aggregate; it backs a recommendation only when
   the crop has no numeric field evidence, and the answer then says so. ``cypher_row_policy``
   classifies every row of a query once (tier, purpose gate, policy yield) so no query
   re-evaluates the rules per aggregate.

7. **Presence-only evidence.** A trial with an excluded kg/ha (rule 1) at a regional-tier row adds
   no number, but it shows that the crop was tested at that climate. In the main mode (forage
   records are never BSL) a crop whose ONLY evidence is such trials is reported as a regional
   recommendation with no yield (``presence_only_applies``, ``cypher_presence_gate``); the
   excluded kg/ha are never read.

8. **Irrigation regime.** A trial stores its regime as an AGROVOC URI (rainfed / irrigated), but
   one source stores the literals ``secano`` / ``regadío``. ``irrigation_regime`` classifies a
   stored value or a requested one as ``secano``, ``regadio`` or none, and every comparison of
   regimes (the request filters, the reference median, the water-regime weight, the evidence
   page, the presence scan) goes through it (``cypher_irrigation_match``), so a literal counts
   exactly like its URI. A trial without a regime, or with an unrecognised value, never matches a
   requested regime.

9. **Aggregate sources.** The rows of a source in ``AGGREGATE_SOURCES`` (GENVCE: zone and national
   averages, no trial location published) are ``regional`` evidence whatever site they are linked
   to: a representative city stands in for a zone, so the row is not a field trial there. The
   source is matched like rule 1 (``source_id`` and ``dataSource``, every alias of the source
   registry). Their kg/ha stay measured numbers: the regional tier averages them, the field tier
   never sees them. A reader of the regional tier scans every site of the climate, not only the
   aggregate pseudo-sites, because these rows sit at field-named ones.

Explicit properties written by ingestion (``yieldBasis`` today; ``siteKind`` and a
grain/forage ``yieldMetric`` vocabulary later) are read first, here; callers do not change.

Cost on hot paths: the Cypher fragments run once per trial row. The purpose, basis and
dry-matter fragments scan the ``qualityParams`` text, and ``cypher_content_key`` runs a
per-trial relationship expand (``COLLECT``). Put the cheap filters first (``yieldKgHa IS
NOT NULL``, crop, source), then the policy predicates, and build the dedup key last on the
rows that survive. Measure any per-request query that would evaluate them over the whole
graph; if latency requires it, have ingestion write the answer as a property (the policy
already reads ``yieldBasis`` first).
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any, NamedTuple

from app.ingestion.normalization_registry import source_spellings
from app.ingestion.trial_site_geo import AGGREGATE_PATTERNS, is_aggregate_site_name

# Bump when a rule changes, so backtest baselines name the policy they were measured under.
POLICY_VERSION = "2026-10-06.1"

# ── (a) source policy ────────────────────────────────────────────────────────
# Lowercased ``source_id`` / ``dataSource`` values whose kg/ha are not measurements.
# Production carries source_id 'BSL' with dataSource null, 'bsa bundessortenamt' or 'bsa';
# 'bundessortenamt' is the remaining alias that canonicalises to BSL at ingestion.
NUMERIC_YIELD_EXCLUDED_SOURCES: frozenset[str] = frozenset({
    "bsl",
    "bsa",
    "bsa bundessortenamt",
    "bundessortenamt",
})

# Canonical ``source_id`` of the sources whose rows are zone/national averages, with the reason.
# Their rows are regional evidence whatever site they are linked to (rule 9).
AGGREGATE_SOURCES: Mapping[str, str] = {
    "GENVCE": "zone/national averages, no trial location published",
}
# Lowercased ``source_id`` / ``dataSource`` spellings of those sources (the ids and every alias of
# the source registry), the same matching as the excluded sources.
AGGREGATE_SOURCE_SPELLINGS: frozenset[str] = frozenset(
    spelling for source in AGGREGATE_SOURCES for spelling in source_spellings(source)
)

# ── (b) purpose ──────────────────────────────────────────────────────────────
PURPOSE_GRAIN = "grain"
PURPOSE_FORAGE = "forage"
PURPOSE_FRESH = "fresh"
PURPOSE_UNKNOWN = "unknown"

MODE_MAIN = "main"
MODE_FORAGE = "forage"
PURPOSE_MODES: tuple[str, ...] = (MODE_MAIN, MODE_FORAGE)

# ``yieldMetric`` tokens, tested in this order: forage, grain, fresh.
FORAGE_METRIC_TOKENS: tuple[str, ...] = (
    "forage", "silage", "fresh_matter", "green_matter", "materia_verde", "materia_fresca",
)
GRAIN_METRIC_TOKENS: tuple[str, ...] = ("grain", "seed")
FRESH_METRIC_TOKENS: tuple[str, ...] = ("fresh",)

# Lowercased ``qualityParams`` keys that only a forage analysis reports.
FORAGE_QUALITY_KEYS: frozenset[str] = frozenset({
    # neutral-detergent fibre
    "ndf_pct",
    "fnd_pct",
    "fnd_pct_s_ms",
    "fibra_neutro_detergente_pct",
    "fibra_neutro_detergente_pct_sms",
    # organic-matter digestibility
    "digestibility_pct",
    "digestibilidad_materia_organica_pct",
    "organic_matter_digestibility_pct",
    "dmo_pct",
    "materia_organica_digestible_kg_ha",
    # net energy (UFL)
    "concentracion_energetica_ufl_kg_ms",
    "concentracion_energetica_ufl_ha",
    "energy_ufl_kg_ms",
    "energy_concentration_ufl_kg_ms",
    # ear share of whole-plant dry matter (silage maize)
    "aporte_mazorca_pct",
    "aportacion_mazorca_pct",
    "ear_contribution_pct",
})

# Crop family by EPPO code (metadata; codes present in the graph or the species registry).
CROP_FAMILY_GRAIN = "grain"
CROP_FAMILY_FORAGE = "forage"
CROP_FAMILY_FRESH = "fresh"
CROP_FAMILY_OTHER = "other"
CROP_FAMILIES: Mapping[str, frozenset[str]] = {
    # ASSUMPTION: VICSA (common vetch), VICNA (narbonne vetch) and SETIT (foxtail millet / moha)
    # are listed as grain although they are often grown for forage; their forage records are
    # still routed to forage by the record evidence, and the family only drives labels and the
    # grain-only backtest — confirm before relying on it elsewhere.
    # cereals, pseudo-cereals, grain legumes, oilseeds
    CROP_FAMILY_GRAIN: frozenset({
        "TRZAX", "TRZDU", "TRZAW", "TRZSP", "HORVX", "ZEAMX", "ZEAMA", "SECCE", "TTLSS",
        "AVESA", "ORYSA", "SORVU", "ELEIN", "SETIT", "QUCHX", "FAGES",
        "BRSNN", "BRSNW", "HELAN", "GLXMA", "LINUS", "LIUUT", "SINAL",
        "PIBSX", "PIBAR", "VICFX", "LUPAL", "CIEAR", "CIEAS", "LENCU", "VICER", "VICNA",
        "VICSA", "LTHSA", "PHSVX",
    }),
    # forage legumes and cover crops
    CROP_FAMILY_FORAGE: frozenset({
        "MEDSA", "MEDSC", "MEDPO", "TRFFR", "TRFRE", "TRFSU", "ONBVI", "LOTCO", "MEUOF", "VICVI",
    }),
    # vegetables, fruit trees, grape
    CROP_FAMILY_FRESH: frozenset({
        "LYPES", "CPSAN", "SOLME", "BRSOX", "BRSOK", "APUGV", "LACSA", "ALLPO", "ALLSA",
        "ALLCE", "CYUCA", "ASPOF", "CUMME", "FRAAN", "PRNAV", "PRNPS", "PRNAR", "PRNDO",
        "MABSD", "PYUCO", "VITVI",
    }),
    # nuts, olive, tubers, roots, industrial crops (and any unmapped code)
    # ASSUMPTION: OLVEU (olive), PRNDU (almond) and SOLTU (potato) are "other": their main
    # product (fruit, kernel, tuber) is neither grain nor fresh produce, and the family is
    # metadata only — confirm before relying on it elsewhere.
    CROP_FAMILY_OTHER: frozenset({"PRNDU", "PIAVE", "OLVEU", "SOLTU", "BEAVX", "CNISA"}),
}

# ── forage basis ─────────────────────────────────────────────────────────────
BASIS_DRY_MATTER = "dry_matter"
BASIS_FRESH_MATTER = "fresh_matter"
BASIS_UNKNOWN = "unknown"

DRY_MATTER_METRIC_TOKENS: tuple[str, ...] = ("dry_matter", "materia_seca")
FRESH_MATTER_METRIC_TOKENS: tuple[str, ...] = (
    "fresh_matter", "green_matter", "materia_verde", "materia_fresca",
)
# ``qualityParams`` keys holding the dry-matter % of the harvested material (first wins).
DRY_MATTER_PCT_KEYS: tuple[str, ...] = ("dry_matter_pct", "materia_seca_pct", "ms_pct")


class ForageBasisRule(NamedTuple):
    """Basis stated by a source document for one (source, crop, year) trial set."""

    source_id: str
    crop_eppo: str
    year: int
    basis: str
    document: str
    quote: str


_NA = "Navarra Agraria"
FORAGE_BASIS_SOURCE_RULES: tuple[ForageBasisRule, ...] = (
    ForageBasisRule("NAVARRA-AGRARIA", "SETIT", 2014, BASIS_DRY_MATTER,
                    f"{_NA} 209 (2015), moha trial 2014, Tabla 2",
                    "Tabla 2. Ensayo de moha 2014. Producción y calidad ... kg ms/ha"),
    ForageBasisRule("NAVARRA-AGRARIA", "SETIT", 2016, BASIS_DRY_MATTER,
                    f"{_NA} 222, Juansenea 2016 forage species, Tabla 2 (moha)",
                    "resultados de producción (1 kg ms/ha), proteína bruta (2% sms)"),
    ForageBasisRule("NAVARRA-AGRARIA", "ZEAMX", 2019, BASIS_DRY_MATTER,
                    f"{_NA} 239, forage maize varieties 2019, Tablas 1-6",
                    "Tabla 1. Resultados de los ensayos de maíz forraje ciclos 200-300. "
                    "Oskotz 2019 ... (kg ms/ha)"),
    ForageBasisRule("NAVARRA-AGRARIA", "ZEAMX", 2020, BASIS_DRY_MATTER,
                    f"{_NA} 245, forage maize varieties, campaign 2020, Tablas 1-4",
                    "Tabla 1. Resultados de los ensayos de maíz forraje ciclos 200-300. "
                    "Oskotz 2020 ... Producción (kg ms/ha)"),
    ForageBasisRule("NAVARRA-AGRARIA", "ZEAMX", 2022, BASIS_DRY_MATTER,
                    f"{_NA} 255, forage maize varieties 2022, Tablas 1-3",
                    "La producción media final es de 25.632 kg materia seca/ha."),
    # The 2017-2021 alfalfa trial is stored twice, under its last trial year and
    # under the publication year (identical values).
    ForageBasisRule("NAVARRA-AGRARIA", "MEDSA", 2021, BASIS_DRY_MATTER,
                    f"{_NA} 256, alfalfa variety trial 2017-2021, Tabla 1",
                    "Tabla 1. Resultados del ensayo de alfalfa 2017-2021 ... "
                    "Producción (kg materia seca/ha)"),
    ForageBasisRule("NAVARRA-AGRARIA", "MEDSA", 2023, BASIS_DRY_MATTER,
                    f"{_NA} 256, alfalfa variety trial 2017-2021, Tabla 1",
                    "Tabla 1. Resultados del ensayo de alfalfa 2017-2021 ... "
                    "Producción (kg materia seca/ha)"),
)

# ── (c) site kind ────────────────────────────────────────────────────────────
SITE_KIND_FIELD = "field"
SITE_KIND_AGGREGATE = "aggregate"
FIELD_SCOPE = "site"  # aggregationScope of a located field trial (others: national, regional, unlocated)

# Evidence tier of a (trial, site) row (rule 6).
EVIDENCE_TIER_FIELD = "field"
EVIDENCE_TIER_REGIONAL = "regional"
EVIDENCE_TIERS: tuple[str, ...] = (EVIDENCE_TIER_FIELD, EVIDENCE_TIER_REGIONAL)

# Substring patterns beyond the geo-backfill ones in ``trial_site_geo``.
EXTRA_AGGREGATE_SITE_PATTERNS: tuple[str, ...] = (
    "bsl deutschland",  # BSL Köppen containers ("BSL Deutschland Cfb/Dfb/Uebergang")
    "bundesweit",
    "zones ",  # INRA Maroc agro-ecological zones
    "bour ",
    "régions",
    "chaouia",
    "large adaptation",
    "non spécifié",
)
AGGREGATE_SITE_PATTERNS: tuple[str, ...] = tuple(AGGREGATE_PATTERNS) + EXTRA_AGGREGATE_SITE_PATTERNS

# Exact (lowercased, trimmed) names of regional / zone / network pseudo-sites whose
# words also appear inside real site names ("Sesma (Navarra)").
AGGREGATE_SITE_NAMES: tuple[str, ...] = (
    "navarra",
    "secanos frescos de navarra",
    "red genvce",
)


# ── Python classifiers ───────────────────────────────────────────────────────

def _norm(value: Any) -> str:
    return value.strip().lower() if isinstance(value, str) else ""


def is_excluded_source(source_id: str | None, data_source: str | None) -> bool:
    """True when the trial's source may not contribute kg/ha to numeric aggregates."""
    return (_norm(source_id) in NUMERIC_YIELD_EXCLUDED_SOURCES
            or _norm(data_source) in NUMERIC_YIELD_EXCLUDED_SOURCES)


def is_aggregate_source(source_id: str | None, data_source: str | None) -> bool:
    """True when the trial's source publishes zone/national averages (rule 9): regional evidence
    at whatever site the trial is linked to."""
    return (_norm(source_id) in AGGREGATE_SOURCE_SPELLINGS
            or _norm(data_source) in AGGREGATE_SOURCE_SPELLINGS)


def is_derived_yield(trial: Mapping[str, Any]) -> bool:
    """The trial's ``yieldKgHa`` was estimated from a note by a backfill (``yieldDerivationMethod``
    set): a fabricated number, whatever the source."""
    return trial.get("yieldDerivationMethod") is not None


def is_excluded_yield(trial: Mapping[str, Any]) -> bool:
    """The trial's kg/ha is not a measurement (rule 1): an excluded source or a derived value."""
    return is_excluded_source(trial.get("source_id"), trial.get("dataSource")) or is_derived_yield(trial)


def _metric_has(yield_metric: str | None, tokens: Iterable[str]) -> bool:
    low = _norm(yield_metric)
    return any(tok in low for tok in tokens)


def has_forage_indicators(quality_params: str | Mapping[str, Any] | None) -> bool:
    """Forage-analysis keys in ``qualityParams`` (JSON object string, or a decoded mapping).

    On a string the key must appear quoted (``"ndf_pct"``), the same test the Cypher
    fragment runs, so a key name inside a value or a longer key does not match.
    """
    if isinstance(quality_params, Mapping):
        return any(_norm(k) in FORAGE_QUALITY_KEYS for k in quality_params)
    if isinstance(quality_params, str):
        low = quality_params.lower()
        return any(f'"{k}"' in low for k in FORAGE_QUALITY_KEYS)
    return False


def yield_purpose(yield_metric: str | None,
                  quality_params: str | Mapping[str, Any] | None) -> str:
    """Purpose of the record's yield, from the record only: grain|forage|fresh|unknown."""
    if has_forage_indicators(quality_params) or _metric_has(yield_metric, FORAGE_METRIC_TOKENS):
        return PURPOSE_FORAGE
    if _metric_has(yield_metric, GRAIN_METRIC_TOKENS):
        return PURPOSE_GRAIN
    if _metric_has(yield_metric, FRESH_METRIC_TOKENS):
        return PURPOSE_FRESH
    return PURPOSE_UNKNOWN


def check_mode(mode: str) -> str:
    if mode not in PURPOSE_MODES:
        raise ValueError(f"unknown purpose mode: {mode!r}")
    return mode


def in_purpose_mode(purpose: str, mode: str = MODE_MAIN) -> bool:
    """Main mode takes every purpose but forage; forage mode takes forage only."""
    if check_mode(mode) == MODE_FORAGE:
        return purpose == PURPOSE_FORAGE
    return purpose != PURPOSE_FORAGE


def crop_family(crop_eppo: str | None) -> str:
    code = crop_eppo.strip().upper() if isinstance(crop_eppo, str) else ""
    for family, codes in CROP_FAMILIES.items():
        if code in codes:
            return family
    return CROP_FAMILY_OTHER


def is_numeric_yield_eligible(trial: Mapping[str, Any], mode: str = MODE_MAIN) -> bool:
    """Source, derivation and purpose gate for ``mode`` (graph property names): not an excluded
    source, not a derived kg/ha, and the purpose fits the mode.

    It checks nothing about the forage basis, so in forage mode it only says the record is
    forage evidence (use it for counts and presence). A numeric forage aggregate must use
    ``is_forage_numeric_evidence``, which also needs a kg dry-matter value.
    """
    if is_excluded_yield(trial):
        return False
    return in_purpose_mode(yield_purpose(trial.get("yieldMetric"), trial.get("qualityParams")), mode)


def is_forage_numeric_evidence(trial: Mapping[str, Any]) -> bool:
    """Forage-eligible AND convertible to kg dry matter/ha (the value is ``forage_dm_yield``).

    Unknown-basis rows, and fresh-matter rows without a usable DM %, fail it: they count as
    forage evidence but never contribute a number.
    """
    return is_numeric_yield_eligible(trial, MODE_FORAGE) and forage_dm_yield(trial) is not None


def is_grain_yield(trial: Mapping[str, Any]) -> bool:
    """Grain yield of a grain-family crop (the backtest's ground truth)."""
    return (
        not is_excluded_yield(trial)
        and crop_family(trial.get("cropEppo")) == CROP_FAMILY_GRAIN
        and yield_purpose(trial.get("yieldMetric"), trial.get("qualityParams"))
        in (PURPOSE_GRAIN, PURPOSE_UNKNOWN)
    )


def _source_rule_basis(source_id: Any, crop_eppo: Any, year: Any) -> str | None:
    src = source_id.strip().upper() if isinstance(source_id, str) else ""
    crop = crop_eppo.strip().upper() if isinstance(crop_eppo, str) else ""
    for rule in FORAGE_BASIS_SOURCE_RULES:
        if (rule.source_id, rule.crop_eppo) == (src, crop) and year == rule.year:
            return rule.basis
    return None


def forage_basis(trial: Mapping[str, Any]) -> str:
    """dry_matter|fresh_matter|unknown, from the record or a cited source rule only."""
    explicit = _norm(trial.get("yieldBasis"))
    if explicit in (BASIS_DRY_MATTER, BASIS_FRESH_MATTER):
        return explicit
    metric = trial.get("yieldMetric")
    if _metric_has(metric, DRY_MATTER_METRIC_TOKENS):
        return BASIS_DRY_MATTER
    if _metric_has(metric, FRESH_MATTER_METRIC_TOKENS):
        return BASIS_FRESH_MATTER
    rule = _source_rule_basis(trial.get("source_id"), trial.get("cropEppo"), trial.get("year"))
    return rule or BASIS_UNKNOWN


def dry_matter_pct(quality_params: str | Mapping[str, Any] | None) -> float | None:
    """Dry-matter % of the harvested material, if ``qualityParams`` reports one in (0, 100]."""
    obj: Any = quality_params
    if isinstance(quality_params, str):
        try:
            obj = json.loads(quality_params)
        except ValueError:
            return None
    if not isinstance(obj, Mapping):
        return None
    low = {_norm(k): v for k, v in obj.items()}
    for key in DRY_MATTER_PCT_KEYS:
        value = low.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value) if 0 < value <= 100 else None
    return None


def forage_dm_yield(trial: Mapping[str, Any]) -> float | None:
    """Forage yield in kg dry matter/ha, or None when the basis or the DM % is missing."""
    kg = trial.get("yieldKgHa")
    if kg is None:
        return None
    basis = forage_basis(trial)
    if basis == BASIS_DRY_MATTER:
        return float(kg)
    if basis == BASIS_FRESH_MATTER:
        dm = dry_matter_pct(trial.get("qualityParams"))
        return float(kg) * dm / 100.0 if dm is not None else None
    return None


def is_field_scope(aggregation_scope: str | None) -> bool:
    return aggregation_scope is None or _norm(aggregation_scope) == FIELD_SCOPE


def is_aggregate_site(name: str | None) -> bool:
    low = _norm(name)
    if not low:
        return True
    return (is_aggregate_site_name(name)
            or low in AGGREGATE_SITE_NAMES
            or any(p in low for p in EXTRA_AGGREGATE_SITE_PATTERNS))


def site_kind(name: str | None) -> str:
    return SITE_KIND_AGGREGATE if is_aggregate_site(name) else SITE_KIND_FIELD


def is_field_evidence(aggregation_scope: str | None, site_name: str | None, *,
                      source_id: str | None = None, data_source: str | None = None) -> bool:
    """Rule 4 (scope and site) and rule 9 (the trial's source; omit it and the row is judged on
    scope and site alone)."""
    return (is_field_scope(aggregation_scope) and not is_aggregate_site(site_name)
            and not is_aggregate_source(source_id, data_source))


def check_tier(tier: str) -> str:
    if tier not in EVIDENCE_TIERS:
        raise ValueError(f"unknown evidence tier: {tier!r}")
    return tier


def evidence_tier(aggregation_scope: str | None, site_name: str | None, *,
                  source_id: str | None = None, data_source: str | None = None) -> str:
    """``field`` or ``regional`` for one (trial, site) row (rule 6)."""
    return (EVIDENCE_TIER_FIELD
            if is_field_evidence(aggregation_scope, site_name,
                                 source_id=source_id, data_source=data_source)
            else EVIDENCE_TIER_REGIONAL)


# ── irrigation regime (rule 8) ───────────────────────────────────────────────
REGIME_RAINFED = "secano"
REGIME_IRRIGATED = "regadio"
IRRIGATION_URIS: Mapping[str, str] = {
    REGIME_RAINFED: "http://aims.fao.org/aos/agrovoc/c_6436",
    REGIME_IRRIGATED: "http://aims.fao.org/aos/agrovoc/c_3954",
}
# Lowercased spellings under which a trial (or a request) names each regime: the URI and the
# literals of the source that does not use it (INIAV: 306 trials with "secano").
IRRIGATION_SPELLINGS: Mapping[str, tuple[str, ...]] = {
    REGIME_RAINFED: (IRRIGATION_URIS[REGIME_RAINFED], "secano"),
    REGIME_IRRIGATED: (IRRIGATION_URIS[REGIME_IRRIGATED], "regadío", "regadio"),
}
# Spellings of the farmer-facing request parameter, besides the stored ones above.
IRRIGATION_REQUEST_ALIASES: Mapping[str, str] = {
    "secano": REGIME_RAINFED, "rainfed": REGIME_RAINFED, "secano/rainfed": REGIME_RAINFED,
    "regadío": REGIME_IRRIGATED, "regadio": REGIME_IRRIGATED,
    "irrigated": REGIME_IRRIGATED, "irrigado": REGIME_IRRIGATED,
}


def irrigation_regime(value: Any) -> str | None:
    """``secano`` | ``regadio`` for a stored or requested regime value (URI or literal,
    case-insensitive, trimmed); None for no value or an unrecognised one."""
    key = _norm(value)
    for regime, spellings in IRRIGATION_SPELLINGS.items():
        if key in spellings:
            return regime
    return None


def irrigation_uri(request: str | None) -> str | None:
    """The stored AGROVOC URI of a requested regime (``secano``, ``regadío``, ``rainfed``, ...);
    None when nothing (or nothing recognised) is requested."""
    regime = IRRIGATION_REQUEST_ALIASES.get(_norm(request))
    return IRRIGATION_URIS[regime] if regime else None


def irrigation_matches(value: Any, target: Any) -> bool:
    """A trial's regime ``value`` satisfies the requested ``target``. No target: always. A target
    that names no regime matches nothing; neither does a missing or unrecognised value."""
    if target is None:
        return True
    regime = irrigation_regime(target)
    return regime is not None and irrigation_regime(value) == regime


def policy_yield(trial: Mapping[str, Any], mode: str = MODE_MAIN) -> float | None:
    """The kg/ha a trial contributes to a numeric aggregate of ``mode``, or None.

    Main mode: ``yieldKgHa`` of a record that passes ``is_numeric_yield_eligible``. Forage
    mode: ``forage_dm_yield`` (kg dry matter/ha) of a record that is
    ``is_forage_numeric_evidence``. None never means 0: the trial adds no number.
    """
    if not is_numeric_yield_eligible(trial, mode):
        return None
    if check_mode(mode) == MODE_FORAGE:
        return forage_dm_yield(trial)
    kg = trial.get("yieldKgHa")
    return None if kg is None else float(kg)


def is_other_purpose_evidence(trial: Mapping[str, Any], mode: str = MODE_MAIN) -> bool:
    """A trial of the other purpose that a ``mode`` answer reports as a count only.

    Main mode counts forage trials from a non-excluded source (the "N forage trials" notice).
    Forage mode reports no off-mode count, so it is always False there.
    """
    return check_mode(mode) == MODE_MAIN and is_numeric_yield_eligible(trial, MODE_FORAGE)


def has_unconverted_kg(trial: Mapping[str, Any], mode: str = MODE_MAIN) -> bool:
    """Eligible trial with a kg value that contributes no number (forage basis unknown)."""
    return (is_numeric_yield_eligible(trial, mode) and trial.get("yieldKgHa") is not None
            and policy_yield(trial, mode) is None)


# Trial properties null-coalesced to '' in the dedup key (mirrors the Cypher ``coalesce``):
# an absent value and an empty one are the same observation, a present one is not.
CONTENT_KEY_BLANK_DEFAULT_FIELDS: tuple[str, ...] = (
    "irrigationRegime", "productionSystem",
    "rootstock", "scion", "trainingSystem", "plantingYear", "plantingDensityTreesHa", "cropCycle",
)


def content_key(trial: Mapping[str, Any], site_names: Iterable[str]) -> tuple:
    """Dedup key of one trial (graph property names); equal keys = one observation."""
    return (
        trial.get("cropEppo"),
        trial.get("varietyNormalized"),
        trial.get("year"),
        trial.get("yieldKgHa"),
        trial.get("yieldNoteS1"),
        *("" if trial.get(f) is None else trial.get(f) for f in CONTENT_KEY_BLANK_DEFAULT_FIELDS),
        tuple(sorted(set(site_names))),
    )


# ── Cypher fragment builders ─────────────────────────────────────────────────
# Each returns a Cypher expression (boolean, string, number or list) over the given node
# alias, ready to drop into a WHERE / WITH. Literals are inlined (constants of this
# module), so callers pass no extra parameters.

_ALIAS = re.compile(r"[A-Za-z][A-Za-z0-9_]*")


def _alias(name: str) -> str:
    if not isinstance(name, str) or not _ALIAS.fullmatch(name):
        raise ValueError(f"invalid Cypher alias: {name!r}")
    return name


def _cypher_str(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _cypher_list(values: Iterable[str]) -> str:
    return "[" + ", ".join(_cypher_str(v) for v in values) + "]"


def _cypher_norm(expr: str) -> str:
    return f"toLower(trim(coalesce({expr}, '')))"


def cypher_excluded_source(vt: str = "vt") -> str:
    vt = _alias(vt)
    excluded = _cypher_list(sorted(NUMERIC_YIELD_EXCLUDED_SOURCES))
    return (f"({_cypher_norm(f'{vt}.source_id')} IN {excluded} "
            f"OR {_cypher_norm(f'{vt}.dataSource')} IN {excluded})")


def cypher_aggregate_source(vt: str = "vt") -> str:
    """True when the trial's source publishes zone/national averages (see ``is_aggregate_source``)."""
    vt = _alias(vt)
    spellings = _cypher_list(sorted(AGGREGATE_SOURCE_SPELLINGS))
    return (f"({_cypher_norm(f'{vt}.source_id')} IN {spellings} "
            f"OR {_cypher_norm(f'{vt}.dataSource')} IN {spellings})")


def cypher_derived_yield(vt: str = "vt") -> str:
    """True when the trial's kg/ha was derived from a note (see ``is_derived_yield``)."""
    vt = _alias(vt)
    return f"({vt}.yieldDerivationMethod IS NOT NULL)"


def cypher_excluded_yield(vt: str = "vt") -> str:
    """True when the trial's kg/ha is not a measurement (see ``is_excluded_yield``)."""
    return f"({cypher_excluded_source(vt)} OR {cypher_derived_yield(vt)})"


def _cypher_any_contains(expr: str, tokens: Iterable[str], var: str) -> str:
    return f"any({var} IN {_cypher_list(tokens)} WHERE {expr} CONTAINS {var})"


def _cypher_forage_indicators_over(quality_lower: str) -> str:
    keys = (f'"{k}"' for k in sorted(FORAGE_QUALITY_KEYS))
    return _cypher_any_contains(quality_lower, keys, "ep_key")


def cypher_forage_indicators(vt: str = "vt") -> str:
    vt = _alias(vt)
    return _cypher_forage_indicators_over(f"toLower(coalesce({vt}.qualityParams, ''))")


def _cypher_yield_purpose_over(quality_lower: str, metric: str) -> str:
    """The purpose CASE over a lowercased ``qualityParams`` text and a normalised metric."""
    return (
        f"(CASE WHEN {_cypher_forage_indicators_over(quality_lower)} "
        f"OR {_cypher_any_contains(metric, FORAGE_METRIC_TOKENS, 'ep_tok')} "
        f"THEN {_cypher_str(PURPOSE_FORAGE)} "
        f"WHEN {_cypher_any_contains(metric, GRAIN_METRIC_TOKENS, 'ep_tok')} "
        f"THEN {_cypher_str(PURPOSE_GRAIN)} "
        f"WHEN {_cypher_any_contains(metric, FRESH_METRIC_TOKENS, 'ep_tok')} "
        f"THEN {_cypher_str(PURPOSE_FRESH)} "
        f"ELSE {_cypher_str(PURPOSE_UNKNOWN)} END)"
    )


def cypher_yield_purpose(vt: str = "vt") -> str:
    """String expression: 'grain' | 'forage' | 'fresh' | 'unknown' (see ``yield_purpose``)."""
    vt = _alias(vt)
    return _cypher_yield_purpose_over(f"toLower(coalesce({vt}.qualityParams, ''))",
                                      _cypher_norm(f"{vt}.yieldMetric"))


def cypher_purpose_mode_gate(purpose_expr: str, mode: str = MODE_MAIN) -> str:
    """``in_purpose_mode`` over an already computed purpose expression (evaluate it once)."""
    op = "=" if check_mode(mode) == MODE_FORAGE else "<>"
    return f"({purpose_expr} {op} {_cypher_str(PURPOSE_FORAGE)})"


def cypher_in_purpose_mode(vt: str = "vt", mode: str = MODE_MAIN) -> str:
    return cypher_purpose_mode_gate(cypher_yield_purpose(vt), mode)


def cypher_crop_family(vt: str = "vt") -> str:
    """String expression: the crop family of ``vt.cropEppo`` (see ``crop_family``)."""
    vt = _alias(vt)
    code = f"toUpper(trim(coalesce({vt}.cropEppo, '')))"
    whens = " ".join(
        f"WHEN {code} IN {_cypher_list(sorted(codes))} THEN {_cypher_str(family)}"
        for family, codes in CROP_FAMILIES.items()
    )
    return f"(CASE {whens} ELSE {_cypher_str(CROP_FAMILY_OTHER)} END)"


def cypher_numeric_yield_eligible(vt: str = "vt", mode: str = MODE_MAIN) -> str:
    """True when the trial's ``yieldKgHa`` may enter a numeric aggregate of ``mode``."""
    return f"(NOT {cypher_excluded_yield(vt)} AND {cypher_in_purpose_mode(vt, mode)})"


def cypher_grain_yield(vt: str = "vt") -> str:
    """True for a grain yield of a grain-family crop (see ``is_grain_yield``)."""
    grain_or_unknown = _cypher_list((PURPOSE_GRAIN, PURPOSE_UNKNOWN))
    return (f"(NOT {cypher_excluded_yield(vt)} "
            f"AND {cypher_crop_family(vt)} = {_cypher_str(CROP_FAMILY_GRAIN)} "
            f"AND {cypher_yield_purpose(vt)} IN {grain_or_unknown})")


def cypher_forage_basis(vt: str = "vt") -> str:
    """String expression: 'dry_matter' | 'fresh_matter' | 'unknown' (see ``forage_basis``)."""
    vt = _alias(vt)
    explicit = _cypher_norm(f"{vt}.yieldBasis")
    metric = _cypher_norm(f"{vt}.yieldMetric")
    key = (f"[toUpper(trim(coalesce({vt}.source_id, ''))), "
           f"toUpper(trim(coalesce({vt}.cropEppo, ''))), {vt}.year]")
    rule_whens = ""
    for basis in (BASIS_DRY_MATTER, BASIS_FRESH_MATTER):
        keys = [f"[{_cypher_str(r.source_id)}, {_cypher_str(r.crop_eppo)}, {int(r.year)}]"
                for r in FORAGE_BASIS_SOURCE_RULES if r.basis == basis]
        if keys:
            rule_whens += f"WHEN {key} IN [{', '.join(keys)}] THEN {_cypher_str(basis)} "
    return (
        f"(CASE WHEN {explicit} IN {_cypher_list((BASIS_DRY_MATTER, BASIS_FRESH_MATTER))} "
        f"THEN {explicit} "
        f"WHEN {_cypher_any_contains(metric, DRY_MATTER_METRIC_TOKENS, 'ep_tok')} "
        f"THEN {_cypher_str(BASIS_DRY_MATTER)} "
        f"WHEN {_cypher_any_contains(metric, FRESH_MATTER_METRIC_TOKENS, 'ep_tok')} "
        f"THEN {_cypher_str(BASIS_FRESH_MATTER)} "
        f"{rule_whens}"
        f"ELSE {_cypher_str(BASIS_UNKNOWN)} END)"
    )


def cypher_dry_matter_pct(vt: str = "vt") -> str:
    """Numeric expression: DM % read from the ``qualityParams`` JSON text (see ``dry_matter_pct``)."""
    vt = _alias(vt)
    qp = f"toLower(coalesce({vt}.qualityParams, ''))"

    def _read(key: str) -> str:
        after_key = f"split({qp}, {_cypher_str(chr(34) + key + chr(34) + ':')})[1]"
        return f"toFloat(trim(split(split({after_key}, ',')[0], '}}')[0]))"

    first = f"coalesce({', '.join(_read(k) for k in DRY_MATTER_PCT_KEYS)})"
    return f"[ep_dm IN [{first}] WHERE ep_dm > 0 AND ep_dm <= 100][0]"


def cypher_forage_dm_yield(vt: str = "vt") -> str:
    """Numeric expression: kg dry matter/ha (see ``forage_dm_yield``), null when not convertible."""
    vt = _alias(vt)
    return (
        f"(CASE {cypher_forage_basis(vt)} "
        f"WHEN {_cypher_str(BASIS_DRY_MATTER)} THEN toFloat({vt}.yieldKgHa) "
        f"WHEN {_cypher_str(BASIS_FRESH_MATTER)} "
        f"THEN toFloat({vt}.yieldKgHa) * {cypher_dry_matter_pct(vt)} / 100.0 "
        f"ELSE null END)"
    )


def cypher_forage_numeric_evidence(vt: str = "vt") -> str:
    """True when the trial is forage-eligible and has a kg dry-matter value (see ``is_forage_numeric_evidence``)."""
    return (f"({cypher_numeric_yield_eligible(vt, MODE_FORAGE)} "
            f"AND {cypher_forage_dm_yield(vt)} IS NOT NULL)")


def cypher_field_scope(vt: str = "vt") -> str:
    vt = _alias(vt)
    return f"(coalesce(toLower(trim({vt}.aggregationScope)), {_cypher_str(FIELD_SCOPE)}) = {_cypher_str(FIELD_SCOPE)})"


def _cypher_aggregate_site_over(name: str) -> str:
    """The aggregate-site test over an already lowercased, trimmed name expression: the name is
    referenced once per site-name list and once per pattern, so callers pass a variable."""
    return (f"({name} = '' OR {name} IN {_cypher_list(AGGREGATE_SITE_NAMES)} "
            f"OR any(ep_pat IN {_cypher_list(AGGREGATE_SITE_PATTERNS)} WHERE {name} CONTAINS ep_pat))")


def cypher_aggregate_site(ts: str = "ts") -> str:
    ts = _alias(ts)
    return _cypher_aggregate_site_over(_cypher_norm(f"{ts}.name"))


def cypher_field_evidence(vt: str = "vt", ts: str = "ts") -> str:
    """True when the (trial, site) row is field evidence."""
    return (f"({cypher_field_scope(vt)} AND NOT {cypher_aggregate_site(ts)} "
            f"AND NOT {cypher_aggregate_source(vt)})")


def cypher_content_key(vt: str = "vt") -> str:
    """List expression: group by it to count content-identical trials once."""
    vt = _alias(vt)
    blanks = ", ".join(f"coalesce({vt}.{f}, '')" for f in CONTENT_KEY_BLANK_DEFAULT_FIELDS)
    return (
        f"[{vt}.cropEppo, {vt}.varietyNormalized, {vt}.year, {vt}.yieldKgHa, {vt}.yieldNoteS1, "
        f"{blanks}, "
        f"COLLECT {{ MATCH ({vt})-[:TRIAL_AT]->(ep_site:TrialSite) "
        f"RETURN DISTINCT ep_site.name AS ep_name ORDER BY ep_name }}]"
    )


def _cypher_evidence_tier_over(vt: str, site_name: str) -> str:
    return (f"(CASE WHEN ({cypher_field_scope(vt)} AND NOT {_cypher_aggregate_site_over(site_name)} "
            f"AND NOT {cypher_aggregate_source(vt)}) "
            f"THEN {_cypher_str(EVIDENCE_TIER_FIELD)} ELSE {_cypher_str(EVIDENCE_TIER_REGIONAL)} END)")


def cypher_evidence_tier(vt: str = "vt", ts: str = "ts") -> str:
    """String expression: 'field' | 'regional' for the (trial, site) row."""
    vt, ts = _alias(vt), _alias(ts)
    return _cypher_evidence_tier_over(vt, _cypher_norm(f"{ts}.name"))


# ── irrigation regime (rule 8) ───────────────────────────────────────────────
def cypher_irrigation_regime(expr: str) -> str:
    """String expression: ``'secano'`` | ``'regadio'`` | null for a regime value ``expr``."""
    norm = _cypher_norm(expr)
    whens = " ".join(f"WHEN {norm} IN {_cypher_list(spellings)} THEN {_cypher_str(regime)}"
                     for regime, spellings in IRRIGATION_SPELLINGS.items())
    return f"(CASE {whens} END)"


def cypher_irrigation_match(expr: str, target: str = "$irrigation_uri") -> str:
    """Boolean (never null): no regime requested (``target`` null), or ``expr`` names the same
    regime as ``target`` (a URI or a literal, whichever the trial and the request use)."""
    same = f"{cypher_irrigation_regime(expr)} = {cypher_irrigation_regime(target)}"
    return f"({target} IS NULL OR coalesce({same}, false))"


def cypher_irrigation_any(regimes_expr: str, target: str = "$irrigation_uri") -> str:
    """Boolean (never null): no regime requested, or some value of the list ``regimes_expr``
    names the requested regime."""
    same = f"{cypher_irrigation_regime('ep_reg')} = {cypher_irrigation_regime(target)}"
    return (f"({target} IS NULL OR any(ep_reg IN {regimes_expr} WHERE coalesce({same}, false)))")


# ── numeric candidates and tier gates ────────────────────────────────────────
def cypher_numeric_candidate(vt: str = "vt", excluded: str | None = None) -> str:
    """Necessary condition of a policy yield: a kg/ha value that is a measurement (not from an
    excluded source, not derived from a note).

    ``cypher_row_policy`` builds ``ep_y`` (and ``ep_unconv``) from this same predicate, so a
    query can put it in its cheap ``WHERE`` (BSL rows never reach the policy block) and stay
    equal to the policy by construction. ``excluded`` is an already computed boolean (the row
    policy passes its ``ep_excluded`` column); by default the exclusion test is inlined.
    """
    vt = _alias(vt)
    excluded = excluded or cypher_excluded_yield(vt)
    return f"({vt}.yieldKgHa IS NOT NULL AND NOT {excluded})"


def cypher_tier_prefilter(tier: str, vt: str = "vt") -> str:
    """Cheap ``AND`` term (before the row policy) that drops only rows ``cypher_tier_gate`` would
    drop anyway: the regional tier keeps numeric rows, so the excluded-source (BSL) bulk at the
    aggregate containers never reaches the policy. Empty for the field tier."""
    if check_tier(tier) == EVIDENCE_TIER_REGIONAL:
        return f"AND {cypher_numeric_candidate(vt)}"
    return ""


def cypher_numeric_gate_expr(tier: str) -> str:
    """Boolean over the row-policy columns: a (trial, site) row of ``tier`` that carries a policy
    number (``ep_y`` is non-null only for an in-mode record of a source that is not excluded)."""
    return f"ep_tier = {_cypher_str(check_tier(tier))} AND ep_in_mode AND ep_y IS NOT NULL"


def cypher_tier_gate_expr(tier: str, with_other: bool = True) -> str:
    """Boolean over the row-policy columns that selects the rows an answer of ``tier`` reads.

    Regional: numeric rows only. Field: the rows of the purpose mode, plus (``with_other``) the
    other-purpose rows that are counted and never averaged.
    """
    if check_tier(tier) == EVIDENCE_TIER_REGIONAL:
        return cypher_numeric_gate_expr(tier)
    rows = "(ep_in_mode OR ep_other)" if with_other else "ep_in_mode"
    return f"ep_tier = {_cypher_str(tier)} AND {rows}"


def cypher_tier_gate(tier: str, with_other: bool = True) -> str:
    """``WHERE`` clause of ``cypher_tier_gate_expr`` (ends with a newline)."""
    return f"WHERE {cypher_tier_gate_expr(tier, with_other)}\n"


def cypher_numeric_tier_gate(tier: str) -> str:
    """``WHERE`` clause of ``cypher_numeric_gate_expr`` (ends with a newline)."""
    return f"WHERE {cypher_numeric_gate_expr(tier)}\n"


def passes_tier_gate(tier: str, row_tier: str, in_mode: bool, other: bool, y: float | None,
                     with_other: bool = True) -> bool:
    """Python twin of ``cypher_tier_gate_expr`` over one row's policy columns."""
    if row_tier != check_tier(tier):
        return False
    if tier == EVIDENCE_TIER_REGIONAL:
        return in_mode and y is not None
    return in_mode or (with_other and other)


def presence_only_applies(mode: str = MODE_MAIN) -> bool:
    """Presence-only evidence (rule 7) is reported in the main mode only."""
    return check_mode(mode) == MODE_MAIN


def cypher_presence_gate_expr() -> str:
    """Boolean over the row-policy columns: a regional-tier row with an excluded kg/ha (excluded
    source or derived value) within the purpose mode (it proves presence and carries no policy
    number)."""
    return (f"ep_tier = {_cypher_str(EVIDENCE_TIER_REGIONAL)} AND ep_in_mode AND ep_excluded")


def cypher_presence_gate() -> str:
    """``WHERE`` clause of ``cypher_presence_gate_expr`` (ends with a newline)."""
    return f"WHERE {cypher_presence_gate_expr()}\n"


def cypher_presence_prefilter(vt: str = "vt") -> str:
    """Cheap ``AND`` term (before the row policy): only trials with an excluded kg/ha (excluded
    source or derived value) can be presence-only."""
    return f"AND {cypher_excluded_yield(vt)}"


def is_presence_only_evidence(trial: Mapping[str, Any], site_name: str | None,
                              mode: str = MODE_MAIN) -> bool:
    """Python twin of the presence gate over one (trial, site) row (graph property names)."""
    return (
        presence_only_applies(mode)
        and evidence_tier(trial.get("aggregationScope"), site_name, source_id=trial.get("source_id"),
                          data_source=trial.get("dataSource")) == EVIDENCE_TIER_REGIONAL
        and is_excluded_yield(trial)
        and in_purpose_mode(yield_purpose(trial.get("yieldMetric"), trial.get("qualityParams")), mode)
    )


def yield_basis(mode: str = MODE_MAIN) -> str | None:
    """Basis of the numbers a ``mode`` answer reports: forage yields are kg dry matter/ha
    (``policy_yield`` converts them); the main mode reports the record's own unit (None)."""
    return BASIS_DRY_MATTER if check_mode(mode) == MODE_FORAGE else None


ROW_POLICY_COLUMNS: tuple[str, ...] = (
    "ep_tier", "ep_in_mode", "ep_other", "ep_y", "ep_unconv", "ep_excluded",
)


def cypher_row_policy(mode: str = MODE_MAIN, vt: str = "vt", ts: str = "ts",
                      carry: Iterable[str] = ()) -> str:
    """Chained ``WITH`` clauses that classify every (trial, site) row ONCE.

    Place it right after the ``MATCH ... WHERE`` cheap filters. It keeps ``vt``, ``ts`` and
    the ``carry`` variables and adds the columns of ``ROW_POLICY_COLUMNS``:

    - ``ep_tier``: 'field' | 'regional' (rule 6);
    - ``ep_in_mode``: the record's purpose fits ``mode`` (rule 2);
    - ``ep_other``: a trial of the other purpose to count (main mode only, see
      ``is_other_purpose_evidence``), else false;
    - ``ep_y``: the kg/ha the row adds to a numeric aggregate of ``mode`` (``policy_yield``),
      null for an excluded source, an off-mode record, no kg, or a forage yield that cannot be
      converted to dry matter (it is built from ``cypher_numeric_candidate``);
    - ``ep_unconv``: eligible trial with kg but no number (``has_unconverted_kg``);
    - ``ep_excluded``: the trial's kg/ha is excluded from numeric aggregates: excluded source or
      derived value (rule 1, ``is_excluded_yield``).

    The lowercased ``qualityParams`` text, the normalised metric and the lowercased site name
    are each computed once per row, and every key, token and pattern is tested against that
    variable; the dry-matter yield is evaluated only for rows already in forage mode.
    """
    vt, ts = _alias(vt), _alias(ts)
    carried = "".join(f", {_alias(c)}" for c in carry)
    mode = check_mode(mode)
    yield_expr = (cypher_forage_dm_yield(vt) if mode == MODE_FORAGE else f"toFloat({vt}.yieldKgHa)")
    other = "(NOT ep_excluded AND NOT ep_in_mode)" if mode == MODE_MAIN else "false"
    purpose = _cypher_yield_purpose_over("ep_text", "ep_metric")
    candidate = cypher_numeric_candidate(vt, excluded="ep_excluded")
    return (
        f"WITH {vt}, {ts}{carried}, toLower(coalesce({vt}.qualityParams, '')) AS ep_text, "
        f"{_cypher_norm(f'{vt}.yieldMetric')} AS ep_metric, "
        f"{_cypher_norm(f'{ts}.name')} AS ep_site_lc, "
        f"{cypher_excluded_yield(vt)} AS ep_excluded\n"
        f"WITH {vt}, {ts}{carried}, ep_excluded, "
        f"{_cypher_evidence_tier_over(vt, 'ep_site_lc')} AS ep_tier, "
        f"{cypher_purpose_mode_gate(purpose, mode)} AS ep_in_mode\n"
        f"WITH {vt}, {ts}{carried}, ep_tier, ep_in_mode, ep_excluded, {other} AS ep_other, "
        f"CASE WHEN ep_in_mode AND {candidate} THEN {yield_expr} END AS ep_y\n"
        f"WITH {vt}, {ts}{carried}, ep_tier, ep_in_mode, ep_other, ep_y, ep_excluded, "
        f"(ep_in_mode AND {candidate} AND ep_y IS NULL) AS ep_unconv\n"
    )
