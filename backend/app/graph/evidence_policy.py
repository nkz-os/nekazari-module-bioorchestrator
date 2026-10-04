"""Evidence policy — the single source of truth for what counts as numeric yield evidence.

Every reader of trial yields (extrapolation, medians, recommend, the evidence page and
the backtest) classifies trials through this module, either with the Python
classifiers or with the Cypher fragment builders below. Both implement the same rules
and the tests keep them in agreement; no rule (source, purpose, basis, site pattern,
dedup key) is written anywhere else.

Rules (owner decisions 2026-10-04):

1. **Source.** BSL ``yieldKgHa`` values are the 1–9 note times a per-crop constant, not
   measurements. Trials from an excluded source never contribute kg/ha to a numeric
   aggregate (expected yield, interval, medians, relative yield, backtest); they may
   still count as presence evidence. Matched on ``source_id`` and ``dataSource``
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
   ones.
4. **Site kind.** A (trial, site) row is *field* evidence only when the trial's
   ``aggregationScope`` is ``'site'`` (or absent) and the site is not an aggregate
   pseudo-site (national/regional registries, "average of N locations", zones, unknown
   or empty names). Aggregate evidence is shown apart, never mixed with field evidence.
5. **Content dedup.** Trials with identical observed content — crop, normalized variety,
   year, yield (kg/ha and note), irrigation regime, production system and the set of
   linked site names — are one observation, whatever their ``mergeKey``.

Explicit properties written by ingestion (``yieldBasis`` today; ``siteKind`` and a
grain/forage ``yieldMetric`` vocabulary later) are read first, here; callers do not change.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from typing import Any, NamedTuple

from app.ingestion.trial_site_geo import AGGREGATE_PATTERNS, is_aggregate_site_name

# Bump when a rule changes, so backtest baselines name the policy they were measured under.
POLICY_VERSION = "2026-10-04.1"

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


def _check_mode(mode: str) -> str:
    if mode not in PURPOSE_MODES:
        raise ValueError(f"unknown purpose mode: {mode!r}")
    return mode


def in_purpose_mode(purpose: str, mode: str = MODE_MAIN) -> bool:
    """Main mode takes every purpose but forage; forage mode takes forage only."""
    if _check_mode(mode) == MODE_FORAGE:
        return purpose == PURPOSE_FORAGE
    return purpose != PURPOSE_FORAGE


def crop_family(crop_eppo: str | None) -> str:
    code = crop_eppo.strip().upper() if isinstance(crop_eppo, str) else ""
    for family, codes in CROP_FAMILIES.items():
        if code in codes:
            return family
    return CROP_FAMILY_OTHER


def is_numeric_yield_eligible(trial: Mapping[str, Any], mode: str = MODE_MAIN) -> bool:
    """May this trial's ``yieldKgHa`` enter a numeric aggregate of ``mode``? (graph property names)

    Forage mode additionally needs a known basis before averaging (``forage_basis``).
    """
    if is_excluded_source(trial.get("source_id"), trial.get("dataSource")):
        return False
    return in_purpose_mode(yield_purpose(trial.get("yieldMetric"), trial.get("qualityParams")), mode)


def is_grain_yield(trial: Mapping[str, Any]) -> bool:
    """Grain yield of a grain-family crop (the backtest's ground truth)."""
    return (
        not is_excluded_source(trial.get("source_id"), trial.get("dataSource"))
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


def is_field_evidence(aggregation_scope: str | None, site_name: str | None) -> bool:
    return is_field_scope(aggregation_scope) and not is_aggregate_site(site_name)


def content_key(trial: Mapping[str, Any], site_names: Iterable[str]) -> tuple:
    """Dedup key of one trial (graph property names); equal keys = one observation."""
    return (
        trial.get("cropEppo"),
        trial.get("varietyNormalized"),
        trial.get("year"),
        trial.get("yieldKgHa"),
        trial.get("yieldNoteS1"),
        trial.get("irrigationRegime") or "",
        trial.get("productionSystem") or "",
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


def _cypher_any_contains(expr: str, tokens: Iterable[str], var: str) -> str:
    return f"any({var} IN {_cypher_list(tokens)} WHERE {expr} CONTAINS {var})"


def cypher_forage_indicators(vt: str = "vt") -> str:
    vt = _alias(vt)
    keys = (f'"{k}"' for k in sorted(FORAGE_QUALITY_KEYS))
    return _cypher_any_contains(f"toLower(coalesce({vt}.qualityParams, ''))", keys, "ep_key")


def cypher_yield_purpose(vt: str = "vt") -> str:
    """String expression: 'grain' | 'forage' | 'fresh' | 'unknown' (see ``yield_purpose``)."""
    vt = _alias(vt)
    metric = _cypher_norm(f"{vt}.yieldMetric")
    return (
        f"(CASE WHEN {cypher_forage_indicators(vt)} "
        f"OR {_cypher_any_contains(metric, FORAGE_METRIC_TOKENS, 'ep_tok')} "
        f"THEN {_cypher_str(PURPOSE_FORAGE)} "
        f"WHEN {_cypher_any_contains(metric, GRAIN_METRIC_TOKENS, 'ep_tok')} "
        f"THEN {_cypher_str(PURPOSE_GRAIN)} "
        f"WHEN {_cypher_any_contains(metric, FRESH_METRIC_TOKENS, 'ep_tok')} "
        f"THEN {_cypher_str(PURPOSE_FRESH)} "
        f"ELSE {_cypher_str(PURPOSE_UNKNOWN)} END)"
    )


def cypher_in_purpose_mode(vt: str = "vt", mode: str = MODE_MAIN) -> str:
    op = "=" if _check_mode(mode) == MODE_FORAGE else "<>"
    return f"({cypher_yield_purpose(vt)} {op} {_cypher_str(PURPOSE_FORAGE)})"


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
    return f"(NOT {cypher_excluded_source(vt)} AND {cypher_in_purpose_mode(vt, mode)})"


def cypher_grain_yield(vt: str = "vt") -> str:
    """True for a grain yield of a grain-family crop (see ``is_grain_yield``)."""
    grain_or_unknown = _cypher_list((PURPOSE_GRAIN, PURPOSE_UNKNOWN))
    return (f"(NOT {cypher_excluded_source(vt)} "
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


def cypher_field_scope(vt: str = "vt") -> str:
    vt = _alias(vt)
    return f"(coalesce(toLower(trim({vt}.aggregationScope)), {_cypher_str(FIELD_SCOPE)}) = {_cypher_str(FIELD_SCOPE)})"


def cypher_aggregate_site(ts: str = "ts") -> str:
    ts = _alias(ts)
    name = _cypher_norm(f"{ts}.name")
    return (f"({name} = '' OR {name} IN {_cypher_list(AGGREGATE_SITE_NAMES)} "
            f"OR any(ep_pat IN {_cypher_list(AGGREGATE_SITE_PATTERNS)} WHERE {name} CONTAINS ep_pat))")


def cypher_field_evidence(vt: str = "vt", ts: str = "ts") -> str:
    """True when the (trial, site) row is field evidence."""
    return f"({cypher_field_scope(vt)} AND NOT {cypher_aggregate_site(ts)})"


def cypher_content_key(vt: str = "vt") -> str:
    """List expression: group by it to count content-identical trials once."""
    vt = _alias(vt)
    return (
        f"[{vt}.cropEppo, {vt}.varietyNormalized, {vt}.year, {vt}.yieldKgHa, {vt}.yieldNoteS1, "
        f"coalesce({vt}.irrigationRegime, ''), coalesce({vt}.productionSystem, ''), "
        f"COLLECT {{ MATCH ({vt})-[:TRIAL_AT]->(ep_site:TrialSite) "
        f"RETURN DISTINCT ep_site.name AS ep_name ORDER BY ep_name }}]"
    )
