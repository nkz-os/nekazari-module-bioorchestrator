"""Quality gate: licence, identity, taxonomy, yield and plausibility checks on a canonical bundle.

``run_gate(bundle, registries, target)`` takes the bundle a contract produced and returns a
:class:`GateReport`: errors (they stop a build), warnings (a human reviews them) and the numbers
behind them. It never touches a database and never changes the bundle. The report serialises to JSON
(:meth:`GateReport.to_json`) and :func:`exit_code` turns it into a process exit status.

Targets
-------
``production``: a source whose licence is not ``allowed`` or ``permission_granted`` in the sources
registry is an ERROR, whatever the contract says (a contract only says how to read a source, never
whether it may be loaded). The licence is checked for every source named anywhere in the bundle
(its own id, its build report, and the documents, studies and units it holds), so relabelling a
bundle of a denied source as a licensed one does not get it past the gate: a row of another source
than the bundle's is an error of its own.
``local-test``: the same condition is a WARNING and the result is marked ``not_publishable``
(``publishable = False``), so an adapter awaiting a permission can be built and tested locally but
its export can never be published.

Errors (see :data:`RULES` for the full list, each with its severity)
-------------------------------------------------------------------
licence, unregistered source, foreign-source rows; crop not a canonical EPPO code; variable or unit
not registered, or not the variable's unit; vocabulary value not a registered one (the row models
do not check vocabularies); a yield without metric or purpose; a metric whose purpose is not the
row's; a value outside a ``reviewed`` range; the same unit key or observation key twice (identical
rows: ``duplicate_key``; different content: ``key_conflict``); a row that points at a document,
study, site, variety or unit the bundle does not hold; an OBSERVED site label that does not resolve
in the sites registry; a site row without a valid ``siteKind`` or with another kind than the
registry; a unit of a variety study without a variety; a year outside 1900 < year <= 2100;
``yield == ordinal score x 1000`` (the old gate's fabrication rule); a bundle built with other
registries than the gate checks against (its range and variety results would be stale).

Warnings
--------
value outside an ``assumption`` range; the range checks that could NOT be made (counted by reason,
a skipped check is no check); unknown yield basis; a unit with no observed site (an explicit gap,
counted and listed by source, never an error); treatment codes in site names; a field site without
coordinates; no year; a variety not in the registry (also written to a review-queue file, never
merged automatically); robust per-study outliers (modified z-score by median absolute deviation,
> 3.5); identical yields across several varieties of one study; a cumulative-looking source field
for a yield; a content duplicate of a unit already in an existing graph (optional, read-only); and
the warnings of the contract engine and of the adapter, passed through.

Decisions that are the gate's and not the engine's: the engine measures (``bundle.report``: range
findings, skipped checks, unresolved vocabulary, unregistered varieties); the gate decides severity.
What the engine already measured is reused, not recomputed.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict

from . import identity
from .contracts import YIELD_VARIABLE, Bundle
from .model import UnitRow
from .registries import Registries, UnknownEntryError, lookup_key

GATE_VERSION = 1

Target = Literal["production", "local-test"]
Severity = Literal["error", "warning"]
TARGETS: tuple[str, ...] = ("production", "local-test")

EXIT_OK = 0
EXIT_ERRORS = 1

# Report size and statistics parameters.
MAX_EXAMPLES = 10
MAX_GROUPS = 50
OUTLIER_MIN_GROUP = 8  # a median and a MAD from fewer values than this say nothing
OUTLIER_Z = 3.5  # Iglewicz and Hoaglin's cut-off for the modified z-score
OUTLIER_MAD_SCALE = 0.6745  # makes the MAD comparable to a standard deviation for normal data
IDENTICAL_MIN_VARIETIES = 3  # two varieties with one yield can be chance; three or more is a copy
YEAR_EXCLUSIVE_MIN = 1900
YEAR_MAX = 2100  # a fixed bound: a gate that reads the clock gives different answers on different days

# A multi-year or cumulative figure named as the source of an annual yield (rule inherited from the old gate).
_CUMULATIVE_KEY = re.compile(r"cumulative|_(?:19|20)\d\d_(?:19|20)\d\d", re.IGNORECASE)
# Experimental treatment codes (letter+digit pairs such as C1N0) at the end or in the middle of a place
# name, set apart by a space, dash, slash or bracket.
_TREATMENT_CODE = re.compile(r"(?:^|[\s\-–—/(,])(?:[A-Z]\d{1,3}){2,}(?=$|[\s)\-–—/,])")


@dataclass(frozen=True)
class Rule:
    severity: Severity
    message: str


RULES: dict[str, Rule] = {
    # envelope and licence
    "registries_mismatch": Rule("error", "the bundle was built with other registries than the gate checks against; "
                                         "its range and variety results are stale"),
    "source_mismatch": Rule("error", "the bundle and its build report name different sources"),
    "foreign_source_row": Rule("error", "a row carries another source than the bundle's (a licence bypass vector)"),
    "unregistered_source": Rule("error", "a source of the bundle is not in the sources registry, so its licence is unknown"),
    "licence_not_permitted": Rule("error", "the source's licence does not allow commercial use (allowed or "
                                           "permission_granted) and the target is production"),
    "licence_not_publishable": Rule("warning", "the source's licence does not allow commercial use: allowed for "
                                               "local-test, the result is not publishable"),
    # keys and references
    "duplicate_key": Rule("error", "the same key appears twice in the bundle with identical content"),
    "key_conflict": Rule("error", "the same key appears twice in the bundle with different content"),
    "orphan_observation": Rule("error", "an observation belongs to a unit that is not in the bundle"),
    "dangling_reference": Rule("error", "a row points at a document, study, site or variety the bundle does not hold"),
    # taxonomy
    "unresolved_eppo": Rule("error", "the crop is not a canonical EPPO code of the crops registry"),
    "unregistered_variable": Rule("error", "an observation names a variable that is not registered"),
    "unregistered_unit": Rule("error", "an observation carries a unit that is not registered"),
    "unit_mismatch": Rule("error", "an observation's unit differs from its variable's unit"),
    "vocabulary_unregistered": Rule("error", "a field holds a value that is not a registered vocabulary value"),
    "unresolved_vocab": Rule("error", "a vocabulary literal the source printed is not in the vocabulary (dropped by the engine)"),
    # yield
    "yield_without_metric": Rule("error", "a yield carries no metric (grain, forage, fresh...)"),
    "yield_without_purpose": Rule("error", "a yield carries no purpose"),
    "yield_purpose_mismatch": Rule("error", "the purpose of a yield is not the purpose of its metric"),
    "unknown_basis": Rule("warning", "a yield has an unknown basis (moisture or matter)"),
    "fabricated_yield": Rule("error", "a yield equals an ordinal score times 1000: fabricated, not measured"),
    "cumulative_yield_source": Rule("warning", "a yield was read from a source field named like a cumulative or "
                                               "multi-year figure"),
    # sites
    "unresolved_site": Rule("error", "a site label was observed but does not resolve in the sites registry"),
    "site_kind_invalid": Rule("error", "a site row has no valid siteKind (field, aggregate, region)"),
    "site_kind_mismatch": Rule("error", "a site row's siteKind differs from the sites registry's"),
    "no_observed_site": Rule("warning", "the source states no site for these units (an explicit gap, not an error)"),
    "treatment_code_in_site_name": Rule("warning", "a site name carries treatment codes (a treatment, not a place)"),
    "field_site_without_coordinates": Rule("warning", "a field site has no coordinates"),
    # units
    "missing_variety": Rule("error", "a unit of a variety study names no variety"),
    "bad_year": Rule("error", "a unit's year is outside 1900 < year <= 2100"),
    "missing_year": Rule("warning", "a unit has no single year"),
    # values
    "value_out_of_reviewed_range": Rule("error", "a value is outside the plausible range an agronomist reviewed"),
    "value_out_of_assumption_range": Rule("warning", "a value is outside an assumed (unreviewed) plausible range"),
    "range_checks_skipped": Rule("warning", "numeric values for which no plausible range applies were NOT range-checked"),
    "outlier_in_study": Rule("warning", "a value is an outlier among the same variable of its study (modified z-score)"),
    "identical_values_across_varieties": Rule("warning", "several varieties of one study have the very same yield"),
    # review
    "unregistered_variety": Rule("warning", "a variety is not in the varieties registry; queued for review, never merged"),
    "content_duplicate_in_graph": Rule("warning", "a unit has the content of a unit already in the graph under another key"),
    # pass-through
    "engine_warning": Rule("warning", "warnings of the contract engine"),
    "adapter_warning": Rule("warning", "warnings of the adapter that read the raw data"),
}


# ═════════════════════════════════════════════════════════════════════════════
# the report
# ═════════════════════════════════════════════════════════════════════════════

class _Out(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Finding(_Out):
    """All the hits of one rule: how many, how they split, and a few examples to look at."""

    rule: str
    severity: Severity
    message: str
    count: int
    groups: dict[str, int] = {}  # breakdown (by source, label, reason, range...), most frequent first
    groups_omitted: int = 0  # groups beyond MAX_GROUPS, not listed
    examples: tuple[str, ...] = ()  # at most MAX_EXAMPLES, in sorted order


class RangeSummary(_Out):
    """The range checks of the run, skipped ones included."""

    evaluated: int
    in_range: int
    out_of_range_reviewed: int
    out_of_range_assumption: int
    skipped: int
    skipped_by_reason: dict[str, int]
    not_applicable: int


class OutlierSummary(_Out):
    """The per-study outlier scan: how many groups could be judged and why the others could not."""

    min_group_size: int
    z_threshold: float
    groups: int
    evaluated: int
    too_small: int
    mad_zero: int
    flagged: int


class ReviewQueueItem(_Out):
    crop_eppo: str
    name: str  # as the source prints it
    units: int


class GateReport(_Out):
    gate_version: int
    target: Target
    source_id: str
    contract_hash: str
    registries_hash: str
    status: Literal["pass", "fail"]
    publishable: bool
    not_publishable_reasons: tuple[str, ...]
    errors: tuple[Finding, ...]
    warnings: tuple[Finding, ...]
    error_counts: dict[str, int]
    warning_counts: dict[str, int]
    rows: dict[str, int]
    range_checks: RangeSummary
    outliers: OutlierSummary
    gaps: dict[str, int]
    varieties_by_status: dict[str, int]
    review_queue: tuple[ReviewQueueItem, ...]
    content_duplicate_check: Literal["run", "not_run"]

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        """Deterministic JSON text: the same bundle and registries always give the same bytes."""
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def exit_code(report: GateReport) -> int:
    """0 when the gate found no error (also for a local-test run that is not publishable), else 1."""
    return EXIT_OK if report.status == "pass" else EXIT_ERRORS


class ExistingGraph(Protocol):
    """Read-only view of the units already in a graph, for the optional content-duplicate check.

    ``unit_identities`` yields ``(unit_key, content_key)`` for every observation unit of the source
    the graph already holds, with ``content_key`` computed by :func:`unit_content_key`. The gate calls
    nothing else on it and never writes.
    """

    def unit_identities(self, source_id: str) -> Iterable[tuple[str, str]]: ...


def unit_content_key(unit: UnitRow) -> str:
    """What a unit says, without where it was printed: the same content in another table or document.

    Built from observed values only (crop, variety, site, season, irrigation, production system,
    factor levels, rootstock, clone, planting year) and the yield, in the normal form of the registries
    (NFC, case-folded, spaces collapsed), so it does not change with a registry. It leaves out the
    source, the document and the row discriminator, which is what makes it a *content* key.
    """
    def norm(value: str | None) -> str | None:
        return lookup_key(value) if value else None

    levels = sorted(
        ([level.factor, level.level, level.unit] for level in unit.factor_levels),
        key=lambda triple: identity.canonical_json({"t": triple}))
    payload = {
        "kind": "unit-content",
        "crop": unit.crop_eppo,
        "variety": norm(unit.raw_variety),
        "site": norm(unit.raw_site),
        "season": norm(unit.raw_season),
        "irrigation": norm(unit.raw_irrigation),
        "production_system": norm(unit.raw_production_system),
        "factors": [[norm(f) if isinstance(f, str) else f for f in triple] for triple in levels],
        "rootstock": norm(unit.rootstock),
        "clone": norm(unit.clone),
        "planting_year": unit.planting_year,
        "yield_kg_ha": unit.yield_kg_ha,
    }
    return hashlib.sha256(identity.canonical_json(payload).encode("utf-8")).hexdigest()


# ═════════════════════════════════════════════════════════════════════════════
# collecting findings
# ═════════════════════════════════════════════════════════════════════════════

class _Collector:
    """Counts hits per rule, with a breakdown and examples, so thousands of rows give one finding."""

    def __init__(self) -> None:
        self._count: Counter[str] = Counter()
        self._groups: dict[str, Counter[str]] = defaultdict(Counter)
        self._examples: dict[str, set[str]] = defaultdict(set)

    def hit(self, rule: str, *, example: str | None = None, group: str | None = None, count: int = 1) -> None:
        if rule not in RULES:
            raise KeyError(f"unknown gate rule {rule!r}")
        self._count[rule] += count
        if group is not None:
            self._groups[rule][group] += count
        if example is not None:
            self._examples[rule].add(example)

    def findings(self) -> tuple[tuple[Finding, ...], tuple[Finding, ...]]:
        errors: list[Finding] = []
        warnings: list[Finding] = []
        for rule in sorted(self._count):
            spec = RULES[rule]
            ordered = sorted(self._groups[rule].items(), key=lambda item: (-item[1], item[0]))
            finding = Finding(
                rule=rule, severity=spec.severity, message=spec.message, count=self._count[rule],
                groups=dict(ordered[:MAX_GROUPS]), groups_omitted=max(0, len(ordered) - MAX_GROUPS),
                examples=tuple(sorted(self._examples[rule])[:MAX_EXAMPLES]))
            (errors if spec.severity == "error" else warnings).append(finding)
        return tuple(errors), tuple(warnings)


# ═════════════════════════════════════════════════════════════════════════════
# the checks
# ═════════════════════════════════════════════════════════════════════════════

def _index(rows: Iterable[Any], key_of: Any, kind: str, c: _Collector) -> dict[str, Any]:
    """Rows by key; a repeated key is a duplicate (same content) or a conflict (different content)."""
    index: dict[str, Any] = {}
    for row in rows:
        key = key_of(row)
        first = index.get(key)
        if first is None:
            index[key] = row
        elif first == row:
            c.hit("duplicate_key", group=kind, example=f"{kind}:{key}")
        else:
            c.hit("key_conflict", group=kind, example=f"{kind}:{key}")
    return index


def _vocab_ok(registries: Registries, kind: str, value: str) -> bool:
    """A normalised vocabulary value: the entry's id or its stored form, never an alias or a literal."""
    entry = registries.vocab_entry(kind, value)
    return entry is not None and value in (entry.id, entry.stored)


def _check_vocab(registries: Registries, c: _Collector, kind: str, value: str | None, where: str) -> None:
    if value is not None and not _vocab_ok(registries, kind, value):
        c.hit("vocabulary_unregistered", group=f"{kind}: {value}", example=where)


def _check_sources(bundle: Bundle, registries: Registries, target: str, c: _Collector) -> tuple[str, ...]:
    """Licence and provenance of every source the bundle names; returns the not-publishable reasons."""
    report = bundle.report
    if report.source_id != bundle.source_id:
        c.hit("source_mismatch", example=f"bundle {bundle.source_id}, report {report.source_id}")
    if report.registries_hash != registries.registries_hash:
        c.hit("registries_mismatch", example=f"built with {report.registries_hash[:12]}, "
                                              f"gate has {registries.registries_hash[:12]}")

    row_sources: Counter[str] = Counter(
        row.source_id for rows in (bundle.documents, bundle.studies, bundle.units) for row in rows)
    unit_sources: Counter[str] = Counter(unit.source_id for unit in bundle.units)
    for source_id, count in sorted(row_sources.items()):
        if source_id != bundle.source_id:
            c.hit("foreign_source_row", group=source_id, count=count, example=f"source:{source_id}")

    reasons: list[str] = []
    for source_id in sorted({bundle.source_id, report.source_id, *row_sources}):
        try:
            source = registries.source(source_id)
        except UnknownEntryError:
            c.hit("unregistered_source", group=source_id, count=max(1, row_sources[source_id]),
                  example=f"source:{source_id}")
            continue
        if source.loadable:
            continue
        verdict = source.licence.commercial_use
        count = max(1, unit_sources[source_id])
        label = f"{source_id} ({verdict})"
        if target == "production":
            c.hit("licence_not_permitted", group=label, count=count, example=f"source:{source_id}")
        else:
            c.hit("licence_not_publishable", group=label, count=count, example=f"source:{source_id}")
            reasons.append(f"{source_id}: commercial use is {verdict}")
    return tuple(reasons)


def _check_taxonomy_rows(bundle: Bundle, registries: Registries, c: _Collector) -> dict[str, dict[str, Any]]:
    """Documents, studies, sites and varieties; returns their indexes by key."""
    documents = _index(bundle.documents, identity.document_key, "document", c)
    studies = _index(bundle.studies, identity.study_key, "study", c)
    sites = _index(bundle.sites, identity.site_key, "site", c)
    varieties = _index(bundle.varieties, identity.variety_key, "variety", c)

    canonical_crops: dict[str, bool] = {}

    def eppo_ok(code: str) -> bool:
        if code not in canonical_crops:
            crop = registries.crop(code)
            canonical_crops[code] = crop is not None and crop.eppo == code
        return canonical_crops[code]

    for key, study in studies.items():
        if study.crop_eppo is not None and not eppo_ok(study.crop_eppo):
            c.hit("unresolved_eppo", group=study.crop_eppo, example=f"study:{key}")
        _check_vocab(registries, c, "study_type", study.study_type, f"study:{key}")
    for key, variety in varieties.items():
        if not eppo_ok(variety.crop_eppo):
            c.hit("unresolved_eppo", group=variety.crop_eppo, example=f"variety:{key}")

    kinds = {entry.id for entry in registries.vocab_entries("site_kind")}
    for site_id, site in sites.items():
        registered = registries.site(site_id)
        if registered is None or registered.id != site_id:
            c.hit("unresolved_site", group=site_id, example=f"site:{site_id}")
        kind = site.site_kind
        if not isinstance(kind, str) or kind not in kinds:
            c.hit("site_kind_invalid", group=str(kind), example=f"site:{site_id}")
        elif registered is not None and registered.site_kind != kind:
            c.hit("site_kind_mismatch", group=f"{kind} != {registered.site_kind}", example=f"site:{site_id}")
        if site.site_kind == "field" and site.latitude is None:
            c.hit("field_site_without_coordinates", example=f"site:{site_id}")
        if _TREATMENT_CODE.search(site.name):
            c.hit("treatment_code_in_site_name", group=site.name, example=f"site:{site_id}")
    return {"documents": documents, "studies": studies, "sites": sites, "varieties": varieties}


def _check_yield_context(
    registries: Registries, c: _Collector, *, metric: str | None, purpose: str | None, where: str, kind: str,
) -> None:
    """The yield rules shared by a unit's derived copy and an observation: metric, purpose, coherence."""
    if metric is None:
        c.hit("yield_without_metric", group=kind, example=where)
    else:
        _check_vocab(registries, c, "yield_metric", metric, where)
    if purpose is None:
        c.hit("yield_without_purpose", group=kind, example=where)
    else:
        _check_vocab(registries, c, "purpose", purpose, where)
    if metric is not None and purpose is not None:
        entry = registries.vocab_entry("yield_metric", metric)
        if entry is not None and entry.purpose is not None and entry.purpose != purpose:
            c.hit("yield_purpose_mismatch", group=f"{metric} gives {entry.purpose}, row says {purpose}", example=where)


def _check_units(
    bundle: Bundle, registries: Registries, indexes: Mapping[str, Mapping[str, Any]], c: _Collector,
) -> dict[str, UnitRow]:
    units = _index(bundle.units, identity.unit_key, "unit", c)
    canonical_crops: dict[str, bool] = {}
    for key, unit in units.items():
        where = f"unit:{key}"
        if unit.crop_eppo not in canonical_crops:
            crop = registries.crop(unit.crop_eppo)
            canonical_crops[unit.crop_eppo] = crop is not None and crop.eppo == unit.crop_eppo
        if not canonical_crops[unit.crop_eppo]:
            c.hit("unresolved_eppo", group=unit.crop_eppo, example=where)

        # links the bundle must be able to follow
        if unit.document_key not in indexes["documents"]:
            c.hit("dangling_reference", group="unit.document_key", example=where)
        if unit.study_key is not None and unit.study_key not in indexes["studies"]:
            c.hit("dangling_reference", group="unit.study_key", example=where)
        if unit.variety_key is not None and unit.variety_key not in indexes["varieties"]:
            c.hit("dangling_reference", group="unit.variety_key", example=where)
        if unit.site_key is not None and unit.site_key not in indexes["sites"]:
            c.hit("dangling_reference", group="unit.site_key", example=where)

        # sites: an observed label that does not resolve is an error; none observed is a counted gap
        if unit.raw_site is not None and unit.site_key is None:
            c.hit("unresolved_site", group=unit.raw_site, example=where)
        elif unit.raw_site is None and unit.site_key is None:
            c.hit("no_observed_site", group=unit.source_id, example=where)
        if unit.raw_site is not None and _TREATMENT_CODE.search(unit.raw_site):
            c.hit("treatment_code_in_site_name", group=unit.raw_site, example=where)

        # variety and year
        study = indexes["studies"].get(unit.study_key) if unit.study_key is not None else None
        if study is not None and study.study_type == "variety" and unit.raw_variety is None:
            c.hit("missing_variety", example=where)
        if unit.year is None:
            c.hit("missing_year", example=where)
        elif not YEAR_EXCLUSIVE_MIN < unit.year <= YEAR_MAX:
            c.hit("bad_year", group=str(unit.year), example=where)

        # vocabularies the row models do not check
        _check_vocab(registries, c, "irrigation", unit.irrigation_regime, where)
        _check_vocab(registries, c, "production_system", unit.production_system, where)
        _check_vocab(registries, c, "purpose", unit.purpose, where)
        _check_vocab(registries, c, "yield_basis", unit.yield_basis, where)

        # the derived yield copy
        if unit.yield_kg_ha is not None:
            _check_yield_context(registries, c, metric=unit.yield_metric, purpose=unit.purpose,
                                 where=where, kind="unit")
    return units


def _check_observations(
    bundle: Bundle, registries: Registries, units: Mapping[str, UnitRow], c: _Collector,
) -> tuple[dict[tuple[Any, ...], list[tuple[float, str]]], dict[tuple[Any, ...], dict[str, set[float]]]]:
    """Per-observation rules; returns the comparable groups for the outlier and identical-value scans."""
    observations = _index(bundle.observations, identity.obs_key, "observation", c)
    variables: dict[str, Any] = {}
    unit_codes: dict[str, str | None] = {}

    def unit_code(printed: str | None) -> str | None:
        if printed is None:
            return None
        if printed not in unit_codes:
            try:
                unit_codes[printed] = registries.unit(printed).code
            except UnknownEntryError:
                unit_codes[printed] = "\0unregistered"
        return unit_codes[printed]

    comparable: dict[tuple[Any, ...], list[tuple[float, str]]] = defaultdict(list)
    by_variety: dict[tuple[Any, ...], dict[str, set[float]]] = defaultdict(dict)
    yields: dict[str, list[float]] = defaultdict(list)
    ordinals: dict[str, list[tuple[str, float]]] = defaultdict(list)

    for key, obs in observations.items():
        where = f"obs:{key}"
        unit = units.get(obs.unit_key)
        if unit is None:
            c.hit("orphan_observation", example=where)

        if obs.variable_id not in variables:
            try:
                variables[obs.variable_id] = registries.variable(obs.variable_id)
            except UnknownEntryError:
                variables[obs.variable_id] = None
        variable = variables[obs.variable_id]
        if variable is None:
            c.hit("unregistered_variable", group=obs.variable_id, example=where)
        code = unit_code(obs.unit)
        if code == "\0unregistered":
            c.hit("unregistered_unit", group=str(obs.unit), example=where)
        elif variable is not None and code != unit_code(variable.unit):
            c.hit("unit_mismatch", group=f"{obs.variable_id}: {obs.unit} (registry: {variable.unit})", example=where)

        _check_vocab(registries, c, "yield_basis", obs.basis, where)
        _check_vocab(registries, c, "purpose", obs.purpose, where)

        is_yield = obs.variable_id == YIELD_VARIABLE or obs.metric is not None
        if is_yield:
            _check_yield_context(registries, c, metric=obs.metric, purpose=obs.purpose, where=where,
                                 kind="observation")
            if obs.basis is None or obs.basis == "unknown":
                c.hit("unknown_basis", group=str(obs.basis), example=where)
            if obs.raw_key and _CUMULATIVE_KEY.search(obs.raw_key):
                c.hit("cumulative_yield_source", group=obs.raw_key, example=where)
            if obs.value is not None:
                yields[obs.unit_key].append(obs.value)
        elif variable is not None and variable.scale == "ordinal" and obs.value is not None:
            ordinals[obs.unit_key].append((obs.variable_id, obs.value))

        if unit is None or variable is None or obs.value is None or variable.scale not in ("ratio", "percent"):
            continue
        if unit.study_key is None:
            continue
        group_key = (unit.study_key, obs.variable_id, obs.stage, obs.qualifier, obs.unit, obs.metric,
                     obs.basis, obs.moisture_pct)
        comparable[group_key].append((obs.value, key))
        if is_yield and (unit.variety_key or unit.raw_variety) is not None:
            slot = (unit.study_key, obs.variable_id, obs.stage, obs.qualifier, unit.raw_site, unit.raw_season,
                    unit.raw_irrigation, unit.raw_production_system, unit.factor_levels, obs.basis,
                    obs.moisture_pct)
            by_variety[slot].setdefault(unit.variety_key or unit.raw_variety or "", set()).add(obs.value)

    for unit_key, values in yields.items():
        for variable_id, score in ordinals.get(unit_key, ()):
            if score != 0 and any(value == score * 1000 for value in values):
                c.hit("fabricated_yield", group=variable_id, example=f"unit:{unit_key}")
    return comparable, by_variety


def _check_ranges(bundle: Bundle, c: _Collector) -> RangeSummary:
    """The range findings the engine measured, given their severity; skipped checks counted by reason."""
    checks = bundle.report.range_checks
    reviewed = assumption = 0
    for finding in checks.out_of_range:
        detail = f"obs:{finding.obs_key} {finding.variable_id}={finding.value:g} outside " \
                 f"[{finding.range_min:g}, {finding.range_max:g}]"
        if finding.range_status == "reviewed":
            reviewed += 1
            c.hit("value_out_of_reviewed_range", group=finding.range_id, example=detail)
        else:
            assumption += 1
            c.hit("value_out_of_assumption_range", group=finding.range_id, example=detail)
    listed = 0
    for reason, count in sorted(checks.skipped_by_reason.items()):
        c.hit("range_checks_skipped", group=reason, count=count)
        listed += count
    if checks.skipped > listed:
        c.hit("range_checks_skipped", group="unspecified", count=checks.skipped - listed)
    return RangeSummary(
        evaluated=checks.evaluated, in_range=checks.in_range, out_of_range_reviewed=reviewed,
        out_of_range_assumption=assumption, skipped=checks.skipped,
        skipped_by_reason=dict(sorted(checks.skipped_by_reason.items())), not_applicable=checks.not_applicable)


def _check_outliers(comparable: Mapping[tuple[Any, ...], Sequence[tuple[float, str]]], c: _Collector) -> OutlierSummary:
    """Robust per-study outliers: modified z-score from the median and the MAD, as a warning only."""
    evaluated = too_small = mad_zero = flagged = 0
    for group_key in sorted(comparable, key=repr):
        values = comparable[group_key]
        if len(values) < OUTLIER_MIN_GROUP:
            too_small += 1
            continue
        numbers = [value for value, _ in values]
        median = statistics.median(numbers)
        mad = statistics.median(abs(value - median) for value in numbers)
        if mad == 0:
            mad_zero += 1
            continue
        evaluated += 1
        for value, key in values:
            z = OUTLIER_MAD_SCALE * (value - median) / mad
            if abs(z) > OUTLIER_Z:
                flagged += 1
                c.hit("outlier_in_study", group=str(group_key[1]), example=f"obs:{key} value={value:g} z={z:.1f}")
    return OutlierSummary(
        min_group_size=OUTLIER_MIN_GROUP, z_threshold=OUTLIER_Z, groups=len(comparable), evaluated=evaluated,
        too_small=too_small, mad_zero=mad_zero, flagged=flagged)


def _check_identical(by_variety: Mapping[tuple[Any, ...], Mapping[str, set[float]]], c: _Collector) -> None:
    for slot in sorted(by_variety, key=repr):
        varieties = by_variety[slot]
        values = {value for seen in varieties.values() for value in seen}
        if len(varieties) >= IDENTICAL_MIN_VARIETIES and len(values) == 1:
            c.hit("identical_values_across_varieties", group=str(slot[1]),
                  count=1, example=f"study:{slot[0]} {len(varieties)} varieties, value {next(iter(values)):g}")


def _review_queue(bundle: Bundle, c: _Collector) -> tuple[ReviewQueueItem, ...]:
    items: list[ReviewQueueItem] = []
    for entry, units in sorted(bundle.report.unregistered_varieties.items()):
        eppo, _, name = entry.partition(":")
        items.append(ReviewQueueItem(crop_eppo=eppo, name=name, units=units))
        c.hit("unregistered_variety", group=eppo, count=units, example=entry)
    return tuple(items)


def _check_graph(bundle: Bundle, existing: ExistingGraph, c: _Collector) -> None:
    known: dict[str, set[str]] = defaultdict(set)
    for unit_key, content_key in existing.unit_identities(bundle.source_id):
        known[content_key].add(unit_key)
    for unit in bundle.units:
        key = identity.unit_key(unit)
        others = known.get(unit_content_key(unit), set()) - {key}
        if others:
            c.hit("content_duplicate_in_graph", group=unit.source_id, example=f"unit:{key}")


def _pass_through(bundle: Bundle, adapter_warnings: Sequence[Any], c: _Collector) -> None:
    for message in bundle.report.warnings:
        c.hit("engine_warning", example=message)
    for warning in adapter_warnings:
        c.hit("adapter_warning", group=warning.code, count=warning.count,
              example=f"{warning.code}: {warning.message}")
    for literal, count in sorted(bundle.report.unresolved_vocab.items()):
        c.hit("unresolved_vocab", group=literal, count=count, example=literal)


def run_gate(
    bundle: Bundle,
    registries: Registries,
    target: Target,
    *,
    existing: ExistingGraph | None = None,
    adapter_warnings: Sequence[Any] = (),
) -> GateReport:
    """Check a bundle against the registries for ``target`` and report; the bundle is never changed.

    ``existing`` (optional) is a read-only view of a graph for the content-duplicate warning;
    ``adapter_warnings`` are the ``AdapterResult.warnings`` of the reading, passed through.
    """
    if target not in TARGETS:
        raise ValueError(f"unknown gate target {target!r}; expected one of {TARGETS}")
    c = _Collector()
    reasons = _check_sources(bundle, registries, target, c)
    indexes = _check_taxonomy_rows(bundle, registries, c)
    units = _check_units(bundle, registries, indexes, c)
    comparable, by_variety = _check_observations(bundle, registries, units, c)
    range_summary = _check_ranges(bundle, c)
    outliers = _check_outliers(comparable, c)
    _check_identical(by_variety, c)
    queue = _review_queue(bundle, c)
    if existing is not None:
        _check_graph(bundle, existing, c)
    _pass_through(bundle, adapter_warnings, c)

    errors, warnings = c.findings()
    return GateReport(
        gate_version=GATE_VERSION, target=target, source_id=bundle.source_id,
        contract_hash=bundle.report.contract_hash, registries_hash=registries.registries_hash,
        status="fail" if errors else "pass", publishable=not errors and not reasons,
        not_publishable_reasons=reasons, errors=errors, warnings=warnings,
        error_counts={f.rule: f.count for f in errors}, warning_counts={f.rule: f.count for f in warnings},
        rows={"documents": len(bundle.documents), "studies": len(bundle.studies), "sites": len(bundle.sites),
              "varieties": len(bundle.varieties), "units": len(bundle.units),
              "observations": len(bundle.observations)},
        range_checks=range_summary, outliers=outliers, gaps=dict(bundle.report.gaps),
        varieties_by_status=dict(sorted(Counter(v.status for v in bundle.varieties).items())),
        review_queue=queue, content_duplicate_check="run" if existing is not None else "not_run")


# ═════════════════════════════════════════════════════════════════════════════
# files
# ═════════════════════════════════════════════════════════════════════════════

def _write_atomic(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)
    return path


def write_report(report: GateReport, directory: str | Path) -> Path:
    """Write the JSON report as ``gate-<source>-<target>.json`` under ``directory``."""
    return _write_atomic(Path(directory) / f"gate-{report.source_id}-{report.target}.json", report.to_json())


def write_review_queue(report: GateReport, directory: str | Path) -> Path:
    """Write the variety review queue as ``review-queue-<source>.json`` under ``directory``.

    Always written, empty when nothing is queued, so a missing file never means "nothing to review".
    """
    document = {
        "source_id": report.source_id,
        "registries_hash": report.registries_hash,
        "items": [item.model_dump(mode="json") for item in report.review_queue],
    }
    text = json.dumps(document, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    return _write_atomic(Path(directory) / f"review-queue-{report.source_id}.json", text)


__all__ = [
    "EXIT_ERRORS",
    "EXIT_OK",
    "GATE_VERSION",
    "RULES",
    "TARGETS",
    "ExistingGraph",
    "Finding",
    "GateReport",
    "OutlierSummary",
    "RangeSummary",
    "ReviewQueueItem",
    "Rule",
    "exit_code",
    "run_gate",
    "unit_content_key",
    "write_report",
    "write_review_queue",
]
