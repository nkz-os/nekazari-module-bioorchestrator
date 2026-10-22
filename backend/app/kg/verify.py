"""Acceptance checks of a build (spec section 7, plan task 9): read the graph and say pass or fail.

``verify`` only reads (``MATCH ... RETURN``, one small aggregate per check, so it is safe against a
graph served by a small heap). It checks, per build:

1. counts: the bundle equals the contract's ``expected``, and the graph holds what the bundle holds
   (units, observations per variable, sites, documents, studies, varieties);
2. no duplicate natural keys, and no group of units with the same content *and the same observations* under
   different keys (units that share crop, variety, site and season but carry different traits are distinct);
3. no gate error, and nothing unmapped: no observation without a registered variable, no unresolved
   site or vocabulary literal left in a bundle report;
4. orphans below the declared thresholds (units without a site, sites without a unit, documents
   without a unit);
5. every yield (on the unit and as an observation) carries its metric;
6. every source in the graph, and every source a unit names, has a permitted licence.

Regional aggregates are reported apart from field sites (``SitesView``): zone aggregates and the
unlabelled-table aggregate never share a number with a field plot.

Nothing here decides anything: a check is ``pass`` or ``fail`` with the numbers behind it.
"""
from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from neo4j import READ_ACCESS

from . import identity
from .contracts import YIELD_VARIABLE, Bundle, Contract
from .existing_graph import Neo4jExistingGraph
from .gate import GateReport
from .registries import LOADABLE_COMMERCIAL_USE, Registries

logger = logging.getLogger(__name__)

OBSERVATION_CHUNK = 500
EXAMPLE_LIMIT = 10

# label -> the property that is its natural key (the identity of a node)
NATURAL_KEYS: dict[str, str] = {
    "Source": "sourceId",
    "Crop": "eppo",
    "Variable": "variableId",
    "TrialSite": "siteKey",
    "ArticleSource": "documentKey",
    "Study": "studyKey",
    "Variety": "varietyKey",
    "ObservationUnit": "unitKey",
    "Observation": "obsKey",
}


# Labels a restored copy of the served graph shares with the legacy ingesters (whose nodes may lack the F1 key).
LEGACY_SHARED_LABELS = frozenset({"ArticleSource", "TrialSite"})


@dataclass(frozen=True)
class Thresholds:
    """What a build may leave dangling. Declared, not inferred; the default is zero everywhere."""

    max_units_without_site_ratio: float = 0.0
    max_sites_without_units: int = 0
    max_documents_without_units: int = 0


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: dict[str, Any]
    problems: tuple[str, ...] = ()


@dataclass(frozen=True)
class SitesView:
    """Sites and the units on them, field plots apart from regional aggregates."""

    field_sites: int
    field_units: int
    zone_aggregate_sites: int
    zone_aggregate_units: int
    unlabelled_aggregate_sites: int
    unlabelled_aggregate_units: int
    units_without_site: int

    def to_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class VerifyReport:
    checks: tuple[Check, ...]
    sites: dict[str, SitesView] = field(default_factory=dict)  # per source id, plus "ALL"

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks)

    @property
    def problems(self) -> list[str]:
        return [f"{check.name}: {problem}" for check in self.checks for problem in check.problems]

    def check(self, name: str) -> Check:
        for check in self.checks:
            if check.name == name:
                return check
        raise KeyError(name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "checks": [{"name": c.name, "ok": c.ok, "detail": c.detail, "problems": list(c.problems)}
                       for c in self.checks],
            "sites": {source: view.to_dict() for source, view in sorted(self.sites.items())},
        }


async def _rows(driver: Any, database: str | None, cypher: str, **params: Any) -> list[dict[str, Any]]:
    async with driver.session(database=database, default_access_mode=READ_ACCESS) as session:
        result = await session.run(cypher, **params)
        return [dict(record) async for record in result]


async def _scalar(driver: Any, database: str | None, cypher: str, **params: Any) -> int:
    rows = await _rows(driver, database, cypher, **params)
    return int(rows[0]["c"]) if rows else 0


# ═════════════════════════════════════════════════════════════════════════════
# 1. counts
# ═════════════════════════════════════════════════════════════════════════════

async def _check_counts(
    driver: Any, database: str | None, bundles: Sequence[Bundle], contracts: Mapping[str, Contract],
) -> Check:
    problems: list[str] = []
    detail: dict[str, Any] = {}
    for bundle in bundles:
        sid = bundle.source_id
        contract = contracts.get(sid)
        want = {"units": len(bundle.units), "observations": len(bundle.observations), "sites": len(bundle.sites)}
        if contract is None:
            problems.append(f"{sid}: no contract given, expected counts cannot be checked")
        else:
            for name, value in contract.expected.model_dump().items():
                if want[name] != value:
                    problems.append(f"{sid}: contract expects {value} {name}, bundle has {want[name]}")
        units = await _scalar(driver, database, "MATCH (u:ObservationUnit {source_id: $s}) RETURN count(u) AS c", s=sid)
        obs = await _scalar(
            driver, database,
            "MATCH (o:Observation)-[:ON_UNIT]->(u:ObservationUnit {source_id: $s}) RETURN count(o) AS c", s=sid)
        sites = await _scalar(driver, database, "MATCH (t:TrialSite) WHERE $s IN t.sourceIds RETURN count(t) AS c", s=sid)
        docs = await _scalar(driver, database, "MATCH (d:ArticleSource {source_id: $s}) RETURN count(d) AS c", s=sid)
        studies = await _scalar(driver, database, "MATCH (d:Study {source_id: $s}) RETURN count(d) AS c", s=sid)
        graph = {"units": units, "observations": obs, "sites": sites, "documents": docs, "studies": studies}
        bundle_counts = {**want, "documents": len(bundle.documents), "studies": len(bundle.studies)}
        for name, value in bundle_counts.items():
            if graph[name] != value:
                problems.append(f"{sid}: graph holds {graph[name]} {name}, bundle has {value}")
        by_variable = {r["v"]: r["c"] for r in await _rows(
            driver, database,
            "MATCH (o:Observation)-[:ON_UNIT]->(u:ObservationUnit {source_id: $s}) "
            "RETURN o.variableId AS v, count(o) AS c ORDER BY v", s=sid)}
        expected_by_variable = dict(sorted(Counter(o.variable_id for o in bundle.observations).items()))
        if by_variable != expected_by_variable:
            for variable in sorted(set(by_variable) | set(expected_by_variable)):
                if by_variable.get(variable) != expected_by_variable.get(variable):
                    problems.append(f"{sid}: variable {variable}: graph {by_variable.get(variable, 0)}, "
                                    f"bundle {expected_by_variable.get(variable, 0)}")
        detail[sid] = {"bundle": bundle_counts, "graph": graph, "observations_by_variable": by_variable}
    return Check("counts", not problems, detail, tuple(problems))


# ═════════════════════════════════════════════════════════════════════════════
# 2. duplicates
# ═════════════════════════════════════════════════════════════════════════════

async def _observation_signatures(driver: Any, database: str | None, unit_keys: Sequence[str]) -> dict[str, tuple[str, ...]]:
    """unit key -> its observations as sorted canonical texts (variable, qualifier, stage, value, unit, basis)."""
    out: dict[str, list[str]] = {key: [] for key in unit_keys}
    for start in range(0, len(unit_keys), OBSERVATION_CHUNK):
        rows = await _rows(
            driver, database,
            "MATCH (o:Observation)-[:ON_UNIT]->(u:ObservationUnit) WHERE u.unitKey IN $keys "
            "RETURN u.unitKey AS key, o.variableId AS v, o.qualifier AS q, o.stage AS st, o.value AS val, "
            "o.valueText AS txt, o.unit AS unit, o.basis AS basis, o.metric AS metric",
            keys=list(unit_keys[start:start + OBSERVATION_CHUNK]))
        for r in rows:
            out[r["key"]].append(identity.canonical_json({k: r[k] for k in ("v", "q", "st", "val", "txt", "unit", "basis", "metric")}))
    return {key: tuple(sorted(texts)) for key, texts in out.items()}


async def _check_duplicates(
    driver: Any, sync_driver: Any, database: str | None, bundles: Sequence[Bundle],
) -> Check:
    problems: list[str] = []
    key_groups: dict[str, int] = {}
    sources = [b.source_id for b in bundles]
    for label, key in NATURAL_KEYS.items():
        # A restored copy of the served graph holds legacy ArticleSource / TrialSite nodes of other sources, some
        # without the F1 key: a missing key is a defect only on the nodes of the sources this build wrote.
        scope = (f"WHERE n.`{key}` IS NOT NULL OR n.source_id IN $sources "
                 "OR any(x IN coalesce(n.sourceIds, []) WHERE x IN $sources) ") \
            if label in LEGACY_SHARED_LABELS else ""
        rows = await _rows(
            driver, database,
            f"MATCH (n:`{label}`) {scope}WITH n.`{key}` AS k, count(*) AS c WHERE c > 1 OR k IS NULL "
            "RETURN count(*) AS groups, coalesce(sum(c), 0) AS nodes", sources=sources)
        groups = int(rows[0]["groups"])
        key_groups[label] = groups
        if groups:
            problems.append(f"{label}: {groups} duplicate or missing {key} group(s) ({rows[0]['nodes']} nodes)")
    content_groups: dict[str, int] = {}
    same_coordinates: dict[str, int] = {}
    examples: dict[str, list[list[str]]] = {}
    reader = Neo4jExistingGraph(sync_driver, database=database)
    for bundle in bundles:
        by_content: dict[str, list[str]] = {}
        for unit_key, content_key in reader.unit_identities(bundle.source_id):
            by_content.setdefault(content_key, []).append(unit_key)
        candidates = [keys for keys in by_content.values() if len(keys) > 1]
        signatures = await _observation_signatures(driver, database, [k for keys in candidates for k in keys])
        # A group is a duplicate only when its units also carry the same observations. Units that share
        # crop, variety, site and season but come from different tables (quality, disease...) and carry
        # different traits are distinct information, reported apart.
        duplicate_groups = [keys for keys in candidates if len({signatures[k] for k in keys}) < len(keys)]
        duplicates = len(duplicate_groups)
        examples[bundle.source_id] = [sorted(keys) for keys in duplicate_groups[:EXAMPLE_LIMIT]]
        content_groups[bundle.source_id] = duplicates
        same_coordinates[bundle.source_id] = len(candidates) - duplicates
        if duplicates:
            problems.append(f"{bundle.source_id}: {duplicates} group(s) of units with the same content and the same "
                            "observations under different keys")
    return Check("duplicates", not problems,
                 {"duplicate_key_groups": key_groups, "content_duplicate_groups": content_groups,
                  "same_coordinates_different_observations_groups": same_coordinates,
                  "content_duplicate_examples": examples}, tuple(problems))


# ═════════════════════════════════════════════════════════════════════════════
# 3. gate and unmapped
# ═════════════════════════════════════════════════════════════════════════════

def _check_gate(gate_reports: Sequence[GateReport], bundles: Sequence[Bundle]) -> Check:
    problems: list[str] = []
    by_source = {report.source_id: report for report in gate_reports}
    for bundle in bundles:
        report = by_source.get(bundle.source_id)
        if report is None:
            problems.append(f"{bundle.source_id}: no gate report given")
        elif report.status != "pass" or report.errors:
            problems.append(f"{bundle.source_id}: gate failed ({dict(report.error_counts)})")
    detail = {sid: {"status": r.status, "errors": dict(r.error_counts), "warnings": dict(r.warning_counts)}
              for sid, r in sorted(by_source.items())}
    return Check("gate", not problems, detail, tuple(problems))


async def _check_unmapped(
    driver: Any, database: str | None, bundles: Sequence[Bundle], registries: Registries,
) -> Check:
    problems: list[str] = []
    for bundle in bundles:
        rep = bundle.report
        if rep.unresolved_sites:
            problems.append(f"{bundle.source_id}: {len(rep.unresolved_sites)} unresolved raw site name(s)")
        if rep.unresolved_vocab:
            problems.append(f"{bundle.source_id}: {len(rep.unresolved_vocab)} unresolved vocabulary literal(s)")
    registered = sorted(v.id for v in registries.variables)
    no_variable = await _scalar(
        driver, database,
        "MATCH (o:Observation) WHERE o.variableId IS NULL OR NOT o.variableId IN $ids RETURN count(o) AS c",
        ids=registered)
    no_link = await _scalar(
        driver, database, "MATCH (o:Observation) WHERE NOT (o)-[:OF_VARIABLE]->(:Variable) RETURN count(o) AS c")
    if no_variable:
        problems.append(f"{no_variable} observation(s) without a registered variable")
    if no_link:
        problems.append(f"{no_link} observation(s) without an OF_VARIABLE link")
    return Check("unmapped", not problems,
                 {"observations_without_registered_variable": no_variable, "observations_without_variable_link": no_link},
                 tuple(problems))


# ═════════════════════════════════════════════════════════════════════════════
# 4. orphans, and the sites view
# ═════════════════════════════════════════════════════════════════════════════

def _unlabelled_site_keys(bundles: Sequence[Bundle], contracts: Mapping[str, Contract]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for bundle in bundles:
        contract = contracts.get(bundle.source_id)
        wanted = contract.sites.unlabelled_site if contract is not None else None
        keys = {identity.site_key(s) for s in bundle.sites}
        out[bundle.source_id] = wanted if wanted in keys else None
    return out


async def _sites_view(driver: Any, database: str | None, source_id: str, unlabelled: str | None) -> SitesView:
    rows = await _rows(
        driver, database,
        "MATCH (t:TrialSite) WHERE $s IN t.sourceIds "
        "OPTIONAL MATCH (u:ObservationUnit {source_id: $s})-[:TRIAL_AT]->(t) "
        "RETURN t.siteKey AS key, t.siteKind AS kind, count(u) AS units", s=source_id)
    counts = {"field": [0, 0], "zone": [0, 0], "unlabelled": [0, 0]}
    for row in rows:
        if row["kind"] == "field":
            bucket = "field"
        elif row["key"] == unlabelled:
            bucket = "unlabelled"
        else:
            bucket = "zone"
        counts[bucket][0] += 1
        counts[bucket][1] += int(row["units"])
    without = await _scalar(
        driver, database,
        "MATCH (u:ObservationUnit {source_id: $s}) WHERE NOT (u)-[:TRIAL_AT]->(:TrialSite) RETURN count(u) AS c",
        s=source_id)
    return SitesView(
        field_sites=counts["field"][0], field_units=counts["field"][1],
        zone_aggregate_sites=counts["zone"][0], zone_aggregate_units=counts["zone"][1],
        unlabelled_aggregate_sites=counts["unlabelled"][0], unlabelled_aggregate_units=counts["unlabelled"][1],
        units_without_site=without)


async def _check_orphans(
    driver: Any, database: str | None, bundles: Sequence[Bundle], thresholds: Thresholds,
    views: Mapping[str, SitesView],
) -> Check:
    problems: list[str] = []
    detail: dict[str, Any] = {}
    for bundle in bundles:
        sid = bundle.source_id
        total = len(bundle.units)
        without_site = views[sid].units_without_site
        ratio = without_site / total if total else 0.0
        empty_sites = [r["key"] for r in await _rows(
            driver, database,
            "MATCH (t:TrialSite) WHERE $s IN t.sourceIds AND NOT ()-[:TRIAL_AT]->(t) "
            "RETURN t.siteKey AS key ORDER BY key LIMIT 50", s=sid)]
        docs = await _scalar(
            driver, database,
            "MATCH (d:ArticleSource {source_id: $s}) WHERE NOT ()-[:SOURCED_FROM]->(d) RETURN count(d) AS c", s=sid)
        detail[sid] = {"units_without_site": without_site, "units_without_site_ratio": ratio,
                       "sites_without_units": empty_sites, "documents_without_units": docs}
        if ratio > thresholds.max_units_without_site_ratio:
            problems.append(f"{sid}: {without_site} of {total} units without a site "
                            f"(limit ratio {thresholds.max_units_without_site_ratio})")
        if len(empty_sites) > thresholds.max_sites_without_units:
            problems.append(f"{sid}: {len(empty_sites)} site(s) without units (limit {thresholds.max_sites_without_units}): "
                            f"{empty_sites[:5]}")
        if docs > thresholds.max_documents_without_units:
            problems.append(f"{sid}: {docs} document(s) without units (limit {thresholds.max_documents_without_units})")
    return Check("orphans", not problems, detail, tuple(problems))


# ═════════════════════════════════════════════════════════════════════════════
# 5. yield metric, 6. licence
# ═════════════════════════════════════════════════════════════════════════════

async def _check_yield_metric(driver: Any, database: str | None) -> Check:
    units = await _scalar(
        driver, database,
        "MATCH (u:ObservationUnit) WHERE u.yieldKgHa IS NOT NULL AND u.yieldMetric IS NULL RETURN count(u) AS c")
    obs = await _scalar(
        driver, database,
        "MATCH (o:Observation {variableId: $v}) WHERE o.value IS NOT NULL AND o.metric IS NULL RETURN count(o) AS c",
        v=YIELD_VARIABLE)
    problems = []
    if units:
        problems.append(f"{units} unit(s) with a yield and no yieldMetric")
    if obs:
        problems.append(f"{obs} yield observation(s) without a metric")
    return Check("yield_metric", not problems, {"units_without_metric": units, "observations_without_metric": obs},
                 tuple(problems))


async def _check_licences(driver: Any, database: str | None, registries: Registries) -> Check:
    problems: list[str] = []
    sources = await _rows(driver, database,
                          "MATCH (s:Source) RETURN s.sourceId AS id, s.commercialUse AS use ORDER BY id")
    present = {r["id"] for r in sources}
    for row in sources:
        if row["use"] not in LOADABLE_COMMERCIAL_USE:
            problems.append(f"source {row['id']}: commercialUse={row['use']!r} is not a permitted licence")
        try:
            if not registries.source(row["id"]).loadable:
                problems.append(f"source {row['id']}: the registry does not permit it")
        except KeyError:
            problems.append(f"source {row['id']}: not in the sources registry")
    named = {r["id"] for r in await _rows(
        driver, database, "MATCH (u:ObservationUnit) RETURN DISTINCT u.source_id AS id")}
    for missing in sorted(named - present):
        problems.append(f"units name source {missing!r} but the graph has no Source node for it")
    return Check("licence", not problems, {"sources": {r["id"]: r["use"] for r in sources}}, tuple(problems))


# ═════════════════════════════════════════════════════════════════════════════
# entry point
# ═════════════════════════════════════════════════════════════════════════════

async def verify(
    driver: Any,
    sync_driver: Any,
    bundles: Sequence[Bundle],
    contracts: Sequence[Contract],
    gate_reports: Sequence[GateReport],
    registries: Registries,
    *,
    thresholds: Thresholds | None = None,
    database: str | None = None,
) -> VerifyReport:
    """Run every acceptance check against the graph; never writes. ``sync_driver`` serves the content-duplicate reader."""
    limits = thresholds or Thresholds()
    by_contract = {c.source_id: c for c in contracts}
    unlabelled = _unlabelled_site_keys(bundles, by_contract)
    views = {b.source_id: await _sites_view(driver, database, b.source_id, unlabelled[b.source_id]) for b in bundles}
    total = SitesView(*(sum(getattr(v, f) for v in views.values()) for f in SitesView.__dataclass_fields__))
    checks = (
        await _check_counts(driver, database, bundles, by_contract),
        await _check_duplicates(driver, sync_driver, database, bundles),
        _check_gate(gate_reports, bundles),
        await _check_unmapped(driver, database, bundles, registries),
        await _check_orphans(driver, database, bundles, limits, views),
        await _check_yield_metric(driver, database),
        await _check_licences(driver, database, registries),
    )
    report = VerifyReport(checks, {**views, "ALL": total})
    for check in checks:
        logger.info("kg verify check=%s ok=%s problems=%d", check.name, check.ok, len(check.problems))
    return report


__all__ = ["NATURAL_KEYS", "Check", "SitesView", "Thresholds", "VerifyReport", "verify"]
