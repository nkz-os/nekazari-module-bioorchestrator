"""Selective removal of whole sources from a restored copy of the graph (T12, option b).

``plan`` reads (READ_ACCESS only) and ``execute`` deletes what the plan names. Both work on the UNION of the
sources given, so a site or document shared by two of them is judged against the trials that remain.

What is removed, for the sources named, and nothing else:

* **Trials**: legacy ``VarietyTrial`` nodes (no ``unitKey``) and F1 ``ObservationUnit`` nodes whose ``source_id`` or
  ``dataSource`` is one of the source's spellings (``normalization_registry.source_spellings``), together with
  their ``Observation`` nodes. A CREA ``ZEAMA`` twin is such a trial; it is counted under its crop.
* **Studies and ArticleSources** that those trials hung on (or that carry the source id) and that are left
  without any trial of another source.
* **TrialSites** that those trials hung on (the scraper-assigned reference cities) or that carry the source id
  in ``source_id`` / ``sourceIds``, and that are left without any trial of any label. Extra empty sites can be
  named explicitly (``extra_site_keys``); they are deleted only if no trial points at them.

Never removed: ``Species``, ``PhenologyStage``, nutrient / rotation / fungi nodes, ``Variety``, ``Crop``,
``Variable``, ``Source``, ``ClimateCell`` or any trial of another source. After a write the label counts are
compared with those before: a label outside :data:`TOUCHED_LABELS` that changed, or any trial of another source
that went missing, is a :class:`ReplaceError` (the deletion is already committed; the error is the alarm).

Batches of at most :data:`BATCH` trials per transaction (heap-bound production server); idempotent: a second run
finds nothing to delete.
"""
from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from app.ingestion.normalization_registry import source_spellings
from neo4j import READ_ACCESS

logger = logging.getLogger(__name__)

BATCH = 500
# The only labels whose counts may change in a run.
TOUCHED_LABELS = frozenset({"VarietyTrial", "ObservationUnit", "Observation", "Study", "ArticleSource", "TrialSite"})


class ReplaceError(RuntimeError):
    """The graph after a removal differs from the plan in a way that must not happen."""


def _sel(alias: str) -> str:
    """Cypher predicate: ``alias`` is a trial of one of the sources in ``$sp`` (lower-cased spellings)."""
    # coalesce: a trial without one of the two properties must evaluate to false, not null, because the
    # predicate is negated to find "trials of other sources" and NOT null is null (the row would vanish)
    return (f"(({alias}:VarietyTrial OR {alias}:ObservationUnit) "
            f"AND (coalesce(toLower({alias}.source_id) IN $sp, false) "
            f"OR coalesce(toLower({alias}.dataSource) IN $sp, false)))")


_SITE_TAGGED = ("(toLower(t.source_id) IN $sp OR any(x IN coalesce(t.sourceIds, []) WHERE toLower(x) IN $sp))")


@dataclass
class SourceCounts:
    legacy_trials: int = 0
    units: int = 0
    observations: int = 0
    by_crop: dict[str, int] = field(default_factory=dict)


@dataclass
class Plan:
    sources: tuple[str, ...]
    per_source: dict[str, SourceCounts]
    study_ids: list[str]
    document_ids: list[str]
    site_ids: list[str]
    studies_deletable: int = 0
    documents_deletable: int = 0
    sites_deletable: list[dict[str, Any]] = field(default_factory=list)
    sites_kept: list[dict[str, Any]] = field(default_factory=list)
    other_trials: dict[str, int] = field(default_factory=dict)
    label_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sources": list(self.sources),
            "per_source": {s: {"legacy_trials": c.legacy_trials, "units": c.units, "observations": c.observations,
                               "by_crop": dict(sorted(c.by_crop.items()))} for s, c in self.per_source.items()},
            "delete": {"studies": self.studies_deletable, "article_sources": self.documents_deletable,
                       "sites": self.sites_deletable},
            "sites_kept": self.sites_kept,
            "trials_of_other_sources": dict(sorted(self.other_trials.items())),
            "label_counts": dict(sorted(self.label_counts.items())),
        }

    @property
    def total_trials(self) -> int:
        return sum(c.legacy_trials + c.units for c in self.per_source.values())


@dataclass
class Result:
    deleted_legacy_trials: int = 0
    deleted_units: int = 0
    deleted_observations: int = 0
    deleted_studies: int = 0
    deleted_documents: int = 0
    deleted_sites: int = 0
    batches: int = 0

    def to_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


def _chunks(items: Sequence[Any], size: int) -> list[Sequence[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


async def _read(driver: Any, database: str | None, cypher: str, **params: Any) -> list[dict[str, Any]]:
    async def work(tx: Any) -> list[dict[str, Any]]:
        return [r.data() for r in await (await tx.run(cypher, **params)).fetch(10_000_000)]

    async with driver.session(database=database, default_access_mode=READ_ACCESS) as session:
        return await session.execute_read(work)


async def label_counts(driver: Any, database: str | None) -> dict[str, int]:
    """Node count per label (count-store lookups, no scan)."""
    labels = [r["label"] for r in await _read(driver, database, "CALL db.labels() YIELD label RETURN label")]
    counts = {}
    for label in sorted(labels):
        counts[label] = (await _read(driver, database, f"MATCH (n:`{label}`) RETURN count(n) AS c"))[0]["c"]
    return {k: v for k, v in counts.items() if v}


def _spellings(sources: Sequence[str]) -> list[str]:
    out: set[str] = set()
    for s in sources:
        out |= source_spellings(s)
    return sorted(out)


async def _trials_of_other_sources(driver: Any, database: str | None, sp: list[str]) -> dict[str, int]:
    rows = await _read(
        driver, database,
        f"MATCH (n:VarietyTrial) WHERE NOT ({_sel('n')}) "
        "RETURN coalesce(n.source_id, n.dataSource, '(none)') AS s, count(n) AS c", sp=sp)
    return {r["s"]: r["c"] for r in rows}


async def plan(driver: Any, sources: Sequence[str], *, database: str | None = None,
               extra_site_keys: Sequence[str] = ()) -> Plan:
    """Read-only: what ``execute`` would delete."""
    sources = tuple(sources)
    sp = _spellings(sources)
    per_source: dict[str, SourceCounts] = {}
    for source in sources:
        one = sorted(source_spellings(source))
        counts = SourceCounts()
        crops: Counter[str] = Counter()
        for row in await _read(
                driver, database,
                f"MATCH (n:VarietyTrial) WHERE {_sel('n')} "
                "RETURN n.unitKey IS NULL AS legacy, coalesce(n.cropEppo, '(none)') AS crop, count(n) AS c", sp=one):
            crops[row["crop"]] += row["c"]
            if row["legacy"]:
                counts.legacy_trials += row["c"]
            else:
                counts.units += row["c"]
        for row in await _read(
                driver, database,
                f"MATCH (n:ObservationUnit) WHERE NOT n:VarietyTrial AND {_sel('n')} "
                "RETURN coalesce(n.cropEppo, '(none)') AS crop, count(n) AS c", sp=one):
            crops[row["crop"]] += row["c"]
            counts.units += row["c"]
        counts.observations = (await _read(
            driver, database,
            f"MATCH (o:Observation)-[:ON_UNIT]->(n) WHERE {_sel('n')} RETURN count(o) AS c", sp=one))[0]["c"]
        counts.by_crop = dict(crops)
        per_source[source] = counts

    study_ids = sorted({r["id"] for r in await _read(
        driver, database,
        f"MATCH (n)-[:IN_STUDY]->(s:Study) WHERE {_sel('n')} RETURN DISTINCT elementId(s) AS id", sp=sp)} | {
        r["id"] for r in await _read(
            driver, database, "MATCH (s:Study) WHERE toLower(s.source_id) IN $sp RETURN elementId(s) AS id", sp=sp)})
    document_ids = sorted({r["id"] for r in await _read(
        driver, database,
        f"MATCH (n)-[:SOURCED_FROM]->(a:ArticleSource) WHERE {_sel('n')} RETURN DISTINCT elementId(a) AS id",
        sp=sp)} | {
        r["id"] for r in await _read(
            driver, database,
            "MATCH (a:ArticleSource) WHERE toLower(a.source_id) IN $sp RETURN elementId(a) AS id", sp=sp)})
    site_ids = sorted({r["id"] for r in await _read(
        driver, database,
        f"MATCH (n)-[:TRIAL_AT]->(t:TrialSite) WHERE {_sel('n')} RETURN DISTINCT elementId(t) AS id", sp=sp)} | {
        r["id"] for r in await _read(
            driver, database, f"MATCH (t:TrialSite) WHERE {_SITE_TAGGED} RETURN elementId(t) AS id", sp=sp)} | {
        r["id"] for r in await _read(
            driver, database, "MATCH (t:TrialSite) WHERE t.siteKey IN $keys RETURN elementId(t) AS id",
            keys=list(extra_site_keys))})

    result = Plan(sources, per_source, study_ids, document_ids, site_ids)
    for chunk in _chunks(study_ids, BATCH):
        rows = await _read(
            driver, database,
            "MATCH (s:Study) WHERE elementId(s) IN $ids "
            f"OPTIONAL MATCH (o)-[:IN_STUDY]->(s) WHERE NOT ({_sel('o')}) "
            "WITH s, count(o) AS others RETURN sum(CASE WHEN others = 0 THEN 1 ELSE 0 END) AS gone",
            ids=list(chunk), sp=sp)
        result.studies_deletable += rows[0]["gone"] or 0
    for chunk in _chunks(document_ids, BATCH):
        rows = await _read(
            driver, database,
            "MATCH (a:ArticleSource) WHERE elementId(a) IN $ids "
            f"OPTIONAL MATCH (o)-[:SOURCED_FROM]->(a) WHERE NOT ({_sel('o')}) "
            "WITH a, count(o) AS others RETURN sum(CASE WHEN others = 0 THEN 1 ELSE 0 END) AS gone",
            ids=list(chunk), sp=sp)
        result.documents_deletable += rows[0]["gone"] or 0
    for chunk in _chunks(site_ids, BATCH):
        for row in await _read(
                driver, database,
                "MATCH (t:TrialSite) WHERE elementId(t) IN $ids "
                f"OPTIONAL MATCH (o)-[:TRIAL_AT]->(t) WHERE NOT ({_sel('o')}) "
                "WITH t, count(o) AS others "
                "RETURN t.siteKey AS siteKey, t.name AS name, t.country AS country, others "
                "ORDER BY siteKey", ids=list(chunk), sp=sp):
            entry = {"siteKey": row["siteKey"], "name": row["name"], "country": row["country"]}
            if row["others"]:
                result.sites_kept.append({**entry, "trials_of_other_sources": row["others"]})
            else:
                result.sites_deletable.append(entry)
    result.sites_deletable.sort(key=lambda s: str(s["siteKey"]))
    result.other_trials = await _trials_of_other_sources(driver, database, sp)
    result.label_counts = await label_counts(driver, database)
    return result


_DELETE_TRIALS = """
MATCH (n:{label}) WHERE {sel}
WITH n LIMIT $b
OPTIONAL MATCH (o:Observation)-[:ON_UNIT]->(n)
WITH n, collect(o) AS obs, n.unitKey IS NULL AS legacy
WITH n, obs, size(obs) AS k, legacy
FOREACH (x IN obs | DETACH DELETE x)
DETACH DELETE n
RETURN sum(CASE WHEN legacy THEN 1 ELSE 0 END) AS legacy, sum(CASE WHEN legacy THEN 0 ELSE 1 END) AS units,
       sum(k) AS observations
"""


_DELETE_STUDIES = (
    "MATCH (x:Study) WHERE elementId(x) IN $ids AND NOT ()-[:IN_STUDY]->(x) DETACH DELETE x RETURN count(x) AS c")
_DELETE_DOCUMENTS = (
    "MATCH (x:ArticleSource) WHERE elementId(x) IN $ids AND NOT ()-[:SOURCED_FROM]->(x) "
    "DETACH DELETE x RETURN count(x) AS c")
_DELETE_SITES = (
    "MATCH (x:TrialSite) WHERE elementId(x) IN $ids AND NOT ()-[:TRIAL_AT]->(x) DETACH DELETE x RETURN count(x) AS c")


async def execute(driver: Any, sources: Sequence[str], *, database: str | None = None,
                  extra_site_keys: Sequence[str] = (), batch: int = BATCH) -> tuple[Plan, Result]:
    """Delete what :func:`plan` names, in batches. Returns the plan that was read and what was deleted."""
    if not 0 < batch <= BATCH:
        raise ReplaceError(f"batch must be 1..{BATCH}")
    sources = tuple(sources)
    sp = _spellings(sources)
    before = await plan(driver, sources, database=database, extra_site_keys=extra_site_keys)
    result = Result()

    async with driver.session(database=database) as session:
        for label in ("VarietyTrial", "ObservationUnit"):  # an F1 variety unit carries both labels
            while True:
                async def work(tx: Any, label: str = label) -> dict[str, Any]:
                    record = await (await tx.run(_DELETE_TRIALS.format(label=label, sel=_sel("n")),
                                                 sp=sp, b=batch)).single()
                    return record.data() if record is not None else {}

                row = await session.execute_write(work)
                if not row or not ((row["legacy"] or 0) + (row["units"] or 0)):
                    break
                result.deleted_legacy_trials += row["legacy"] or 0
                result.deleted_units += row["units"] or 0
                result.deleted_observations += row["observations"] or 0
                result.batches += 1
                logger.info("kg replace-sources label=%s deleted=%s", label, row)

        for ids, cypher, attr in (
            (before.study_ids, _DELETE_STUDIES, "deleted_studies"),
            (before.document_ids, _DELETE_DOCUMENTS, "deleted_documents"),
            (before.site_ids, _DELETE_SITES, "deleted_sites"),
        ):
            for chunk in _chunks(ids, batch):
                async def dependents(tx: Any, cypher: str = cypher, chunk: Sequence[str] = chunk) -> int:
                    record = await (await tx.run(cypher, ids=list(chunk))).single()
                    return int(record["c"]) if record is not None else 0

                setattr(result, attr, getattr(result, attr) + await session.execute_write(dependents))
                result.batches += 1

    after = await plan(driver, sources, database=database, extra_site_keys=extra_site_keys)
    problems = []
    if after.total_trials or any(c.observations for c in after.per_source.values()):
        problems.append(f"{after.total_trials} trial(s) of the replaced sources remain")
    if after.other_trials != before.other_trials:
        problems.append(f"trials of other sources changed: {before.other_trials} -> {after.other_trials}")
    changed = {k for k in before.label_counts.keys() | after.label_counts.keys()
               if before.label_counts.get(k, 0) != after.label_counts.get(k, 0)} - TOUCHED_LABELS
    if changed:
        problems.append(f"labels outside the removal set changed: {sorted(changed)}")
    if problems:
        raise ReplaceError("; ".join(problems))
    return before, result


__all__ = ["BATCH", "Plan", "ReplaceError", "Result", "execute", "label_counts", "plan"]
