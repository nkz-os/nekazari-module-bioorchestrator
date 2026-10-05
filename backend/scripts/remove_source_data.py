#!/usr/bin/env python3
# ruff: noqa: EXE001
"""Licence takedown: remove one data source (and what only it supports) from the graph.

Selection
  1. VarietyTrials of the source. Every distinct ``(source_id, dataSource)`` pair in
     the graph is resolved through ``canonical_source_id`` (the same alias table the
     ingesters use), so ``ctifl`` / ``CTIFL`` / ``LEGACY`` rows tagged
     ``dataSource='ctifl'`` all count as the source.
  2. Optional ``--include-legacy-matches RAW.jsonld ...``: ``LEGACY`` VarietyTrials
     whose (crop, variety, site, year, yield) equals a VarietyTrial record of the
     source's own raw JSON-LD (an old re-extraction that was ingested without a
     source tag).
  3. Dependents that become unreferenced ONLY because of the removal: TrialSite,
     ArticleSource and Rootstock nodes whose every relationship goes to a selected
     trial. A node still referenced by any other node is kept and reported.
     ``--include-owned-unlinked`` also selects nodes that carry the source's own tag
     but were already unreferenced before this run (off by default).

Modes
  --dry-run (default)  read-only: every Cypher statement runs in a READ_ACCESS
                       session through ``execute_read``; nothing is written to the
                       graph. A JSON report of what WOULD be removed is written.
  --apply              deletes in batches of ``--batch-size`` VarietyTrials. Each batch
                       is its own small transaction: collect the batch's dependents,
                       ``DETACH DELETE`` the trials, then delete the dependents that
                       reached zero relationships. Re-running finds nothing (0 changes).
                       The plan report is written BEFORE the first delete and
                       completed afterwards.

Take a database backup before ``--apply``; this tool does not.

Usage:
    PYTHONPATH=. python3 scripts/remove_source_data.py --source ctifl \
        --include-legacy-matches /path/all_trials.jsonld /path/all_trials_enriched.jsonld \
        --dry-run --report /tmp/report.json
    PYTHONPATH=. python3 scripts/remove_source_data.py --source ctifl \
        --include-legacy-matches ... --apply --expect-trials 133
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.graph.dao import EVIDENCE_THRESHOLD
from app.graph.site_canonicalization import normalize_site_key
from app.ingestion.normalization_registry import canonical_source_id
from neo4j import READ_ACCESS, WRITE_ACCESS, GraphDatabase

logger = logging.getLogger("remove_source_data")

LEGACY_SOURCE = "LEGACY"
# Node labels that depend on trials for their existence.
DEPENDENT_LABELS = ("TrialSite", "ArticleSource", "Rootstock")
_DEPENDENT_PREDICATE = " OR ".join(f"d:{label}" for label in DEPENDENT_LABELS)
_OWNER_FIELDS = ("source_id", "source", "dataSource")
_WS_RE = re.compile(r"\s+")

REPORT_VERSION = 1


# ──────────────────────────────────────────────────────────────────────────────
# Key normalization (legacy matching)
# ──────────────────────────────────────────────────────────────────────────────
def _fold(value: Any) -> str:
    """casefold + strip diacritics + collapse whitespace."""
    if value is None:
        return ""
    s = unicodedata.normalize("NFKD", str(value))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return _WS_RE.sub(" ", s).strip().casefold()


def variety_key(name: Any) -> str:
    """Variety identity. Parenthetical qualifiers are KEPT on purpose: they separate
    distinct treatments ("Dream (Production ...)" vs "Dream (Pepiniere ...)")."""
    return _fold(name)


def yield_key(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return None


def year_key(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def crop_keys(eppo: Any, scientific: Any) -> frozenset[str]:
    """Crop identity set: EPPO code and/or scientific name. Two records agree on the
    crop when the sets intersect (a legacy row may carry only one of the two)."""
    out: set[str] = set()
    code = _fold(str(eppo).replace("eppo:", "").replace("EPPO:", "")) if eppo else ""
    if code:
        out.add(code)
    sci = _fold(str(scientific).replace("×", "x")) if scientific else ""
    if sci:
        out.add(sci)
    return frozenset(out)


def site_keys(names: Iterable[Any], source_id: str) -> frozenset[str]:
    """Site identity keys. Besides the canonical site key, a leading source token is
    dropped ("CTIFL Balandran" -> "balandran"), because some scrapers prefix it."""
    out: set[str] = set()
    prefix = _fold(source_id) + " "
    for name in names:
        key = normalize_site_key(name)
        if not key:
            continue
        out.add(key)
        if key.startswith(prefix) and len(key) > len(prefix):
            out.add(key[len(prefix):])
    return frozenset(out)


def _g(node: dict, *keys: str) -> Any:
    for k in keys:
        if node.get(k) is not None:
            return node[k]
    return None


class LegacyIndex:
    """Index of the source's raw VarietyTrial records for legacy-row matching."""

    def __init__(self, source_id: str) -> None:
        self.source_id = source_id
        # (variety, year, yield) -> [(crop_keys, site_keys, raw_id)]
        self._by_key: dict[tuple, list[tuple[frozenset, frozenset, str]]] = defaultdict(list)
        self.records = 0
        self.files: list[str] = []

    def add_file(self, path: str | Path) -> int:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        graph = data.get("@graph", []) if isinstance(data, dict) else data
        added = 0
        for node in graph:
            if not isinstance(node, dict) or node.get("@type") != "VarietyTrial":
                continue
            key = (
                variety_key(node.get("variety")),
                year_key(node.get("year")),
                yield_key(_g(node, "yield_kg_ha", "yieldKgHa")),
            )
            self._by_key[key].append(
                (
                    crop_keys(_g(node, "crop_eppo", "cropEppo"), _g(node, "crop_scientific", "cropScientific")),
                    site_keys([_g(node, "trial_location", "trialLocation")], self.source_id),
                    str(node.get("@id", "")),
                )
            )
            added += 1
        self.records += added
        self.files.append(str(path))
        return added

    def known_sites(self) -> frozenset[str]:
        return frozenset(k for recs in self._by_key.values() for _, sites, _ in recs for k in sites)

    def match(self, vt: dict) -> dict | None:
        """Return match evidence for a graph VarietyTrial row, or None."""
        key = (variety_key(vt.get("variety")), year_key(vt.get("year")), yield_key(vt.get("yieldKgHa")))
        candidates = self._by_key.get(key)
        if not candidates:
            return None
        crops = crop_keys(vt.get("cropEppo"), vt.get("cropScientific"))
        sites = site_keys([vt.get("trialLocation"), *vt.get("siteNames", [])], self.source_id)
        if not sites:
            return None  # never match on variety+year alone
        for raw_crops, raw_sites, raw_id in candidates:
            if not (sites & raw_sites):
                continue
            if crops:
                if not (crops & raw_crops) and raw_crops:
                    continue
                crop_unknown = False
            else:
                # Legacy row without any crop field: variety + site + year + yield must carry
                # the match, and a null yield is too weak to do it alone.
                if key[2] is None:
                    continue
                crop_unknown = True
            return {"rawId": raw_id, "nullYield": key[2] is None, "cropUnknown": crop_unknown}
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Source variants
# ──────────────────────────────────────────────────────────────────────────────
def _pair(source_id: Any, data_source: Any) -> tuple[str, str]:
    return (source_id or "", data_source or "")


def resolve_variants(rows: Iterable[dict], target: str) -> dict:
    """Split the graph's (source_id, dataSource) pairs into: pairs that belong to
    ``target``, LEGACY pairs (candidates for raw matching) and near-miss pairs
    (mention the id but do not resolve to it - reported, never selected)."""
    selected, legacy, near = [], [], []
    needle = target.casefold()
    for row in rows:
        sid, ds, n = row["sid"], row["ds"], row["n"]
        canon = {canonical_source_id(sid), canonical_source_id(ds)} - {None}
        entry = {"source_id": sid, "dataSource": ds, "count": n}
        if target in canon:
            reasons = []
            if canonical_source_id(sid) == target:
                reasons.append("source_id")
            if canonical_source_id(ds) == target:
                reasons.append("dataSource")
            selected.append({**entry, "selectedBy": "+".join(reasons)})
        elif LEGACY_SOURCE in canon and target != LEGACY_SOURCE:
            legacy.append(entry)
        elif any(needle in (v or "").casefold() for v in (sid, ds)):
            near.append(entry)
    return {"selected": selected, "legacy": legacy, "nearMiss": near}


# ──────────────────────────────────────────────────────────────────────────────
# Read-only planning
# ──────────────────────────────────────────────────────────────────────────────
_Q_PAIRS = """
MATCH (v:VarietyTrial)
RETURN v.source_id AS sid, v.dataSource AS ds, count(*) AS n
"""

_Q_TRIALS = """
MATCH (v:VarietyTrial)
WHERE [coalesce(v.source_id, ''), coalesce(v.dataSource, '')] IN $pairs
OPTIONAL MATCH (v)-[:TRIAL_AT]->(t:TrialSite)
RETURN elementId(v) AS id, v.mergeKey AS mergeKey, v.source_id AS source_id,
       v.dataSource AS dataSource, v.cropEppo AS cropEppo, v.cropScientific AS cropScientific,
       v.variety AS variety, v.trialLocation AS trialLocation, v.year AS year,
       v.yieldKgHa AS yieldKgHa, (v.yieldNoteS1 IS NOT NULL) AS hasNoteS1,
       coalesce(v.rankingEligible, true) AS rankingEligible,
       collect(DISTINCT t.name) AS siteNames
"""

_Q_DEPENDENTS = f"""
MATCH (v)-[r]-(d)
WHERE elementId(v) IN $ids AND ({_DEPENDENT_PREDICATE})
RETURN elementId(d) AS id, labels(d)[0] AS label, count(r) AS refs
"""

_Q_DEPENDENT_INFO = f"""
MATCH (d)
WHERE elementId(d) IN $ids AND ({_DEPENDENT_PREDICATE})
RETURN elementId(d) AS id, labels(d)[0] AS label, d.mergeKey AS mergeKey, d.siteKey AS siteKey,
       d.name AS name, d.articleTitle AS articleTitle, d.source_id AS source_id,
       d.source AS source, d.dataSource AS dataSource, COUNT {{ (d)--() }} AS degree
"""

_Q_DEPENDENT_NEIGHBOURS = """
MATCH (d)--(x)
WHERE elementId(d) IN $ids
RETURN elementId(d) AS dep, elementId(x) AS other, labels(x)[0] AS label,
       coalesce(x.source_id, x.dataSource, '?') AS sid
"""

_Q_OWNED = f"""
MATCH (d)
WHERE {_DEPENDENT_PREDICATE}
RETURN elementId(d) AS id, labels(d)[0] AS label, d.mergeKey AS mergeKey, d.siteKey AS siteKey,
       d.name AS name, d.articleTitle AS articleTitle, d.source_id AS source_id,
       d.source AS source, d.dataSource AS dataSource, COUNT {{ (d)--() }} AS degree
"""

_Q_IMPACT = """
MATCH (v:VarietyTrial)
WHERE v.cropEppo IN $codes OR toLower(coalesce(v.cropScientific, '')) IN $scis
WITH v, EXISTS { (v)-[:TRIAL_AT]->(:TrialSite) } AS hasSite
RETURN coalesce(v.cropEppo, '') AS eppo, toLower(coalesce(v.cropScientific, '')) AS sci,
       coalesce(v.source_id, '') AS sid,
       (hasSite AND (v.yieldKgHa IS NOT NULL OR v.yieldNoteS1 IS NOT NULL)
         AND coalesce(v.rankingEligible, true)) AS rankable,
       count(*) AS n
"""


def _chunks(items: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _read(driver, query: str, **params) -> list[dict]:
    """One read transaction, READ_ACCESS session. Never writes."""
    def work(tx):
        return [r.data() for r in tx.run(query, **params)]

    with driver.session(default_access_mode=READ_ACCESS) as session:
        return session.execute_read(work)


def _owned_by(props: dict, target: str) -> bool:
    return any(canonical_source_id(props.get(f)) == target for f in _OWNER_FIELDS)


def _is_rankable(vt: dict) -> bool:
    return bool(
        vt.get("siteNames")
        and (vt.get("yieldKgHa") is not None or vt.get("hasNoteS1"))
        and vt.get("rankingEligible", True)
    )


def build_plan(
    driver,
    target: str,
    *,
    legacy_paths: Sequence[str | Path] = (),
    include_owned_unlinked: bool = False,
    chunk: int = 1000,
) -> dict:
    """Compute everything that --apply would delete. READ ONLY."""
    pairs = _read(driver, _Q_PAIRS)
    variants = resolve_variants(pairs, target)

    source_pairs = [list(_pair(v["source_id"], v["dataSource"])) for v in variants["selected"]]
    trials: dict[str, dict] = {}
    if source_pairs:
        for row in _read(driver, _Q_TRIALS, pairs=source_pairs):
            row["selectedBy"] = next(
                v["selectedBy"] for v in variants["selected"]
                if _pair(v["source_id"], v["dataSource"]) == _pair(row["source_id"], row["dataSource"])
            )
            trials[row["id"]] = row

    legacy_report: dict = {"files": [], "rawRecords": 0, "candidates": 0, "matched": 0, "matchedNullYield": 0,
                           "matchedCropUnknown": 0, "unmatched": []}
    if legacy_paths:
        index = LegacyIndex(target)
        for path in legacy_paths:
            index.add_file(path)
        legacy_report["files"] = [Path(p).name for p in index.files]
        legacy_report["rawRecords"] = index.records
        legacy_pairs = [list(_pair(v["source_id"], v["dataSource"])) for v in variants["legacy"]]
        candidates = _read(driver, _Q_TRIALS, pairs=legacy_pairs) if legacy_pairs else []
        legacy_report["candidates"] = len(candidates)
        known_sites = index.known_sites()
        for row in candidates:
            evidence = index.match(row)
            if evidence is None:
                # Listed only when the row sits at a site the raw data knows: a near miss
                # worth a human look. Other LEGACY rows are unrelated and not listed.
                if site_keys([row.get("trialLocation"), *row["siteNames"]], target) & known_sites:
                    legacy_report["unmatched"].append(
                        {"id": row["id"], "mergeKey": row["mergeKey"], "variety": row["variety"],
                         "site": row["trialLocation"], "year": row["year"], "yieldKgHa": row["yieldKgHa"]}
                    )
                continue
            row["selectedBy"] = "legacy_match"
            row["match"] = evidence
            trials[row["id"]] = row
            legacy_report["matched"] += 1
            legacy_report["matchedNullYield"] += int(evidence["nullYield"])
            legacy_report["matchedCropUnknown"] += int(evidence["cropUnknown"])

    selected_ids = set(trials)

    # Dependents: count references coming from the selected trials, compare with degree.
    refs: Counter[str] = Counter()
    for part in _chunks(sorted(selected_ids), chunk):
        for row in _read(driver, _Q_DEPENDENTS, ids=list(part)):
            refs[row["id"]] += row["refs"]
    info: dict[str, dict] = {}
    for part in _chunks(sorted(refs), chunk):
        for row in _read(driver, _Q_DEPENDENT_INFO, ids=list(part)):
            info[row["id"]] = row

    removed_deps: list[dict] = []
    kept_candidates: list[dict] = []
    for dep_id, node in info.items():
        entry = _dependent_entry(node, target)
        entry["referencesFromRemoved"] = refs[dep_id]
        if node["degree"] == refs[dep_id]:
            entry["reason"] = "referenced only by removed trials"
            removed_deps.append(entry)
        else:
            entry["referencesRemaining"] = node["degree"] - refs[dep_id]
            kept_candidates.append(entry)

    kept_ids = [e["id"] for e in kept_candidates]
    if kept_ids:
        remaining_by: dict[str, Counter] = defaultdict(Counter)
        for part in _chunks(sorted(kept_ids), chunk):
            for row in _read(driver, _Q_DEPENDENT_NEIGHBOURS, ids=list(part)):
                if row["other"] not in selected_ids:
                    remaining_by[row["dep"]][f"{row['label']}:{canonical_source_id(row['sid']) or row['sid']}"] += 1
        for entry in kept_candidates:
            entry["reason"] = "still referenced by other nodes"
            entry["remainingReferencesBy"] = dict(remaining_by.get(entry["id"], {}))

    owned_unlinked: list[dict] = []
    owned_kept: list[dict] = []
    removed_ids = {e["id"] for e in removed_deps}
    for node in _read(driver, _Q_OWNED):
        if not _owned_by(node, target) or node["id"] in removed_ids:
            continue
        entry = _dependent_entry(node, target)
        if node["degree"] == 0:
            entry["reason"] = "carries the source tag, already unreferenced before this run"
            owned_unlinked.append(entry)
        elif node["id"] not in refs:
            entry["reason"] = "carries the source tag but no removed trial points at it"
            entry["references"] = node["degree"]
            owned_kept.append(entry)
    if owned_kept:
        by_dep: dict[str, Counter] = defaultdict(Counter)
        for part in _chunks(sorted(e["id"] for e in owned_kept), chunk):
            for row in _read(driver, _Q_DEPENDENT_NEIGHBOURS, ids=list(part)):
                by_dep[row["dep"]][f"{row['label']}:{canonical_source_id(row['sid']) or row['sid']}"] += 1
        for entry in owned_kept:
            entry["remainingReferencesBy"] = dict(by_dep.get(entry["id"], {}))
    if include_owned_unlinked:
        removed_deps.extend(owned_unlinked)

    impact = _impact(driver, trials, target)

    return {
        "variants": variants,
        "legacy": legacy_report,
        "trials": sorted(trials.values(), key=lambda r: (r["selectedBy"], str(r["mergeKey"]))),
        "dependentsRemoved": sorted(removed_deps, key=lambda e: (e["label"], str(e["key"]))),
        "dependentsKept": sorted(kept_candidates, key=lambda e: (e["label"], str(e["key"]))),
        "ownedUnlinked": owned_unlinked,
        "ownedKept": owned_kept,
        "includeOwnedUnlinked": include_owned_unlinked,
        "impact": impact,
    }


def _dependent_entry(node: dict, target: str) -> dict:
    return {
        "id": node["id"],
        "label": node["label"],
        "key": node.get("mergeKey") or node.get("siteKey") or node.get("name") or node.get("articleTitle"),
        "name": node.get("name") or node.get("articleTitle"),
        "owner": next((canonical_source_id(node.get(f)) for f in _OWNER_FIELDS if node.get(f)), None),
    }


def _impact(driver, trials: dict[str, dict], target: str) -> list[dict]:
    """Per crop: trial evidence before/after, by source. 'Rankable' mirrors what the
    recommender can use: linked to a site, with a yield (or S1 note), ranking-eligible."""
    if not trials:
        return []
    codes = sorted({str(t["cropEppo"]).upper() for t in trials.values() if t.get("cropEppo")})
    scis = sorted({str(t["cropScientific"]).lower() for t in trials.values() if t.get("cropScientific")})
    rows = _read(driver, _Q_IMPACT, codes=codes, scis=scis)

    sci_to_eppo = {r["sci"]: r["eppo"].upper() for r in rows if r["eppo"] and r["sci"]}

    def group(eppo: str, sci: str) -> str:
        return (eppo or sci_to_eppo.get(sci) or "").upper() or sci or "?"

    total: dict[str, Counter] = defaultdict(Counter)
    for r in rows:
        crop = group(r["eppo"], r["sci"])
        src = canonical_source_id(r["sid"]) or "?"
        total[crop][(src, bool(r["rankable"]))] += r["n"]

    removed: dict[str, Counter] = defaultdict(Counter)
    names: dict[str, str] = {}
    for t in trials.values():
        sci = str(t.get("cropScientific") or "").lower()
        crop = group(str(t.get("cropEppo") or ""), sci)
        src = canonical_source_id(t.get("source_id")) or "?"
        removed[crop][(src, _is_rankable(t))] += 1
        if t.get("cropScientific"):
            names[crop] = t["cropScientific"]

    out = []
    for crop in sorted(removed):
        remaining_rankable_by: Counter = Counter()
        remaining_all = remaining_rankable = 0
        for (src, rankable), n in total[crop].items():
            left = n - removed[crop].get((src, rankable), 0)
            remaining_all += left
            if rankable:
                remaining_rankable += left
                if left:
                    remaining_rankable_by[src] += left
        out.append({
            "crop": crop,
            "scientific": names.get(crop),
            "removedTrials": sum(removed[crop].values()),
            "removedRankable": sum(n for (_, r), n in removed[crop].items() if r),
            "remainingTrials": remaining_all,
            "remainingRankable": remaining_rankable,
            "remainingRankableBySource": dict(remaining_rankable_by),
            "evidenceThreshold": EVIDENCE_THRESHOLD,
            "belowEvidenceThresholdAfter": remaining_rankable < EVIDENCE_THRESHOLD,
        })
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Apply
# ──────────────────────────────────────────────────────────────────────────────
_Q_BATCH_DEPS = f"""
MATCH (v)-[]-(d)
WHERE elementId(v) IN $ids AND ({_DEPENDENT_PREDICATE})
RETURN DISTINCT elementId(d) AS id
"""

_Q_BATCH_DELETE_TRIALS = """
MATCH (v:VarietyTrial)
WHERE elementId(v) IN $ids
WITH v, elementId(v) AS id, v.mergeKey AS mergeKey
DETACH DELETE v
RETURN id, mergeKey
"""

_Q_BATCH_DELETE_DEPS = f"""
MATCH (d)
WHERE elementId(d) IN $ids AND ({_DEPENDENT_PREDICATE}) AND NOT EXISTS {{ (d)--() }}
WITH d, elementId(d) AS id, labels(d)[0] AS label,
     coalesce(d.mergeKey, d.siteKey, d.name, d.articleTitle) AS key
DETACH DELETE d
RETURN id, label, key
"""


def _delete_batch(driver, trial_ids: Sequence[str]) -> dict:
    """One small write transaction for one batch of trials."""
    def work(tx):
        dep_ids = [r["id"] for r in tx.run(_Q_BATCH_DEPS, ids=list(trial_ids))]
        trials = [r.data() for r in tx.run(_Q_BATCH_DELETE_TRIALS, ids=list(trial_ids))]
        deps = [r.data() for r in tx.run(_Q_BATCH_DELETE_DEPS, ids=dep_ids)] if dep_ids else []
        return {"trials": trials, "dependents": deps}

    with driver.session(default_access_mode=WRITE_ACCESS) as session:
        return session.execute_write(work)


def _delete_unlinked_batch(driver, ids: Sequence[str]) -> list[dict]:
    def work(tx):
        return [r.data() for r in tx.run(_Q_BATCH_DELETE_DEPS, ids=list(ids))]

    with driver.session(default_access_mode=WRITE_ACCESS) as session:
        return session.execute_write(work)


def apply_plan(driver, plan: dict, batch_size: int) -> dict:
    trial_ids = [t["id"] for t in plan["trials"]]
    deleted_trials: list[dict] = []
    deleted_deps: list[dict] = []
    batches = 0
    for part in _chunks(trial_ids, batch_size):
        result = _delete_batch(driver, part)
        deleted_trials += result["trials"]
        deleted_deps += result["dependents"]
        batches += 1
        logger.info("batch=%d trials_deleted=%d dependents_deleted=%d", batches,
                    len(result["trials"]), len(result["dependents"]))
    if plan["includeOwnedUnlinked"]:
        unlinked = [e["id"] for e in plan["ownedUnlinked"]]
        for part in _chunks(unlinked, batch_size):
            deleted_deps += _delete_unlinked_batch(driver, part)
            batches += 1
    return {"batches": batches, "trials": deleted_trials, "dependents": deleted_deps}


# ──────────────────────────────────────────────────────────────────────────────
# Report + CLI
# ──────────────────────────────────────────────────────────────────────────────
def _counts(plan: dict) -> dict:
    by_sel = Counter(t["selectedBy"] for t in plan["trials"])
    removed = Counter(e["label"] for e in plan["dependentsRemoved"])
    kept = Counter(e["label"] for e in plan["dependentsKept"])
    unlinked = Counter(e["label"] for e in plan["ownedUnlinked"])
    owned_kept = Counter(e["label"] for e in plan["ownedKept"])
    return {
        "VarietyTrial": {"total": len(plan["trials"]), "bySelection": dict(by_sel)},
        "dependentsRemoved": dict(removed),
        "relationshipsToRemovedDependents": sum(
            e.get("referencesFromRemoved", 0) for e in plan["dependentsRemoved"]
        ),
        "dependentsKept": dict(kept),
        "ownedUnlinked": dict(unlinked),
        "ownedKept": dict(owned_kept),
    }


def _trial_view(t: dict) -> dict:
    keep = ("id", "mergeKey", "source_id", "dataSource", "selectedBy", "cropEppo", "cropScientific",
            "variety", "trialLocation", "year", "yieldKgHa", "siteNames", "match")
    return {k: t[k] for k in keep if k in t}


def build_report(plan: dict, *, source: str, mode: str, batch_size: int, started: str, applied: dict | None,
                 post_check: dict | None) -> dict:
    report = {
        "tool": "remove_source_data",
        "reportVersion": REPORT_VERSION,
        "mode": mode,
        "source": source,
        "startedAt": started,
        "finishedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "batchSize": batch_size,
        "counts": _counts(plan),
        "variants": plan["variants"],
        "legacyMatching": plan["legacy"],
        "trials": [_trial_view(t) for t in plan["trials"]],
        "dependentsRemoved": plan["dependentsRemoved"],
        "dependentsKept": plan["dependentsKept"],
        "ownedUnlinked": {"selectedForRemoval": plan["includeOwnedUnlinked"], "nodes": plan["ownedUnlinked"]},
        "ownedKept": plan["ownedKept"],
        "impact": plan["impact"],
    }
    if applied is not None:
        report["applied"] = {
            "batches": applied["batches"],
            "deletedTrials": applied["trials"],
            "deletedDependents": applied["dependents"],
            "counts": {
                "VarietyTrial": len(applied["trials"]),
                **dict(Counter(d["label"] for d in applied["dependents"])),
            },
        }
        report["postCheck"] = post_check
    return report


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def run(
    *,
    driver,
    source: str,
    apply: bool = False,
    legacy_paths: Sequence[str | Path] = (),
    batch_size: int = 200,
    include_owned_unlinked: bool = False,
    expect_trials: int | None = None,
    report_path: str | Path | None = None,
) -> dict:
    """Plan (always read-only) and, if ``apply``, delete. Returns the report dict."""
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    target = canonical_source_id(source)
    if not target:
        raise ValueError("source must be a non-empty id")
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    mode = "apply" if apply else "dry-run"
    plan = build_plan(driver, target, legacy_paths=legacy_paths, include_owned_unlinked=include_owned_unlinked)
    logger.info("source=%s mode=%s counts=%s", target, mode, json.dumps(_counts(plan), ensure_ascii=False))

    path = Path(report_path) if report_path else None
    if not apply:
        report = build_report(plan, source=target, mode=mode, batch_size=batch_size, started=started,
                              applied=None, post_check=None)
        if path:
            _write_report(path, report)
        return report

    if expect_trials is not None and len(plan["trials"]) != expect_trials:
        raise SystemExit(
            f"refusing to apply: plan selects {len(plan['trials'])} VarietyTrials, expected {expect_trials}"
        )
    if path:  # plan on disk BEFORE the first delete
        _write_report(path, build_report(plan, source=target, mode="apply-planned", batch_size=batch_size,
                                         started=started, applied=None, post_check=None))
    applied = apply_plan(driver, plan, batch_size)
    after = build_plan(driver, target, legacy_paths=legacy_paths, include_owned_unlinked=include_owned_unlinked)
    post = {"remainingTrials": len(after["trials"]), "remainingDependents": len(after["dependentsRemoved"])}
    report = build_report(plan, source=target, mode=mode, batch_size=batch_size, started=started,
                          applied=applied, post_check=post)
    if path:
        _write_report(path, report)
    return report


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Remove one data source from the graph (licence takedown).")
    p.add_argument("--source", required=True, help="source id, any known variant (e.g. ctifl / CTIFL)")
    p.add_argument("--include-legacy-matches", nargs="+", metavar="RAW_JSONLD", default=[],
                   help="raw JSON-LD of the source; LEGACY trials matching a record are also removed")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="read-only plan (default)")
    mode.add_argument("--apply", action="store_true", help="write: delete in batches")
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--include-owned-unlinked", action="store_true",
                   help="also remove nodes tagged with the source that were already unreferenced")
    p.add_argument("--expect-trials", type=int, default=None,
                   help="with --apply: abort unless the plan selects exactly this many VarietyTrials")
    p.add_argument("--report", default=None, help="JSON report path (default: ./remove_source_data_<SRC>_<mode>.json)")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)
    from app.core.config import (
        settings,  # deferred: tests inject a driver and never reach this
    )

    target = canonical_source_id(args.source) or args.source
    mode = "apply" if args.apply else "dry-run"
    report_path = args.report or f"remove_source_data_{target}_{mode}.json"
    driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        report = run(
            driver=driver,
            source=args.source,
            apply=args.apply,
            legacy_paths=args.include_legacy_matches,
            batch_size=args.batch_size,
            include_owned_unlinked=args.include_owned_unlinked,
            expect_trials=args.expect_trials,
            report_path=report_path,
        )
    finally:
        driver.close()
    print(json.dumps({"mode": report["mode"], "source": report["source"], "counts": report["counts"],
                      "report": report_path, **({"postCheck": report["postCheck"]} if "postCheck" in report else {})},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
