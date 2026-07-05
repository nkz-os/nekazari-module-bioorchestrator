"""TrialSite canonicalization planning (0.4 spec, idempotent).

Pure planning separated from Neo4j execution so the decision logic is unit
testable without a database. Groups TrialSites by ``normalize_site_key(name)``;
municipality disagreement no longer blocks a merge (source-agnostic identity is
the name key, not municipality). Members within ``SPLIT_KM`` of each other (or
lacking geo) are merged into the richest survivor; if two geo-present members
in a same-name group are further apart than ``SPLIT_KM``, the group is instead
split into disambiguated ``site_key`` clusters and flagged ``needsHumanReview``.
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections import defaultdict

_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*$")
_WS_RE = re.compile(r"\s+")


def normalize_site_key(name: str | None) -> str:
    """Stable, source-agnostic identity key for a physical site.

    lower + trim + NFKD diacritic-fold + strip one trailing parenthetical
    qualifier (e.g. "Córdoba (Alameda del Obispo)" -> "cordoba") + collapse
    internal whitespace. Shared by the migration and base_ingester so the
    MERGE key and the migration key agree byte-for-byte.
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = _PAREN_RE.sub("", s)
    s = _WS_RE.sub(" ", s).strip().lower()
    return s


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km. Mirrors the formula in graph/dao.py."""
    lat1r, lon1r, lat2r, lon2r = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = lat2r - lat1r, lon2r - lon1r
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1r) * math.cos(lat2r) * math.sin(dlon / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(a))

# Fields that make a TrialSite "rich"; survivor = the node with most non-null.
RICHNESS_FIELDS = ("climateClass", "latitude", "municipality", "soilTexture", "annualRainfallMm")


def _norm(value: str | None) -> str:
    return (value or "").strip().lower()


def _richness(site: dict) -> int:
    return sum(1 for f in RICHNESS_FIELDS if site.get(f) is not None)


def _pick_survivor(members: list[dict]) -> dict:
    # Most non-null richness fields; deterministic tie-break by id.
    return max(members, key=lambda m: (_richness(m), str(m["id"])))


def _backfill(survivor: dict, members: list[dict]) -> dict:
    out: dict = {}
    for f in RICHNESS_FIELDS:
        if survivor.get(f) is not None:
            continue
        for m in members:
            if m["id"] == survivor["id"]:
                continue
            if m.get(f) is not None:
                out[f] = m[f]
                break
    return out


SPLIT_KM = 15.0


def _has_geo(s: dict) -> bool:
    return s.get("latitude") is not None and s.get("longitude") is not None


def _disambiguated_key(name_key: str, s: dict) -> str:
    return f"{name_key}#{round(float(s['latitude']), 2)},{round(float(s['longitude']), 2)}"


def _greedy_geo_clusters(members: list[dict]) -> list[list[dict]]:
    """Single-linkage-ish: seed clusters by representatives > SPLIT_KM apart.
    Deterministic (input order). Non-geo members attach to the first cluster.
    """
    clusters: list[list[dict]] = []
    reps: list[dict] = []
    for m in members:
        if not _has_geo(m):
            continue
        placed = False
        for i, r in enumerate(reps):
            if haversine_km(float(m["latitude"]), float(m["longitude"]),
                            float(r["latitude"]), float(r["longitude"])) <= SPLIT_KM:
                clusters[i].append(m)
                placed = True
                break
        if not placed:
            reps.append(m)
            clusters.append([m])
    if not clusters:  # no geo at all
        clusters = [[]]
    for m in members:  # attach geo-less members to first cluster
        if not _has_geo(m):
            clusters[0].append(m)
    return clusters


def plan_site_canonicalization(sites: list[dict]) -> list[dict]:
    """One plan per duplicate-name group (size > 1). See spec §3.1/§4.1."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for site in sites:
        groups[normalize_site_key(site.get("name"))].append(site)

    plans: list[dict] = []
    for name_key, members in groups.items():
        if not name_key or len(members) <= 1:
            continue
        node_ids = [m["id"] for m in members]
        clusters = _greedy_geo_clusters(members)
        if len(clusters) <= 1:
            survivor = _pick_survivor(members)
            plans.append({
                "name": name_key, "action": "merge", "site_key": name_key,
                "node_ids": node_ids, "survivor_id": survivor["id"],
                "merge_ids": [m["id"] for m in members if m["id"] != survivor["id"]],
                "backfill": _backfill(survivor, members),
            })
        else:  # dormant: same name, geo proves > SPLIT_KM apart
            plans.append({
                "name": name_key, "action": "split", "node_ids": node_ids,
                "clusters": [{
                    "site_key": _disambiguated_key(name_key, _pick_survivor(c)),
                    "survivor_id": _pick_survivor(c)["id"],
                    "member_ids": [m["id"] for m in c],
                } for c in clusters],
            })
    return plans


def fetch_trial_sites(driver) -> list[dict]:
    """Load every TrialSite (element id + name + richness fields) from Neo4j."""
    fields = ", ".join("t.%s AS %s" % (f, f) for f in RICHNESS_FIELDS)
    query = "MATCH (t:TrialSite) RETURN elementId(t) AS id, t.name AS name, " + fields
    with driver.session() as session:
        return [dict(r) for r in session.run(query)]


def apply_site_canonicalization(driver, plans: list[dict], dry_run: bool = True) -> dict:
    """Execute (or, when dry_run, only report) the canonicalization plan.

    Merge: backfill survivor nulls, then apoc.refactor.mergeNodes keeping the
    survivor first (mergeRels:true reattaches TRIAL_AT). Flag: mark all group
    members needsHumanReview. Idempotent: canonical input yields an empty plan.
    """
    merge_plans = [p for p in plans if p["action"] == "merge"]
    flag_plans = [p for p in plans if p["action"] == "flag"]
    summary = {
        "merged_groups": len(merge_plans),
        "flagged_groups": len(flag_plans),
        "removed_nodes": sum(len(p["merge_ids"]) for p in merge_plans),
    }
    if dry_run:
        return summary

    with driver.session() as session:
        for p in merge_plans:
            if p["backfill"]:
                session.run(
                    "MATCH (s:TrialSite) WHERE elementId(s)=$sid SET s += $bf",
                    sid=p["survivor_id"], bf=p["backfill"],
                )
            session.run(
                """
                MATCH (surv:TrialSite) WHERE elementId(surv)=$sid
                MATCH (m:TrialSite) WHERE elementId(m) IN $mids
                WITH surv, collect(m) AS ms
                CALL apoc.refactor.mergeNodes([surv] + ms,
                    {mergeRels: true, properties: 'discard'}) YIELD node
                RETURN elementId(node)
                """,
                sid=p["survivor_id"], mids=p["merge_ids"],
            )
        for p in flag_plans:
            session.run(
                "MATCH (t:TrialSite) WHERE elementId(t) IN $ids SET t.needsHumanReview = true",
                ids=p["node_ids"],
            )
    return summary
