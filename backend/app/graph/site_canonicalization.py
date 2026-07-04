"""TrialSite canonicalization planning (0.4 spec, idempotent).

Pure planning separated from Neo4j execution so the decision logic is unit
testable without a database. Groups TrialSites by normalized name; a group whose
members disagree on municipality is flagged ``needsHumanReview`` (not merged),
otherwise it is merged into the richest survivor with null backfill from siblings.
"""

from __future__ import annotations

from collections import defaultdict

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


def plan_site_canonicalization(sites: list[dict]) -> list[dict]:
    """Return one plan per duplicate-name group (size > 1).

    Each site dict carries: id, name, municipality, and the richness fields
    climateClass, latitude, soilTexture, annualRainfallMm.
    """
    groups: dict[str, list[dict]] = defaultdict(list)
    for site in sites:
        groups[_norm(site.get("name"))].append(site)

    plans: list[dict] = []
    for name, members in groups.items():
        if len(members) <= 1:
            continue
        node_ids = [m["id"] for m in members]
        distinct_munis = {_norm(m.get("municipality")) for m in members if _norm(m.get("municipality"))}
        if len(distinct_munis) >= 2:
            plans.append({
                "name": name,
                "action": "flag",
                "node_ids": node_ids,
                "survivor_id": None,
            })
            continue
        survivor = _pick_survivor(members)
        merge_ids = [m["id"] for m in members if m["id"] != survivor["id"]]
        plans.append({
            "name": name,
            "action": "merge",
            "node_ids": node_ids,
            "survivor_id": survivor["id"],
            "merge_ids": merge_ids,
            "backfill": _backfill(survivor, members),
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
