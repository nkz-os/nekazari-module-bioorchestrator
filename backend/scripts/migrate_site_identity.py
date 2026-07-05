"""In-place TrialSite identity migration (spec §4.3).

Sets siteKey on every TrialSite, collapses duplicate-name groups by the
geo-guard, reattaches TRIAL_AT (apoc mergeRels). Fails loud if any
VarietyTrial is lost. Dry-run by default; --execute gated.

Back up Neo4j before --execute.
"""

from __future__ import annotations

import argparse
import logging
import os

from neo4j import GraphDatabase

from app.graph.site_canonicalization import (
    apply_site_canonicalization,
    fetch_trial_sites,
    normalize_site_key,
    plan_site_canonicalization,
)

logger = logging.getLogger("migrate_site_identity")
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://bioorchestrator-neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "bioorchestrator")


def _counts(driver) -> dict:
    with driver.session() as s:
        return {
            "vt": s.run(
                "MATCH (v) WHERE v:VarietyTrial OR v:ManagementTrial RETURN count(v) AS c"
            ).single()["c"],
            "sites": s.run("MATCH (t:TrialSite) RETURN count(t) AS c").single()["c"],
            "orphans": s.run(
                "MATCH (v) WHERE (v:VarietyTrial OR v:ManagementTrial) "
                "AND NOT (v)-[:TRIAL_AT]->() RETURN count(v) AS c"
            ).single()["c"],
        }


def _set_sitekeys(driver) -> None:
    sites = fetch_trial_sites(driver)
    updates = [
        {
            "id": s["id"],
            "k": normalize_site_key(s.get("name")),
            "mk": normalize_site_key(s.get("municipality")),
        }
        for s in sites
    ]
    with driver.session() as s:
        s.run(
            """
            UNWIND $updates AS u
            MATCH (t:TrialSite) WHERE elementId(t) = u.id
            SET t.siteKey = u.k, t.municipalityKey = u.mk
            """,
            updates=updates,
        )


def run(execute: bool = False, driver=None) -> dict:
    own = driver is None
    if own:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        before = _counts(driver)
        if execute:
            _set_sitekeys(driver)
        plans = plan_site_canonicalization(fetch_trial_sites(driver))
        summary = apply_site_canonicalization(driver, plans, dry_run=not execute)
        after = _counts(driver) if execute else before
        result = {
            "vt_before": before["vt"],
            "vt_after": after["vt"],
            "sites_before": before["sites"],
            "sites_after": after["sites"],
            "orphans_before": before["orphans"],
            "orphans_after": after["orphans"],
            **summary,
        }
        mode = "EXECUTED" if execute else "DRY-RUN (no changes)"
        logger.info(
            "%s | merged=%d split=%d removed=%d vt %d→%d sites %d→%d",
            mode,
            result["merged_groups"],
            result["split_groups"],
            result["removed_nodes"],
            result["vt_before"],
            result["vt_after"],
            result["sites_before"],
            result["sites_after"],
        )
        if execute:
            assert after["vt"] == before["vt"], (
                f"Trial count changed {before['vt']}→{after['vt']} "
                f"(VarietyTrial + ManagementTrial)"
            )
            assert after["orphans"] <= before["orphans"], (
                "orphan trials increased (VarietyTrial + ManagementTrial)"
            )
        return result
    finally:
        if own:
            driver.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="In-place TrialSite identity migration.")
    ap.add_argument("--execute", action="store_true", help="apply (default: dry-run)")
    run(execute=ap.parse_args().execute)


if __name__ == "__main__":
    main()
