"""Canonicalize duplicate-name TrialSites (0.4 spec), idempotent, dry-run by default.

Merges same-name TrialSites into the richest survivor (TRIAL_AT reattached via
apoc.refactor.mergeNodes); groups whose members disagree on municipality are
flagged needsHumanReview instead of merged. Safe to re-run.

Usage (inside a pod with NEO4J_* env set):
    python -m scripts.canonicalize_trial_sites            # dry-run (default)
    python -m scripts.canonicalize_trial_sites --execute  # apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from neo4j import GraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.graph.site_canonicalization import (
    apply_site_canonicalization,
    fetch_trial_sites,
    plan_site_canonicalization,
)
from app.ingestion.base_ingester import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Canonicalize duplicate-name TrialSites.")
    ap.add_argument("--execute", action="store_true",
                    help="apply changes (default: dry-run, no mutation)")
    return ap.parse_args(argv)


def run(execute: bool = False, driver=None) -> dict:
    own_driver = driver is None
    if own_driver:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        sites = fetch_trial_sites(driver)
        plans = plan_site_canonicalization(sites)
        logger.info("TrialSites: %d | duplicate-name groups: %d", len(sites), len(plans))
        for p in plans:
            if p["action"] == "merge":
                logger.info("  MERGE  %-32s %d -> 1", p["name"], len(p["node_ids"]))
            else:
                logger.info("  FLAG   %-32s %d (needsHumanReview)", p["name"], len(p["node_ids"]))
        summary = apply_site_canonicalization(driver, plans, dry_run=not execute)
        mode = "EXECUTED" if execute else "DRY-RUN (no changes)"
        logger.info("%s | merged_groups=%d removed_nodes=%d flagged_groups=%d",
                    mode, summary["merged_groups"], summary["removed_nodes"], summary["flagged_groups"])
        if not execute:
            logger.info("predicted TrialSites after execute: %d",
                        len(sites) - summary["removed_nodes"])
        return summary
    finally:
        if own_driver:
            driver.close()


def main() -> None:
    args = parse_args()
    run(execute=args.execute)


if __name__ == "__main__":
    main()
