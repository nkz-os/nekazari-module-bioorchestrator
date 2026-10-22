"""Gate-first ingest for the multi-source fungi bundle.

Dedicated (not canonical_reingest) because that script's baseline/purge/validation
are hard-scoped to one source_id and would be blind to a multi-source batch.

Usage:
    python -m scripts.ingest_fungi /path/to/vision_2024_fungi.jsonld            # dry-run
    python -m scripts.ingest_fungi /path/to/vision_2024_fungi.jsonld --execute  # write
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from neo4j import AsyncGraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ingestion.base_ingester import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER
from app.ingestion.fungi_ingester import FungiIngester
from app.ingestion.validate_ingest_bundle import validate_bundle

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def _orphans(driver, sids: list[str]) -> int:
    async with driver.session() as s:
        row = await (await s.run(
            "MATCH (v:VarietyTrial) WHERE v.source_id IN $sids "
            "AND NOT (v)-[:TRIAL_AT]->(:TrialSite) RETURN count(v) AS c",
            sids=sids,
        )).single()
        return int(row["c"])


async def _unlinked_mt_with_location(driver, sids: list[str]) -> int:
    """Count ManagementTrials that carry a location but did not link to a site.

    Visibility only — NOT a gate. MT→site linkage is best-effort (management
    trials often cite loose locations), so an unlinked MT is logged as a warning,
    never a hard failure; MTs with a null location are expected and excluded.
    """
    async with driver.session() as s:
        row = await (await s.run(
            "MATCH (m:ManagementTrial) WHERE m.source_id IN $sids "
            "AND m.trialLocation IS NOT NULL "
            "AND NOT (m)-[:TRIAL_AT]->(:TrialSite) RETURN count(m) AS c",
            sids=sids,
        )).single()
        return int(row["c"])


async def run(bundle_path: str, *, execute: bool) -> int:
    path = Path(bundle_path)
    if not path.is_file():
        logger.error("Bundle not found: %s", path)
        return 1

    report = validate_bundle(str(path))
    logger.info("Gate: %s — errors=%d warnings=%d",
                "PASS" if report.ok else "FAIL",
                len(report.errors()), len(report.warnings()))
    if not report.ok:
        for err in report.errors()[:10]:
            logger.error("%s %s", err.code, err.message)
        return 1

    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) if execute else None
    ingester = FungiIngester(driver=driver)
    try:
        nodes = await ingester.transform(str(path))
        logger.info("Transform: %s", {k: len(v) for k, v in nodes.items()})
        sids = sorted({n["source_id"] for n in
                       nodes["variety_trials"] + nodes["management_trials"]
                       if n.get("source_id")})
        logger.info("Batch source_ids: %s", sids)

        if not execute:
            logger.info("DRY RUN — no writes (pass --execute to merge)")
            return 0

        stats = await ingester.merge(nodes)
        logger.info("Merge: %s", stats)

        orphans = await _orphans(driver, sids)
        if orphans:
            logger.error("Post-ingest orphan VarietyTrials: %d", orphans)
            return 1

        unlinked_mt = await _unlinked_mt_with_location(driver, sids)
        if unlinked_mt:
            logger.warning(
                "ManagementTrials with a location that did not link: %d "
                "(visibility only — MT linkage is best-effort, not a gate)",
                unlinked_mt,
            )

        from scripts.canonicalize_trial_sites import run as _canon
        logger.info("Site canonicalization: %s", await asyncio.to_thread(_canon, execute=True))
        logger.info("Fungi ingest OK")
        return 0
    finally:
        if driver is not None:
            await driver.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Gate-first fungi bundle ingest")
    ap.add_argument("bundle")
    ap.add_argument("--execute", action="store_true", help="Write to Neo4j")
    args = ap.parse_args()
    sys.exit(asyncio.run(run(args.bundle, execute=args.execute)))


if __name__ == "__main__":
    main()
