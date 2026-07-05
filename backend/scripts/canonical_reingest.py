"""Gate-first canonical re-ingest for adequate JSON-LD bundles.

Supports source-specific ingesters (Navarra, INIAV). Legacy prod nodes may use
short mergeKeys without the ``eppo:`` canonical segment; re-MERGE without
removing them would duplicate VarietyTrials.

Flow:
  1. Validate bundle (gate must pass)
  2. Print baseline + transform stats
  3. Refuse --execute when legacy-shaped mergeKeys exist unless --purge-legacy
  4. Optionally purge all source nodes (--purge-legacy)
  5. MERGE via BaseIngester

Usage:
    python -m scripts.canonical_reingest navarra /path/to/all_trials_adequate.jsonld --dry-run
    python -m scripts.canonical_reingest navarra /path/to/bundle.jsonld --purge-legacy --execute
    python -m scripts.canonical_reingest iniav /path/to/all_trials_adequate.jsonld --execute
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Callable

from neo4j import AsyncGraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ingestion.base_ingester import BaseIngester, NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER
from app.ingestion.genvce_ingester import GenvceIngester
from app.ingestion.ifapa_ingester import IfapaIngester
from app.ingestion.iniav_ingester import IniavIngester
from app.ingestion.intia_exp_ingester import IntiaExpIngester
from app.ingestion.itacyl_ingester import ItacylIngester
from app.ingestion.navarra_ingester import NavarraIngester
from app.ingestion.validate_ingest_bundle import validate_bundle

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

IngesterFactory = Callable[..., BaseIngester]

SOURCES: dict[str, tuple[IngesterFactory, str]] = {
    "navarra": (NavarraIngester, NavarraIngester.SOURCE_ID),
    "iniav": (IniavIngester, IniavIngester.SOURCE_ID),
    "genvce": (GenvceIngester, GenvceIngester.SOURCE_ID),
    "ifapa": (IfapaIngester, IfapaIngester.SOURCE_ID),
    "itacyl": (ItacylIngester, ItacylIngester.SOURCE_ID),
    "intia": (IntiaExpIngester, IntiaExpIngester.SOURCE_ID),
}

LEGACY_MERGEKEY_MARKER = "eppo:"


async def _baseline(driver, source_id: str) -> dict:
    async with driver.session() as s:
        async def one(q: str, **params) -> int:
            row = await (await s.run(q, **params)).single()
            return int(row["c"])

        return {
            "vt_total": await one("MATCH (v:VarietyTrial) RETURN count(v) AS c"),
            "vt_source": await one(
                "MATCH (v:VarietyTrial {source_id: $sid}) RETURN count(v) AS c", sid=source_id
            ),
            "mt_source": await one(
                "MATCH (m:ManagementTrial {source_id: $sid}) RETURN count(m) AS c", sid=source_id
            ),
            "ts_source": await one(
                "MATCH (t:TrialSite {source_id: $sid}) RETURN count(t) AS c", sid=source_id
            ),
            "legacy_mergekeys": await one(
                "MATCH (v:VarietyTrial {source_id: $sid}) "
                "WHERE v.mergeKey IS NULL OR NOT v.mergeKey CONTAINS $marker "
                "RETURN count(v) AS c",
                sid=source_id,
                marker=LEGACY_MERGEKEY_MARKER,
            ),
            "orphan_vt": await one(
                "MATCH (v:VarietyTrial {source_id: $sid}) "
                "WHERE NOT (v)-[:TRIAL_AT]->(:TrialSite) RETURN count(v) AS c",
                sid=source_id,
            ),
            "trial_at": await one(
                "MATCH (:VarietyTrial {source_id: $sid})-[:TRIAL_AT]->(:TrialSite) "
                "RETURN count(*) AS c",
                sid=source_id,
            ),
        }


async def _purge_source(driver, source_id: str) -> dict:
    """Remove all trial nodes for a source_id (migration step)."""
    async with driver.session() as s:
        vt = await (await s.run(
            "MATCH (v:VarietyTrial {source_id: $sid}) DETACH DELETE v RETURN count(v) AS c",
            sid=source_id,
        )).single()
        mt = await (await s.run(
            "MATCH (m:ManagementTrial {source_id: $sid}) DETACH DELETE m RETURN count(m) AS c",
            sid=source_id,
        )).single()
        as_del = await (await s.run(
            """
            MATCH (a:ArticleSource {source_id: $sid})
            WHERE NOT (a)<-[:SOURCED_FROM]-()
            DETACH DELETE a
            RETURN count(a) AS c
            """,
            sid=source_id,
        )).single()
        return {
            "purged_variety_trials": vt["c"],
            "purged_management_trials": mt["c"],
            "purged_orphan_articles": as_del["c"],
        }


async def run(
    source: str,
    bundle_path: str,
    *,
    dry_run: bool,
    purge_legacy: bool,
    execute: bool,
) -> int:
    if source not in SOURCES:
        logger.error("Unknown source %r — choose from %s", source, ", ".join(SOURCES))
        return 1

    factory, source_id = SOURCES[source]
    path = Path(bundle_path)
    if not path.is_file():
        logger.error("Bundle not found: %s", path)
        return 1

    report = validate_bundle(str(path))
    logger.info(
        "Gate: %s — errors=%d warnings=%d stats=%s",
        "PASS" if report.ok else "FAIL",
        len(report.errors()),
        len(report.warnings()),
        report.stats,
    )
    if not report.ok:
        for err in report.errors()[:10]:
            logger.error("%s %s", err.code, err.message)
        return 1

    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    ingester = factory(driver=driver)

    try:
        before = await _baseline(driver, source_id)
        logger.info("Baseline (%s): %s", source_id, before)

        nodes = await ingester.transform(str(path))
        transform_counts = {k: len(v) for k, v in nodes.items()}
        logger.info("Transform: %s", transform_counts)

        if dry_run or not execute:
            logger.info("DRY RUN — no writes (pass --execute to merge)")
            return 0

        if before["legacy_mergekeys"] > 0 and not purge_legacy:
            logger.error(
                "Refusing --execute: %d legacy-shaped mergeKeys for %s "
                "(pass --purge-legacy to migrate)",
                before["legacy_mergekeys"],
                source_id,
            )
            return 1

        if purge_legacy:
            purged = await _purge_source(driver, source_id)
            logger.info("Purged: %s", purged)

        variety_rows = nodes.get("variety_trials", [])
        expected_unique = {
            uk
            for v in variety_rows
            if not v.get("skip_ingestion")
            for uk in [BaseIngester._variety_unique_key(v)]
            if uk
        }

        merge_stats = await ingester.merge(nodes)
        logger.info("Merge: %s", merge_stats)

        after = await _baseline(driver, source_id)
        logger.info("After (%s): %s", source_id, after)

        if after["orphan_vt"] != 0:
            logger.error("Post-ingest orphan VarietyTrials: %d", after["orphan_vt"])
            return 1

        if after["vt_source"] < len(expected_unique):
            logger.error(
                "VarietyTrial nodes %d < expected unique identities %d",
                after["vt_source"],
                len(expected_unique),
            )
            return 1

        if after["trial_at"] < len(expected_unique):
            logger.error(
                "TRIAL_AT links %d < expected unique identities %d",
                after["trial_at"],
                len(expected_unique),
            )
            return 1

        # Auto-canonicalize sites: a source-scoped ingest re-creates per-source
        # TrialSite duplicates; collapse same-name sites so re-ingest stays
        # idempotent and cannot re-duplicate the graph (incident 2026-07-04).
        from scripts.canonicalize_trial_sites import run as _canonicalize_sites

        canon = await asyncio.to_thread(_canonicalize_sites, execute=True)
        logger.info("Site canonicalization: %s", canon)

        logger.info("Canonical re-ingest OK (%s)", source_id)
        return 0
    finally:
        await driver.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Gate-first canonical bundle re-ingest")
    parser.add_argument(
        "source",
        choices=sorted(SOURCES),
        help="Source ingester (navarra, iniav, …)",
    )
    parser.add_argument("bundle", help="Path to adequate JSON-LD bundle")
    parser.add_argument("--dry-run", action="store_true", help="Validate + transform only")
    parser.add_argument(
        "--purge-legacy",
        action="store_true",
        help="Delete existing source trial nodes before merge (migration)",
    )
    parser.add_argument("--execute", action="store_true", help="Write to Neo4j")
    args = parser.parse_args()

    code = asyncio.run(
        run(
            args.source,
            args.bundle,
            dry_run=args.dry_run,
            purge_legacy=args.purge_legacy,
            execute=args.execute,
        )
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
