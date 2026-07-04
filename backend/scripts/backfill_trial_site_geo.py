"""
Backfill latitude/longitude (and optional agro-climatic fields) on TrialSite
nodes that were ingested without coordinates.

Uses ``trial_site_geo_registry.json`` — no live geocoding at runtime.

Usage:
    python -m scripts.backfill_trial_site_geo --dry-run
    python -m scripts.backfill_trial_site_geo
    python -m scripts.backfill_trial_site_geo --force-names "Gut Ving (Nörvenich),Bembeke"
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from neo4j import AsyncGraphDatabase

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.ingestion.trial_site_geo import (
    geo_updates_for_neo4j,
    is_aggregate_site_name,
    resolve_trial_site_geo,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://bioorchestrator-neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "bioorchestrator")


async def backfill(
    driver,
    dry_run: bool,
    *,
    force_names: set[str] | None = None,
) -> tuple[int, int, int]:
    """Returns (updated, skipped_aggregate, no_registry)."""
    updated = skipped_agg = no_registry = 0
    force_names = {n.strip().lower() for n in (force_names or set())}

    async with driver.session() as session:
        if force_names:
            result = await session.run(
                """
                MATCH (ts:TrialSite)
                WHERE ts.name IS NOT NULL
                RETURN ts.mergeKey AS mk, ts.name AS name
                ORDER BY ts.name
                """
            )
        else:
            result = await session.run(
                """
                MATCH (ts:TrialSite)
                WHERE ts.latitude IS NULL AND ts.name IS NOT NULL
                RETURN ts.mergeKey AS mk, ts.name AS name
                ORDER BY ts.name
                """
            )
        rows = [dict(r) async for r in result]

        logger.info("TrialSites to process: %d", len(rows))

        for row in rows:
            name = row["name"]
            if force_names and name.strip().lower() not in force_names:
                continue
            if not force_names and is_aggregate_site_name(name):
                skipped_agg += 1
                logger.debug("aggregate skip: %s", name)
                continue

            entry = resolve_trial_site_geo(name)
            if not entry:
                no_registry += 1
                logger.warning("no registry entry: %s", name)
                continue

            updates = geo_updates_for_neo4j(entry)
            if dry_run:
                logger.info(
                    "DRY RUN %s -> lat=%s lon=%s conf=%s",
                    name,
                    updates.get("latitude"),
                    updates.get("longitude"),
                    updates.get("geoConfidence"),
                )
                updated += 1
                continue

            await session.run(
                """
                MATCH (ts:TrialSite {mergeKey: $mk})
                SET ts += $updates,
                    ts.updatedAt = datetime()
                """,
                mk=row["mk"],
                updates=updates,
            )
            updated += 1

    return updated, skipped_agg, no_registry


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill TrialSite geo from registry")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument(
        "--force-names",
        help="Comma-separated TrialSite names to refresh even if coords exist",
    )
    args = parser.parse_args()

    force = None
    if args.force_names:
        force = {n.strip() for n in args.force_names.split(",") if n.strip()}

    logger.info("TrialSite geo backfill %s", "DRY RUN" if args.dry_run else "FOR REAL")

    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        updated, skipped, missing = await backfill(driver, args.dry_run, force_names=force)
        logger.info(
            "Done: updated=%d aggregate_skip=%d no_registry=%d",
            updated,
            skipped,
            missing,
        )
    finally:
        await driver.close()


if __name__ == "__main__":
  asyncio.run(main())
