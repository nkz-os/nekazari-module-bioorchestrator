"""
Backfill normalisation for all existing VarietyTrial and ManagementTrial nodes
in Neo4j that were ingested before the normalisation pipeline was introduced.

Applies the same transformations as ``normalize_nodes()`` in BaseIngester:
  - Normalises variety names (uppercase, strip parenthetical tags)
  - Fills missing cropScientific from EPPO codes
  - Normalises trialLocation strings
  - Translates agronomic traits and disease scores to unified vocab
  - Generates canonical mergeKeyNormalized
  - Validates required fields

Usage:
    python -m scripts.backfill_normalization --dry-run          # preview
    python -m scripts.backfill_normalization --batch-size 1000  # for real
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from neo4j import AsyncGraphDatabase

# Ensure we can import from app
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.ingestion.normalization_registry import (
    normalize_variety_name,
    normalize_location,
    eppo_to_scientific,
    normalize_merge_key,
    transform_traits_to_unified,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://bioorchestrator-neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "bioorchestrator")


async def backfill_variety_trials(driver, dry_run: bool, batch_size: int) -> int:
    """Backfill VarietyTrial nodes. Returns count of processed nodes."""
    processed = 0
    errors = 0

    async with driver.session() as session:
        # Count total
        result = await session.run(
            "MATCH (v:VarietyTrial) WHERE v.mergeKeyNormalized IS NULL "
            "RETURN count(v) AS c"
        )
        row = await result.single()
        pending = row["c"]
        logger.info("VarietyTrial pending: %d", pending)

        if pending == 0:
            logger.info("All VarietyTrial nodes already normalised.")
            return 0

        skip = 0
        while skip < pending:
            result = await session.run(
                "MATCH (v:VarietyTrial) "
                "WHERE v.mergeKeyNormalized IS NULL "
                "RETURN v SKIP $skip LIMIT $batch",
                skip=skip, batch=batch_size,
            )
            nodes = [dict(row["v"]) async for row in result]
            if not nodes:
                break

            logger.info("  Batch %d-%d (%d nodes)", skip, skip + len(nodes), len(nodes))
            skip += len(nodes)

            for node in nodes:
                try:
                    updates = _normalise_vt(node)
                    if updates and not dry_run:
                        await session.run(
                            "MATCH (v:VarietyTrial {mergeKey: $mk}) SET v += $updates",
                            mk=node.get("mergeKey"),
                            updates=updates,
                        )
                    processed += 1
                except Exception as e:
                    logger.error("  Error processing %s: %s", node.get("mergeKey"), e)
                    errors += 1

            if dry_run:
                break  # one batch only in dry-run

    logger.info("VarietyTrial done: %d processed, %d errors", processed, errors)
    return processed


async def backfill_management_trials(driver, dry_run: bool, batch_size: int) -> int:
    """Backfill ManagementTrial nodes."""
    processed = 0
    errors = 0

    async with driver.session() as session:
        result = await session.run(
            "MATCH (m:ManagementTrial) WHERE m.varietyNormalized IS NULL "
            "RETURN count(m) AS c"
        )
        row = await result.single()
        pending = row["c"]
        logger.info("ManagementTrial pending: %d", pending)

        if pending == 0:
            return 0

        skip = 0
        while skip < pending:
            result = await session.run(
                "MATCH (m:ManagementTrial) "
                "WHERE m.varietyNormalized IS NULL "
                "RETURN m SKIP $skip LIMIT $batch",
                skip=skip, batch=batch_size,
            )
            nodes = [dict(row["m"]) async for row in result]
            if not nodes:
                break

            logger.info("  Batch %d-%d (%d nodes)", skip, skip + len(nodes), len(nodes))
            skip += len(nodes)

            for node in nodes:
                try:
                    updates = _normalise_mt(node)
                    if updates and not dry_run:
                        await session.run(
                            "MATCH (m:ManagementTrial {mergeKey: $mk}) SET m += $updates",
                            mk=node.get("mergeKey"),
                            updates=updates,
                        )
                    processed += 1
                except Exception as e:
                    logger.error("  Error: %s", e)
                    errors += 1

            if dry_run:
                break

    logger.info("ManagementTrial done: %d processed, %d errors", processed, errors)
    return processed


def _normalise_vt(node: dict) -> dict:
    """Build updates dict for a VarietyTrial node."""
    updates = {}
    source_id = node.get("source_id") or "UNKN"

    # 1. Variety name
    raw_var = node.get("variety")
    if raw_var:
        updates["varietyNormalized"] = normalize_variety_name(raw_var)

    # 2. cropScientific from EPPO
    if not node.get("cropScientific") and node.get("cropEppo"):
        sci = eppo_to_scientific(node["cropEppo"])
        if sci:
            updates["cropScientific"] = sci

    # 3. Location
    raw_loc = node.get("trialLocation")
    loc_info = normalize_location(raw_loc)
    if loc_info:
        updates["locationNormalized"] = loc_info["name"]
        updates["locationCountry"] = loc_info["country"]
        if loc_info["climateClass"]:
            updates["climateClass"] = loc_info["climateClass"]

    # 4. Traits
    traits_raw = node.get("agronomicTraits")
    disease_raw = node.get("diseaseScores")
    if traits_raw or disease_raw:
        t_norm, d_norm = transform_traits_to_unified(
            traits_raw, disease_raw, source_id,
        )
        if t_norm:
            updates["agronomicTraitsUnified"] = t_norm
        if d_norm:
            updates["diseaseScoresUnified"] = d_norm

    # 5. MergeKey normalized
    updates["mergeKeyNormalized"] = normalize_merge_key(
        source_id=source_id,
        eppo=node.get("cropEppo"),
        variety=raw_var,
        year=node.get("year"),
        location=raw_loc,
    )

    # 6. Validation
    missing = []
    if not node.get("cropEppo"):
        missing.append("cropEppo")
    if not raw_var:
        missing.append("variety")
    if not node.get("year") or node.get("year", 0) <= 1900:
        missing.append("year")
    updates["_validationPassed"] = len(missing) == 0

    return updates


def _normalise_mt(node: dict) -> dict:
    """Build updates dict for a ManagementTrial node."""
    updates = {}

    raw_var = node.get("variety")
    if raw_var:
        updates["varietyNormalized"] = normalize_variety_name(raw_var)

    raw_loc = node.get("trialLocation")
    loc_info = normalize_location(raw_loc)
    if loc_info:
        updates["locationNormalized"] = loc_info["name"]
        updates["locationCountry"] = loc_info["country"]

    # Unit resolution (best-effort)
    unit_str = node.get("resultUnit")
    if unit_str:
        try:
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
            from app.ingestion.semantic_mappings import get_qudt_unit
            qudt_info = get_qudt_unit(unit_str)
            if qudt_info:
                updates["unitQudtUri"] = qudt_info.get("qudt_uri")
                updates["unitUcum"] = qudt_info.get("ucum")
        except ImportError:
            pass

    return updates


async def main():
    parser = argparse.ArgumentParser(
        description="Backfill normalisation for existing Neo4j nodes",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no writes")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size")
    parser.add_argument("--vt-only", action="store_true", help="Only backfill VarietyTrial")
    args = parser.parse_args()

    logger.info(
        "Backfill %s (batch=%d)",
        "DRY RUN" if args.dry_run else "FOR REAL",
        args.batch_size,
    )

    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    try:
        total = 0
        total += await backfill_variety_trials(driver, args.dry_run, args.batch_size)
        if not args.vt_only:
            total += await backfill_management_trials(driver, args.dry_run, args.batch_size)
        logger.info("Total nodes processed: %d", total)
    finally:
        await driver.close()

    if args.dry_run:
        logger.info("Dry-run complete. Re-run without --dry-run to write changes.")


if __name__ == "__main__":
    asyncio.run(main())
