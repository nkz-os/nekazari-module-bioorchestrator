#!/usr/bin/env python3
"""Remove Navarra pre-canonical graph residue after perennial MERGE.

Targets:
  1. Cárcar PRNDU year=2011 rows with agrovoc mergeKeys (cumulative totals mis-tagged as annual).
  2. Duplicate VarietyTrial nodes from double perennial ingest: same short mergeKey base
     (NAVARRA-AGRARIA|eppo:…|…|year) with distinct 12-char content hashes.

Keeps the lexicographically smallest mergeKey per base group (deterministic).

Usage:
    PYTHONPATH=. python3 scripts/purge_navarra_precanonical_residual.py --dry-run
    PYTHONPATH=. python3 scripts/purge_navarra_precanonical_residual.py --execute
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from neo4j import AsyncGraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ingestion.base_ingester import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "NAVARRA-AGRARIA"
HASH_SUFFIX_RE = r".*\|[0-9a-f]{12}$"


async def _count_carcar_agrovoc(session) -> int:
    row = await (
        await session.run(
            """
            MATCH (v:VarietyTrial {source_id: $sid})
            WHERE toLower(v.trialLocation) CONTAINS 'cárcar'
              AND v.mergeKey CONTAINS 'agrovoc'
            RETURN count(v) AS c
            """,
            sid=SOURCE_ID,
        )
    ).single()
    return int(row["c"])


async def _count_hash_dupes(session) -> dict:
    row = await (
        await session.run(
            """
            MATCH (v:VarietyTrial {source_id: $sid})
            WHERE v.mergeKey STARTS WITH 'NAVARRA-AGRARIA|eppo:'
              AND v.mergeKey =~ $hash_re
            WITH v,
                 CASE
                   WHEN v.mergeKey =~ $hash_re
                   THEN substring(v.mergeKey, 0, size(v.mergeKey) - 13)
                   ELSE v.mergeKey
                 END AS baseKey
            WITH baseKey, collect(v) AS nodes
            WHERE size(nodes) > 1
            RETURN count(baseKey) AS groups, sum(size(nodes) - 1) AS to_delete
            """,
            sid=SOURCE_ID,
            hash_re=HASH_SUFFIX_RE,
        )
    ).single()
    return {"groups": int(row["groups"]), "to_delete": int(row["to_delete"])}


async def _delete_carcar_agrovoc(session, *, execute: bool) -> int:
    if not execute:
        return await _count_carcar_agrovoc(session)
    row = await (
        await session.run(
            """
            MATCH (v:VarietyTrial {source_id: $sid})
            WHERE toLower(v.trialLocation) CONTAINS 'cárcar'
              AND v.mergeKey CONTAINS 'agrovoc'
            WITH collect(v) AS nodes, count(v) AS c
            FOREACH (n IN nodes | DETACH DELETE n)
            RETURN c
            """,
            sid=SOURCE_ID,
        )
    ).single()
    return int(row["c"])


async def _dedupe_hash_suffix(session, *, execute: bool) -> int:
    if not execute:
        stats = await _count_hash_dupes(session)
        return int(stats["to_delete"])
    row = await (
        await session.run(
            """
            MATCH (v:VarietyTrial {source_id: $sid})
            WHERE v.mergeKey STARTS WITH 'NAVARRA-AGRARIA|eppo:'
              AND v.mergeKey =~ $hash_re
            WITH v,
                 CASE
                   WHEN v.mergeKey =~ $hash_re
                   THEN substring(v.mergeKey, 0, size(v.mergeKey) - 13)
                   ELSE v.mergeKey
                 END AS baseKey
            WITH baseKey, collect(v) AS nodes
            WHERE size(nodes) > 1
            UNWIND nodes AS n
            WITH baseKey, n
            ORDER BY baseKey, n.mergeKey ASC
            WITH baseKey, collect(n) AS sorted
            WITH sorted[0] AS keeper, sorted[1..] AS dups
            UNWIND dups AS dup
            DETACH DELETE dup
            RETURN count(dup) AS deleted
            """,
            sid=SOURCE_ID,
            hash_re=HASH_SUFFIX_RE,
        )
    ).single()
    return int(row["deleted"])


async def _post_stats(session) -> dict:
    carcar = await (
        await session.run(
            """
            MATCH (v:VarietyTrial {source_id: $sid})
            WHERE toLower(v.trialLocation) CONTAINS 'cárcar' AND v.cropEppo = 'PRNDU'
            RETURN count(v) AS vt,
                   sum(CASE WHEN NOT (v)-[:TRIAL_AT]->() THEN 1 ELSE 0 END) AS orphans,
                   min(v.yieldKgHa) AS ymin,
                   max(v.yieldKgHa) AS ymax
            """,
            sid=SOURCE_ID,
        )
    ).single()
    navarra = await (
        await session.run(
            "MATCH (v:VarietyTrial {source_id: $sid}) RETURN count(v) AS c",
            sid=SOURCE_ID,
        )
    ).single()
    return {
        "navarra_vt": int(navarra["c"]),
        "carcar_prndu_vt": int(carcar["vt"]),
        "carcar_orphans": int(carcar["orphans"]),
        "carcar_yield_min": carcar["ymin"],
        "carcar_yield_max": carcar["ymax"],
    }


async def run(*, execute: bool) -> int:
    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        async with driver.session() as session:
            before = await _post_stats(session)
            dup_stats = await _count_hash_dupes(session)
            agrovoc_n = await _count_carcar_agrovoc(session)
            logger.info("Before: %s", before)
            logger.info(
                "Plan: delete %d Cárcar agrovoc rows; dedupe %d hash-suffix nodes (%d groups)",
                agrovoc_n,
                dup_stats["to_delete"],
                dup_stats["groups"],
            )
            if not execute:
                logger.info("DRY RUN — pass --execute to apply")
                return 0

            deleted_agrovoc = await _delete_carcar_agrovoc(session, execute=True)
            deleted_dupes = await _dedupe_hash_suffix(session, execute=True)
            after = await _post_stats(session)
            logger.info(
                "Deleted: agrovoc=%d dupes=%d | After: %s",
                deleted_agrovoc,
                deleted_dupes,
                after,
            )
            if after["carcar_orphans"] != 0:
                logger.error("Cárcar orphans after purge: %d", after["carcar_orphans"])
                return 1
            if after["carcar_prndu_vt"] != 50:
                logger.error(
                    "Expected 50 Cárcar PRNDU VT after purge, got %d",
                    after["carcar_prndu_vt"],
                )
                return 1
            logger.info("Navarra pre-canonical purge OK")
            return 0
    finally:
        await driver.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Purge Navarra pre-canonical VT residue")
    parser.add_argument("--dry-run", action="store_true", help="Report only (default)")
    parser.add_argument("--execute", action="store_true", help="Apply deletes")
    args = parser.parse_args()
    if not args.execute:
        args.dry_run = True
    code = asyncio.run(run(execute=args.execute))
    sys.exit(code)


if __name__ == "__main__":
    main()
