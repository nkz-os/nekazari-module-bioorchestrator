#!/usr/bin/env python3
"""One-shot Wave 3 ingest runner for prod pod (patches + baseline + merge + verify)."""
from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sys

from neo4j import AsyncGraphDatabase

# Patch before importing ingesters (prod image is stale until branch merge).
for src, dst in (
    ("/tmp/site_canonicalization.py", "/app/app/graph/site_canonicalization.py"),
    ("/tmp/base_ingester.py", "/app/app/ingestion/base_ingester.py"),
):
    shutil.copy(src, dst)

from app.ingestion.base_ingester import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER  # noqa: E402
from app.ingestion.navarra_ingester import NavarraIngester  # noqa: E402

BUNDLE = "/tmp/wave3_adequate.jsonld"
SOURCE_ID = NavarraIngester.SOURCE_ID


async def baseline(driver) -> dict:
    async with driver.session() as s:
        async def one(q: str, **params) -> int:
            row = await (await s.run(q, **params)).single()
            return int(row["c"])

        return {
            "vt_total": await one("MATCH (v:VarietyTrial) RETURN count(v) AS c"),
            "vt_source": await one(
                "MATCH (v:VarietyTrial {source_id: $sid}) RETURN count(v) AS c", sid=SOURCE_ID
            ),
            "mt_source": await one(
                "MATCH (m:ManagementTrial {source_id: $sid}) RETURN count(m) AS c", sid=SOURCE_ID
            ),
            "orphan_vt": await one(
                "MATCH (v:VarietyTrial {source_id: $sid}) "
                "WHERE NOT (v)-[:TRIAL_AT]->(:TrialSite) RETURN count(v) AS c",
                sid=SOURCE_ID,
            ),
            "orphan_mt": await one(
                "MATCH (m:ManagementTrial {source_id: $sid}) "
                "WHERE NOT (m)-[:TRIAL_AT]->(:TrialSite) RETURN count(m) AS c",
                sid=SOURCE_ID,
            ),
            "null_clim_sites": await one(
                "MATCH (t:TrialSite)<-[:TRIAL_AT]-(v:VarietyTrial {source_id: $sid}) "
                "WHERE t.climateClass IS NULL AND t.climate_class IS NULL "
                "RETURN count(DISTINCT t) AS c",
                sid=SOURCE_ID,
            ),
            "trial_at": await one(
                "MATCH (:VarietyTrial {source_id: $sid})-[:TRIAL_AT]->(:TrialSite) "
                "RETURN count(*) AS c",
                sid=SOURCE_ID,
            ),
        }


async def run(*, execute: bool) -> int:
    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    ing = NavarraIngester(driver=driver)
    try:
        before = await baseline(driver)
        print("BASELINE", json.dumps(before))

        nodes = await ing.transform(BUNDLE)
        transform = {k: len(v) for k, v in nodes.items()}
        print("TRANSFORM", json.dumps(transform))

        if not execute:
            print("DRY_RUN_OK")
            return 0

        stats = await ing.merge(nodes)
        print("MERGE", json.dumps(stats))

        after = await baseline(driver)
        print("AFTER", json.dumps(after))

        if after["orphan_vt"] != 0:
            print("ERROR orphan_vt != 0", file=sys.stderr)
            return 1

        if after["orphan_mt"] != 0:
            print("WARN orphan_mt != 0", after["orphan_mt"], file=sys.stderr)

        delta = after["vt_source"] - before["vt_source"]
        print("DELTA_VT", delta)
        print("INGEST_OK")
        return 0
    finally:
        await driver.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(execute=args.execute)))


if __name__ == "__main__":
    main()
