"""Seed (:RegionalYieldStat) nodes from the latest Eurostat snapshot in data/eurostat/.

Dry run by default (prints counts); --execute writes. Idempotent: MERGE on key, SET n = row.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from collections import Counter
from pathlib import Path

import yaml

from neo4j import GraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.eurostat_yields import (
    build_series,
    parse_jsonstat,
    to_node_props,
)

CONSTRAINT = (
    "CREATE CONSTRAINT regional_yield_stat_key IF NOT EXISTS "
    "FOR (n:RegionalYieldStat) REQUIRE n.key IS UNIQUE"
)
MERGE = "UNWIND $rows AS row MERGE (n:RegionalYieldStat {key: row.key}) SET n = row"
BATCH = 1000


def latest_snapshot(directory: Path) -> Path:
    found = sorted(directory.glob("apro_cpshr_nuts2_*.json.gz"))
    if not found:
        raise SystemExit(f"no snapshot in {directory}")
    return found[-1]


def build_rows(snapshot: dict, crop_map: dict[str, str]) -> list[dict]:
    rows = build_series(parse_jsonstat(snapshot["jsonstat"]), crop_map)
    return [to_node_props(r, dataset_updated=snapshot.get("dataset_updated"),
                          extracted_at=snapshot["retrieved_at"],
                          nuts_version=snapshot["nuts_version"]) for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed Eurostat regional yields")
    parser.add_argument("--data-dir", type=Path, default=Path(os.getenv("DATA_DIR", "./data")))
    parser.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", ""))
    parser.add_argument("--execute", action="store_true", help="write (default: dry run)")
    args = parser.parse_args()

    path = latest_snapshot(args.data_dir / "eurostat")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        snapshot = json.load(f)
    crop_map = yaml.safe_load((args.data_dir / "eurostat" / "crop_codes.yaml").read_text())["crops"]
    rows = build_rows(snapshot, crop_map)
    per_crop = Counter(r["eppo"] for r in rows)
    print(f"[seed_regional_yields] {path.name}: {len(rows)} rows, "
          f"{len({r['nuts'] for r in rows})} regions, per crop {dict(sorted(per_crop.items()))}")
    if not args.execute:
        print("[seed_regional_yields] dry run, nothing written (use --execute)")
        return

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    try:
        driver.verify_connectivity()
        with driver.session() as session:
            session.run(CONSTRAINT).consume()
            for i in range(0, len(rows), BATCH):
                session.run(MERGE, rows=rows[i:i + BATCH]).consume()
        print(f"[seed_regional_yields] wrote {len(rows)} rows to {args.neo4j_uri}")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
