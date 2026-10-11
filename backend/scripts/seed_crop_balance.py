"""Seed (:CropBalanceCoefficients) nodes from data/crop_balance_coefficients.yaml.

Validates first (registered, redistributable sources; ranges; references). Dry run by default;
--execute writes. Idempotent: MERGE on eppo, SET n = row; crops removed from the file are deleted.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import yaml

from neo4j import GraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.kg.registries import DEFAULT_REGISTRIES_PATH, load_registries
from app.services.crop_balance import to_node_props, validate

CONSTRAINT = (
    "CREATE CONSTRAINT crop_balance_coefficients_eppo IF NOT EXISTS "
    "FOR (n:CropBalanceCoefficients) REQUIRE n.eppo IS UNIQUE"
)
MERGE = """
UNWIND $rows AS row
MERGE (n:CropBalanceCoefficients {eppo: row.eppo})
SET n = row
WITH n
OPTIONAL MATCH (c:Crop {eppo: n.eppo})
FOREACH (_ IN CASE WHEN c IS NULL THEN [] ELSE [1] END |
  MERGE (c)-[:HAS_BALANCE_COEFFICIENTS]->(n))
"""
PRUNE = "MATCH (n:CropBalanceCoefficients) WHERE NOT n.eppo IN $eppos DETACH DELETE n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed crop nutrient-balance coefficients")
    parser.add_argument("--data-dir", type=Path, default=Path(os.getenv("DATA_DIR", "./data")))
    parser.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", ""))
    parser.add_argument("--execute", action="store_true", help="write (default: dry run)")
    args = parser.parse_args()

    doc = yaml.safe_load((args.data_dir / "crop_balance_coefficients.yaml").read_text())
    allowed = {s.source_id for s in load_registries(DEFAULT_REGISTRIES_PATH).loadable_sources()}
    errors = validate(doc, allowed)
    if errors:
        for e in errors:
            print(f"[seed_crop_balance] ERROR {e}")
        raise SystemExit(1)
    rows = [to_node_props(eppo, crop, doc["dataset_version"]) for eppo, crop in doc["crops"].items()]
    print(f"[seed_crop_balance] {len(rows)} crops, dataset {doc['dataset_version']}")
    if not args.execute:
        print("[seed_crop_balance] dry run, nothing written (use --execute)")
        return

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    try:
        driver.verify_connectivity()
        with driver.session() as session:
            session.run(CONSTRAINT).consume()
            session.run(MERGE, rows=rows).consume()
            pruned = session.run(PRUNE, eppos=[r["eppo"] for r in rows]).consume()
        print(f"[seed_crop_balance] wrote {len(rows)} crops, removed "
              f"{pruned.counters.nodes_deleted} stale")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
