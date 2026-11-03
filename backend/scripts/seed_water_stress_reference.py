"""Seed (:WaterStressReference) nodes from data/water_stress_reference.yaml.

One node per species: `payload` is the raw species block as a JSON string (all rows, served or
not; the serving filter is applied at read time), `sourceIds` the source ids it references.
Validates first. Dry run by default; --execute writes. Idempotent: MERGE on species; species
removed from the file are deleted.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from neo4j import GraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.kg.registries import DEFAULT_REGISTRIES_PATH, load_registries
from app.services.water_stress_reference import load, validate

CONSTRAINT = (
    "CREATE CONSTRAINT water_stress_reference_species IF NOT EXISTS "
    "FOR (n:WaterStressReference) REQUIRE n.species IS UNIQUE"
)
MERGE = """
UNWIND $rows AS row
MERGE (n:WaterStressReference {species: row.species})
SET n = row
"""
PRUNE = "MATCH (n:WaterStressReference) WHERE NOT n.species IN $species DETACH DELETE n"


def _source_ids(block: dict) -> list[str]:
    ids: set[str] = set()
    for b in block.get("baselines") or []:
        if b.get("source_id"):
            ids.add(b["source_id"])
    for f in (block.get("stages") or {}).values():
        ids.update(f.get("source_ids") or [])
    return sorted(ids)


def build_rows(doc: dict) -> list[dict]:
    rows = []
    for slug, block in (doc.get("species") or {}).items():
        row = {
            "species": slug,
            "payload": json.dumps(block, sort_keys=True, ensure_ascii=False),
            "sourceIds": _source_ids(block),
        }
        if doc.get("dataset_version"):
            row["datasetVersion"] = str(doc["dataset_version"])
        rows.append(row)
    return rows


def known_sources() -> dict[str, str]:
    regs = load_registries(DEFAULT_REGISTRIES_PATH)
    return {s.source_id: s.licence.commercial_use for s in regs.sources}


def write(driver, rows: list[dict]) -> int:
    with driver.session() as session:
        session.run(CONSTRAINT).consume()
        session.run(MERGE, rows=rows).consume()
        pruned = session.run(PRUNE, species=[r["species"] for r in rows]).consume()
    return pruned.counters.nodes_deleted


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed water-stress reference")
    parser.add_argument("--data-dir", type=Path, default=Path(os.getenv("DATA_DIR", "./data")))
    parser.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", ""))
    parser.add_argument("--execute", action="store_true", help="write (default: dry run)")
    args = parser.parse_args()

    doc = load(args.data_dir / "water_stress_reference.yaml")
    errors = validate(doc, known_sources())
    if errors:
        for e in errors:
            print(f"[seed_water_stress] ERROR {e}")
        raise SystemExit(1)
    rows = build_rows(doc)
    print(f"[seed_water_stress] {len(rows)} species, dataset {doc.get('dataset_version')}")
    if not args.execute:
        print("[seed_water_stress] dry run, nothing written (use --execute)")
        return

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    try:
        driver.verify_connectivity()
        removed = write(driver, rows)
        print(f"[seed_water_stress] wrote {len(rows)} species, removed {removed} stale")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
