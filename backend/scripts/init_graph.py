"""Graph initialisation script — n10s setup + JSON-LD ingestion.

Run manually after `docker-compose up` once Neo4j is healthy:

    docker-compose run --rm backend python scripts/init_graph.py
    docker-compose run --rm backend python scripts/init_graph.py --data-dir ./data/processed
    docker-compose run --rm backend python scripts/init_graph.py --clean  # wipes graph first
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from neo4j import GraphDatabase, Driver


# ── Connection ────────────────────────────────────────────────────────────────

def connect(uri: str, user: str, password: str, retries: int = 30, delay: float = 2.0) -> Driver:
    """Connect to Neo4j with retries (Neo4j may still be starting)."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            driver.verify_connectivity()
            print(f"[init_graph] Connected to Neo4j at {uri}")
            return driver
        except Exception as exc:
            last_exc = exc
            print(f"[init_graph] Attempt {attempt}/{retries} — waiting {delay}s: {exc}")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Neo4j after {retries} attempts: {last_exc}")


# ── Graph initialisation ──────────────────────────────────────────────────────

N10S_GRAPHCONFIG = {
    "handleVocabUris": "MAP",
    "handleMultival": "ARRAY",
    "handleRDFTypes": "LABELS_AND_NODES",
    "keepLangTag": True,
}

CONSTRAINT_QUERY = (
    "CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS "
    "FOR (r:Resource) REQUIRE r.uri IS UNIQUE"
)


def init_n10s(driver: Driver) -> None:
    """Initialise n10s graphconfig and create the required constraint."""
    with driver.session() as session:
        # Check if already initialised to avoid re-init error
        result = session.run(
            "MATCH (c:_GraphConfig) RETURN count(c) AS cnt"
        )
        record = result.single()
        if record and record["cnt"] > 0:
            print("[init_graph] n10s already initialised — skipping graphconfig.init")
        else:
            session.run(
                "CALL n10s.graphconfig.init($cfg)",
                cfg=N10S_GRAPHCONFIG,
            )
            print("[init_graph] n10s graphconfig initialised")

        session.run(CONSTRAINT_QUERY)
        print("[init_graph] Constraint n10s_unique_uri ensured")


# ── Clean ─────────────────────────────────────────────────────────────────────

def clean_graph(driver: Driver) -> None:
    """Delete all nodes and relationships (destructive)."""
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    print("[init_graph] Graph wiped")


# ── JSON-LD ingestion ─────────────────────────────────────────────────────────

JSONLD_EXTENSIONS = {".jsonld", ".json-ld", ".json"}


def is_jsonld(path: Path) -> bool:
    """Return True if the file looks like a JSON-LD document."""
    if path.suffix.lower() not in JSONLD_EXTENSIONS:
        return False
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return "@context" in data or "@graph" in data
    except Exception:
        return False


def ingest_file(driver: Driver, path: Path) -> int:
    """Ingest a single JSON-LD file via n10s. Returns triple count loaded."""
    content = path.read_text(encoding="utf-8")
    with driver.session() as session:
        result = session.run(
            "CALL n10s.rdf.import.inline($payload, 'JSON-LD', {commitSize: 500}) "
            "YIELD terminationStatus, triplesLoaded, triplesParsed, extraInfo "
            "RETURN terminationStatus, triplesLoaded, triplesParsed, extraInfo",
            payload=content,
        )
        record = result.single()
        if record is None:
            return 0
        status = record["terminationStatus"]
        loaded = record["triplesLoaded"] or 0
        if status != "OK":
            print(f"  [WARN] {path.name}: terminationStatus={status} "
                  f"extraInfo={record['extraInfo']}")
        return loaded


def ingest_directory(driver: Driver, data_dir: Path) -> None:
    """Scan data_dir recursively and ingest all JSON-LD files."""
    if not data_dir.exists():
        print(f"[init_graph] Data directory not found: {data_dir} — skipping ingestion")
        return

    files = [p for p in data_dir.rglob("*") if p.is_file() and is_jsonld(p)]
    if not files:
        print(f"[init_graph] No JSON-LD files found in {data_dir}")
        return

    print(f"[init_graph] Found {len(files)} JSON-LD file(s) in {data_dir}")
    total_triples = 0
    errors = 0

    for path in sorted(files):
        try:
            triples = ingest_file(driver, path)
            total_triples += triples
            print(f"  ✓ {path.name}: {triples} triples loaded")
        except Exception as exc:
            errors += 1
            print(f"  ✗ {path.name}: {exc}")

    print(f"\n[init_graph] Ingestion complete — {total_triples} triples total, {errors} error(s)")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialise Neo4j graph with n10s and ingest IkerKeta JSON-LD data"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.getenv("IKERKETA_DATA_DIR", "./data/processed")),
        help="Directory containing IkerKeta JSON-LD output (default: $IKERKETA_DATA_DIR or ./data/processed)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Wipe the graph before ingestion (destructive)",
    )
    parser.add_argument(
        "--neo4j-uri",
        default=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    )
    parser.add_argument(
        "--neo4j-user",
        default=os.getenv("NEO4J_USER", "neo4j"),
    )
    parser.add_argument(
        "--neo4j-password",
        default=os.getenv("NEO4J_PASSWORD", "bioorchestrator"),
    )
    args = parser.parse_args()

    driver = connect(args.neo4j_uri, args.neo4j_user, args.neo4j_password)

    try:
        if args.clean:
            confirm = input("[init_graph] --clean will DELETE ALL nodes. Type 'yes' to confirm: ")
            if confirm.strip().lower() != "yes":
                print("[init_graph] Aborted")
                sys.exit(0)
            clean_graph(driver)

        init_n10s(driver)
        ingest_directory(driver, args.data_dir)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
