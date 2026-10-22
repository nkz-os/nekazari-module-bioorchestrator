"""Apply Cypher migrations from backend/cypher_migrations/ in order (ops entry point).

Thin wrapper over ``app.kg.migrations``, the single strict runner shared with app startup and the
KG build: comments are stripped correctly, "already exists" is success, any other error stops the
run (exit status 1), and each applied file is recorded on a ``SchemaVersion`` node. Unlike app
startup, this also runs the data statements (MERGE/SET) of the migrations.

Usage:
    docker-compose run --rm backend python scripts/apply_cypher_migrations.py
    NEO4J_URI=bolt://localhost:7687 NEO4J_PASSWORD=... python scripts/apply_cypher_migrations.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.kg.migrations import MigrationError, apply_migrations
from neo4j import AsyncGraphDatabase


async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="[migrate] %(levelname)s %(message)s")
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ["NEO4J_PASSWORD"]

    async with AsyncGraphDatabase.driver(uri, auth=(user, password)) as driver:
        try:
            report = await apply_migrations(driver, include_data=True)
        except MigrationError as exc:
            print(f"[migrate] FAILED: {exc}", file=sys.stderr, flush=True)
            return 1
    print(f"[migrate] done: {len(report.applied)} files", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
