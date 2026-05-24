"""Apply Cypher migrations from backend/cypher_migrations/ in order.

Idempotent — uses CREATE...IF NOT EXISTS and MERGE in every migration.

Usage:
    docker-compose run --rm backend python scripts/apply_cypher_migrations.py
    NEO4J_URI=bolt://localhost:7687 NEO4J_PASSWORD=... python scripts/apply_cypher_migrations.py
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from neo4j import AsyncGraphDatabase

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "cypher_migrations"


def _statements(text: str) -> list[str]:
    """Split a .cypher file into individual statements.

    Strips line comments (// ...) and blank lines, then splits on ';'.
    """
    cleaned: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.split("//", 1)[0].rstrip()
        if line:
            cleaned.append(line)
    body = "\n".join(cleaned)
    return [s.strip() for s in body.split(";") if s.strip()]


async def main() -> None:
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ["NEO4J_PASSWORD"]

    async with AsyncGraphDatabase.driver(uri, auth=(user, password)) as driver:
        async with driver.session() as session:
            for migration in sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.cypher")):
                print(f"[migrate] applying {migration.name}", flush=True)
                for stmt in _statements(migration.read_text()):
                    await session.run(stmt)
    print("[migrate] done", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
