"""Seed Neo4j with the initial Action Rules catalog (SP2 Action Rules Engine).

Reads data/action_rules.yaml and populates the knowledge graph via
GraphDAO.create_action_rule, which MERGEs (:ActionRule {id}) nodes and links
species-specific rules to (:Species) via HAS_RULE.

Each rule carries provenance (source_doi / source_short) — no invented
agronomic thresholds (CLAUDE.md life-critical mandate).

Idempotent — uses MERGE, safe to re-run.

Usage:
    python scripts/seed_action_rules.py [--data-dir ./data] [--neo4j-uri bolt://...]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any

import yaml
from neo4j import AsyncDriver, AsyncGraphDatabase

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.graph.dao import GraphDAO


async def connect(uri: str, user: str, password: str) -> AsyncDriver:
    driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
    await driver.verify_connectivity()
    print(f"[seed_action_rules] Connected to {uri}")
    return driver


async def seed(driver: AsyncDriver, data: dict[str, Any]) -> int:
    """Load action rules into Neo4j via GraphDAO. Returns number of rules seeded."""
    dao = GraphDAO(driver)
    count = 0
    for rule in data["rules"]:
        await dao.create_action_rule(rule)
        count += 1
    return count


# ── CLI ──────────────────────────────────────────────────────────────────────

async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed Neo4j with the initial Action Rules catalog"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.getenv("DATA_DIR", "./data")),
        help="Directory containing action_rules.yaml",
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
        default=os.getenv("NEO4J_PASSWORD", ""),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse YAML but do not write to Neo4j",
    )
    args = parser.parse_args()

    yaml_path = args.data_dir / "action_rules.yaml"
    if not yaml_path.exists():
        print(f"[seed_action_rules] ERROR: {yaml_path} not found")
        raise SystemExit(1)

    with yaml_path.open("r") as f:
        data = yaml.safe_load(f)

    rule_count = len(data.get("rules", []))
    print(f"[seed_action_rules] Loaded {rule_count} rules from {yaml_path}")

    if args.dry_run:
        for rule in data["rules"]:
            print(f"  {rule['id']} ({rule.get('category')}) — source: {rule.get('source_short')}")
        return

    if not args.neo4j_password:
        raise SystemExit("[seed_action_rules] Falta NEO4J_PASSWORD (o --neo4j-password).")

    driver = await connect(args.neo4j_uri, args.neo4j_user, args.neo4j_password)
    try:
        count = await seed(driver, data)
        print(f"[seed_action_rules] Done — {count} action rules seeded")
    finally:
        await driver.close()


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
