#!/usr/bin/env python3
# ruff: noqa: EXE001
"""Compare leave-one-site-out backtest metrics against gate and owner SLO.

Usage:
    PYTHONPATH=. python3 scripts/check_backtest_slo.py
    PYTHONPATH=. python3 scripts/check_backtest_slo.py --gate-only
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import settings
from app.eval.backtest import Backtester
from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

# re-baselined 2026-10-02 on CHELSA-enriched graph, hybrid strategy
GATE = {
    "top3_overlap": 0.200,
    "median_abs_error_kg_ha": 850.0,
    "coverage": 0.89,
}
OWNER = {
    "top3_overlap": 0.25,
    "median_abs_error_kg_ha": 800.0,
    "coverage": 0.90,
}


def _check(overall: dict, thresholds: dict, label: str) -> bool:
    ok = True
    overlap = float(overall["top3_overlap"])
    mae = float(overall["median_abs_error_kg_ha"])
    cov = float(overall["coverage"])
    if overlap < thresholds["top3_overlap"]:
        print(f"FAIL [{label}] overlap {overlap} < {thresholds['top3_overlap']}")
        ok = False
    if mae > thresholds["median_abs_error_kg_ha"]:
        print(f"FAIL [{label}] MAE {mae} > {thresholds['median_abs_error_kg_ha']}")
        ok = False
    if cov < thresholds["coverage"]:
        print(f"FAIL [{label}] coverage {cov} < {thresholds['coverage']}")
        ok = False
    if ok:
        print(f"PASS [{label}] overlap={overlap} mae={mae} coverage={cov}")
    return ok


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest SLO gate check")
    parser.add_argument(
        "--gate-only",
        action="store_true",
        help="Exit 0 only on no-regression gate (not owner target)",
    )
    parser.add_argument(
        "--strategy",
        choices=("koppen", "v1", "v2", "hybrid"),
        default="hybrid",
        help="Similarity strategy (default: hybrid)",
    )
    return parser.parse_args(argv)


async def main() -> int:
    args = parse_args()

    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password),
    )
    try:
        dao = GraphDAO(driver)
        print(f"strategy: {args.strategy}")
        report = await Backtester(dao).run(strategy=args.strategy)
        overall = report["overall"]
        print(json.dumps(overall, indent=2))
        gate_ok = _check(overall, GATE, "gate")
        if args.gate_only:
            return 0 if gate_ok else 1
        owner_ok = _check(overall, OWNER, "owner")
        return 0 if gate_ok and owner_ok else 1
    finally:
        await driver.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
