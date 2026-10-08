#!/usr/bin/env python3
# ruff: noqa: EXE001
"""Compare leave-one-site-out backtest metrics against gate and owner SLO.

Usage:
    PYTHONPATH=. python3 scripts/check_backtest_slo.py
    PYTHONPATH=. python3 scripts/check_backtest_slo.py --gate-only

Exit codes:
    0  every metric measured and within the thresholds
    1  a measured metric is outside its threshold (regression)
    2  not measurable: fewer folds than --min-folds, or a metric has no data behind it (null, or a
       zero the report prints for "no fold scored": no median error without scored pairs, no overlap
       without a covered fold) and no measured metric failed. The gate cannot be evaluated, so it is
       neither passed nor failed: re-baseline once field trials exist.
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


# ASSUMPTION: a gate over fewer folds than this says nothing (the last re-baseline had 267); the value
# is a floor against reading a handful of folds as a verdict, not a statistical derivation.
MIN_FOLDS = 30

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_NOT_MEASURABLE = 2

STATUS_PASS = "pass"
STATUS_FAIL = "fail"
STATUS_NOT_MEASURABLE = "not_measurable"

# metric key -> (label, True when higher is better)
_METRICS = {
    "top3_overlap": ("overlap", True),
    "median_abs_error_kg_ha": ("MAE", False),
    "coverage": ("coverage", True),
}


def _value(overall: dict, key: str) -> float | None:
    """The metric as a float, or None when it is absent, null or not a number."""
    raw = overall.get(key)
    if raw is None or isinstance(raw, bool):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _count(overall: dict, key: str) -> int:
    value = _value(overall, key)
    return int(value) if value is not None else 0


def _has_data(overall: dict, key: str, min_folds: int) -> bool:
    """The metric has data behind it. The report prints 0.0 for an overlap with no covered fold and
    for a coverage with no fold, and null for a median error with no scored pair: none is a result."""
    folds = _count(overall, "folds")
    if folds < min_folds or _value(overall, key) is None:
        return False
    if key == "median_abs_error_kg_ha":
        return _count(overall, "error_pairs") > 0
    if key == "top3_overlap":
        coverage = _value(overall, "coverage")
        return coverage is not None and round(coverage * folds) > 0
    return True


def _check(overall: dict, thresholds: dict, label: str, min_folds: int = MIN_FOLDS) -> str:
    """``pass`` | ``fail`` (a measured metric is outside its threshold) | ``not_measurable``
    (no measured metric failed, but at least one has no data: the gate cannot be evaluated)."""
    failed = False
    unmeasurable = []
    values = {}
    for key, (name, higher_is_better) in _METRICS.items():
        value = _value(overall, key)
        values[name] = value
        if not _has_data(overall, key, min_folds):
            unmeasurable.append(name)
            continue
        bad = value < thresholds[key] if higher_is_better else value > thresholds[key]
        if bad:
            op = "<" if higher_is_better else ">"
            print(f"FAIL [{label}] {name} {value} {op} {thresholds[key]}")
            failed = True
    if unmeasurable:
        print(f"NOT MEASURABLE [{label}] {', '.join(unmeasurable)}: no data behind it"
              f" (folds={_count(overall, 'folds')}, min {min_folds}; scored pairs="
              f"{_count(overall, 'error_pairs')}): too few folds could be scored against field evidence")
    if failed:
        return STATUS_FAIL
    if unmeasurable:
        return STATUS_NOT_MEASURABLE
    print(f"PASS [{label}] " + " ".join(f"{k}={v}" for k, v in values.items()))
    return STATUS_PASS


def _exit_code(*statuses: str) -> int:
    if STATUS_FAIL in statuses:
        return EXIT_FAIL
    if STATUS_NOT_MEASURABLE in statuses:
        return EXIT_NOT_MEASURABLE
    return EXIT_OK


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
    parser.add_argument(
        "--min-folds",
        type=int,
        default=MIN_FOLDS,
        help=f"Fewer folds than this: the gate is not measurable (exit {EXIT_NOT_MEASURABLE}; default {MIN_FOLDS})",
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
        gate = _check(overall, GATE, "gate", args.min_folds)
        if args.gate_only:
            return _exit_code(gate)
        return _exit_code(gate, _check(overall, OWNER, "owner", args.min_folds))
    finally:
        await driver.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
