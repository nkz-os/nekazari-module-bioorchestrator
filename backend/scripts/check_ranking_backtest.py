#!/usr/bin/env python3
# ruff: noqa: EXE001
"""Forward-chaining backtest of the variety table, per crop x climate zone x regime x test year.

Usage:
    PYTHONPATH=. python3 scripts/check_ranking_backtest.py --out /path/to/report-dir
    PYTHONPATH=. python3 scripts/check_ranking_backtest.py --crops TRZAX,HORVX --from 2016 --to 2025 --gate

Rows come from the production evidence path (``evidence_sites`` + ``dao.list_trial_evidence`` with the
same page cap as ``parcel_varieties``), never from raw Cypher over trials, so the dedup and the
evidence policy are inherited. The table under test is ``variety_stats``; when it accepts
``ref_year`` the main run passes the test year and the ``flat`` baseline runs without it.

Exit codes (only with --gate; report mode always exits 0):
    0  every gated crop passes      1  a gated crop fails      2  a gated crop is not measurable
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import sys
from collections.abc import Callable
from functools import partial
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.eval import ranking_backtest as rb
from app.graph.dao import _irrigation_uri
from app.graph.evidence_sites import evidence_sites
from app.planning.evidence import EVIDENCE_ROWS_MAX
from app.planning.varieties import variety_stats

DEFAULT_CROPS = rb.GATED_CROPS + rb.MEASURED_CROPS
DEFAULT_REGIMES = ("rainfed", "irrigated")
DEFAULT_TIERS = ("field", "regional")  # field = production default; regional = the aggregate-source history
EXIT_OK, EXIT_FAIL, EXIT_NOT_MEASURABLE = 0, 1, 2
FLAT_REF_YEAR = 0  # no trial is older than year 0, so every recency weight is 1: the table without recency

ZONES_QUERY = """
MATCH (ts:TrialSite)
WITH coalesce(ts.climateClassChelsa, ts.climateClass) AS zone
WHERE zone IS NOT NULL
RETURN DISTINCT zone ORDER BY zone
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Forward-chaining ranking backtest")
    parser.add_argument("--crops", type=lambda s: [c.strip().upper() for c in s.split(",") if c.strip()],
                        default=list(DEFAULT_CROPS), help="EPPO codes, comma-separated")
    parser.add_argument("--regimes", type=lambda s: [r.strip() for r in s.split(",") if r.strip()],
                        default=list(DEFAULT_REGIMES))
    parser.add_argument("--tiers", type=lambda s: [t.strip() for t in s.split(",") if t.strip()],
                        default=list(DEFAULT_TIERS), help="evidence tiers to evaluate (field, regional)")
    parser.add_argument("--from", dest="from_year", type=int, default=rb.TEST_YEAR_FIRST)
    parser.add_argument("--to", dest="to_year", type=int, default=rb.TEST_YEAR_LAST)
    parser.add_argument("--management", default="conventional")
    parser.add_argument("--country", default="ES",
                        help="country of the regional tier's aggregate pseudo-sites (GENVCE zone/national means)")
    parser.add_argument("--min-train-years", type=int, default=rb.MIN_TRAIN_YEARS,
                        help="earlier years with rows a test year needs")
    parser.add_argument("--min-test-envs", type=int, default=rb.MIN_TEST_ENVIRONMENTS,
                        help="test environments a cell needs (an aggregate pseudo-site has one per year)")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--out", type=Path, default=None, help="directory for <prefix>.json and <prefix>.md")
    parser.add_argument("--prefix", default="ranking-backtest")
    parser.add_argument("--gate", action="store_true", help="exit 1/2 on the ASSUMPTION thresholds")
    return parser.parse_args(argv)


def stats_functions(table_fn: Callable, *, test_year: int) -> tuple[Callable, Callable]:
    """(main, flat): main passes ``ref_year=test_year`` when the table accepts it; flat passes
    ``FLAT_REF_YEAR`` so no observation is discounted (``ref_year=None`` would mean "today")."""
    params = inspect.signature(table_fn).parameters
    if "ref_year" in params:
        return partial(table_fn, ref_year=test_year), partial(table_fn, ref_year=FLAT_REF_YEAR)
    return table_fn, table_fn


def cells_for_group(rows: list[dict], *, crop: str, zone: str, regime: str, tier: str, first: int, last: int,
                    table_fn: Callable, seed: int, truncated: bool, min_train_years: int = rb.MIN_TRAIN_YEARS,
                    min_test_envs: int = rb.MIN_TEST_ENVIRONMENTS) -> list[dict]:
    cells = []
    for test_year in rb.test_years_for(rows, first=first, last=last, min_train_years=min_train_years):
        main_fn, flat_fn = stats_functions(table_fn, test_year=test_year)
        cells.append(rb.evaluate_cell(rows, test_year=test_year, stats_fn=main_fn, flat_fn=flat_fn, seed=seed,
                                      min_test_environments=min_test_envs,
                                      crop=crop, zone=zone, regime=regime, tier=tier, truncated=truncated))
    return cells


def regional_groups(sites: list[dict], *, country: str) -> list[str]:
    """The aggregate pseudo-sites (zone / national means) of ``country``: each is one regional group,
    the way the regional tier of a parcel of that country reads its own zone's means."""
    want = country.strip().upper()
    return [s["name"] for s in sites
            if s.get("site_kind") == "aggregate" and (s.get("country") or "").strip().upper() == want]


def exit_code(verdict: dict[str, str], *, gate: bool) -> int:
    if not gate:
        return EXIT_OK
    statuses = set(verdict.values())
    if "fail" in statuses:
        return EXIT_FAIL
    if "not_measurable" in statuses:
        return EXIT_NOT_MEASURABLE
    return EXIT_OK


async def zones(dao) -> list[str]:
    async with dao._driver.session() as session:
        result = await session.run(ZONES_QUERY)
        return [r["zone"] async for r in result]


async def group_rows(dao, *, crop: str, zone: str, regime: str, tier: str,
                     management: str) -> tuple[list[dict], bool, int]:
    """Rows the way ``load_crop_evidence`` fetches them. Field tier: the field sites of the climate
    class (``evidence_sites``, Köppen path). Regional tier: ``zone`` is one aggregate pseudo-site and
    the rows are its own means (the parcel's zone pool of production, without the fallback pool)."""
    if tier == "regional":
        names = [zone]
    else:
        sites = await evidence_sites(dao, {"climate_class": zone, "soil_type": None}, similarity="koppen",
                                     tier=tier, country=None)
        names = [s["name"] for s in sites]
    if not names:
        return [], False, 0
    page = await dao.list_trial_evidence(
        crop=crop, similar_sites=names, variety=None,
        irrigation_uri=_irrigation_uri(regime), page=1, page_size=EVIDENCE_ROWS_MAX,
        purpose="main", tier=tier, management=management,
    )
    rows = page.get("items") or []
    total = int(page.get("total") or 0)
    return rows, total > len(rows), len(names)


async def run(dao, args: argparse.Namespace, *, tier: str, all_zones: list[str]) -> dict:
    cells: list[dict] = []
    groups: list[dict] = []
    for crop in args.crops:
        for zone in all_zones:
            for regime in args.regimes:
                rows, truncated, n_sites = await group_rows(dao, crop=crop, zone=zone, regime=regime, tier=tier,
                                                            management=args.management)
                years = sorted({r["year"] for r in rows if isinstance(r.get("year"), int)})
                groups.append({"crop": crop, "zone": zone, "regime": regime, "n_sites": n_sites,
                               "n_rows": len(rows), "truncated": truncated,
                               "years": [years[0], years[-1]] if years else [],
                               "rows_per_year": {str(y): sum(1 for r in rows if r.get("year") == y) for y in years}})
                if not rows:
                    continue
                cells += cells_for_group(rows, crop=crop, zone=zone, regime=regime, tier=tier, first=args.from_year,
                                         last=args.to_year, table_fn=variety_stats, seed=args.seed,
                                         truncated=truncated, min_train_years=args.min_train_years,
                                         min_test_envs=args.min_test_envs)
    summary = rb.aggregate_by_crop(cells)
    recency_aware = "ref_year" in inspect.signature(variety_stats).parameters
    meta = {"tier": tier, "crops": args.crops, "regimes": args.regimes, "window": [args.from_year, args.to_year],
            "min_train_years": args.min_train_years, "min_test_envs": args.min_test_envs,
            "country": args.country if tier == "regional" else None,
            "management": args.management, "seed": args.seed, "zones": len(all_zones),
            "table": "variety_stats" + (" (recency-aware: ref_year=test year)" if recency_aware else " (flat)"),
            "page_cap": EVIDENCE_ROWS_MAX, "groups_truncated": sum(1 for g in groups if g["truncated"]),
            "n_cells": len(cells), "n_cells_ok": sum(1 for c in cells if c["status"] == "ok")}
    return {"meta": meta, "groups": groups, "cells": cells, "summary": summary, "verdict": rb.gate_verdict(summary)}


def write_report(report: dict, out: Path | None, prefix: str) -> None:
    md = rb.render_markdown(report)
    if out is None:
        print(md)
        return
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{prefix}.json").write_text(json.dumps(report, indent=1, default=str), encoding="utf-8")
    (out / f"{prefix}.md").write_text(md, encoding="utf-8")
    print(f"wrote {out / (prefix + '.json')} and {out / (prefix + '.md')}")


async def main() -> int:
    args = parse_args()
    from app.core.config import settings
    from app.graph.dao import GraphDAO
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password),
                                       notifications_min_severity="OFF")
    reports: dict[str, dict] = {}
    try:
        dao = GraphDAO(driver)
        climate_zones = await zones(dao)
        aggregates = regional_groups(await dao.get_similar_sites(climate_class=None, soil_type=None, limit=None,
                                                                 include_aggregate=True, country=args.country),
                                     country=args.country)
        for tier in args.tiers:
            groups = aggregates if tier == "regional" else climate_zones
            reports[tier] = await run(dao, args, tier=tier, all_zones=groups)
    finally:
        await driver.close()
    for tier, report in reports.items():
        write_report(report, args.out, f"{args.prefix}-{tier}")
        print(json.dumps({"tier": tier, "summary": report["summary"], "verdict": report["verdict"]},
                         indent=1, default=str))
    # The gate reads the production default tier (field); the other tiers are measured, not gated.
    gated = reports.get("field") or next(iter(reports.values()))
    return exit_code(gated["verdict"], gate=args.gate)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
