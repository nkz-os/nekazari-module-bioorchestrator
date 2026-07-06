"""Gate-first multi-source re-ingest for approved perennial JSON-LD bundles.

Splits an owner-approved bundle by ``source_id`` and routes each subgraph to its
ingester (IFAPA_ALMOND, NAVARRA-AGRARIA, INTIA-EXP). Does **not** purge entire
NAVARRA-AGRARIA — cereal trials from Wave 1 must remain.

Flow:
  1. Prepare approved graph (strip skip_ingestion, drop unregistered sources)
  2. Validate full approved bundle (gate must pass)
  3. Per source: baseline → transform → MERGE (no source-wide purge)
  4. Post-check orphans per source

Usage:
    python -m scripts.perennial_canonical_reingest /path/to/candidate.jsonld --dry-run
    python -m scripts.perennial_canonical_reingest /path/to/candidate.jsonld --execute
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import tempfile
from pathlib import Path
from typing import Any

from neo4j import AsyncGraphDatabase

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.ingestion.almond_ifapa_ingester import AlmondIfapaIngester
from app.ingestion.base_ingester import BaseIngester, NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER
from app.ingestion.intia_exp_ingester import IntiaExpIngester
from app.ingestion.navarra_ingester import NavarraIngester
from app.ingestion.validate_ingest_bundle import validate_bundle
from scripts.canonical_reingest import _baseline
from scripts.purge_navarra_precanonical_residual import run as purge_navarra_precanonical

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SKIP_SOURCES = frozenset({"SCIENTIA-PIAVE"})

SOURCE_INGESTERS: dict[str, type[BaseIngester]] = {
    AlmondIfapaIngester.SOURCE_ID: AlmondIfapaIngester,
    NavarraIngester.SOURCE_ID: NavarraIngester,
    IntiaExpIngester.SOURCE_ID: IntiaExpIngester,
}

_TYPES_BY_SOURCE: dict[str, frozenset[str]] = {
    AlmondIfapaIngester.SOURCE_ID: frozenset({"TrialSite", "ArticleSource", "VarietyTrial"}),
    NavarraIngester.SOURCE_ID: frozenset(
        {"TrialSite", "ArticleSource", "VarietyTrial", "ManagementTrial", "HarvestData"}
    ),
    IntiaExpIngester.SOURCE_ID: frozenset(
        {"TrialSite", "ArticleSource", "VarietyTrial", "ManagementTrial"}
    ),
}


def prepare_approved_graph(data: dict[str, Any]) -> dict[str, Any]:
    """Owner approval: drop staging flags and unregistered sources."""
    graph: list[dict[str, Any]] = []
    for node in data.get("@graph", []):
        if node.get("source_id") in SKIP_SOURCES:
            continue
        clean = {
            k: v
            for k, v in node.items()
            if k not in ("skip_ingestion", "review_status")
        }
        graph.append(clean)
    return {"@context": data.get("@context"), "@graph": graph}


def subgraph_for_source(graph: list[dict[str, Any]], source_id: str) -> list[dict[str, Any]]:
    """Nodes for a single ingester — never pass foreign trials to NavarraIngester."""
    allowed = _TYPES_BY_SOURCE[source_id]
    out: list[dict[str, Any]] = []
    for node in graph:
        node_type = node.get("@type")
        if node_type not in allowed:
            continue
        sid = node.get("source_id")
        if sid == source_id:
            out.append(node)
            continue
        # Lone ArticleSource in almond bundle has no source_id field.
        if node_type == "ArticleSource" and source_id == AlmondIfapaIngester.SOURCE_ID and not sid:
            out.append(node)
    return out


def _expected_unique_vt(variety_rows: list[dict]) -> set[str]:
    return {
        uk
        for v in variety_rows
        if not v.get("skip_ingestion")
        for uk in [BaseIngester._variety_unique_key(v)]
        if uk
    }


async def _merge_source(
    driver,
    source_id: str,
    bundle_path: str,
    *,
    execute: bool,
) -> int:
    factory = SOURCE_INGESTERS[source_id]
    ingester = factory(driver=driver)

    before = await _baseline(driver, source_id)
    logger.info("Baseline (%s): %s", source_id, before)

    nodes = await ingester.transform(bundle_path)
    transform_counts = {k: len(v) for k, v in nodes.items()}
    logger.info("Transform (%s): %s", source_id, transform_counts)

    if not execute:
        return 0

    expected = _expected_unique_vt(nodes.get("variety_trials", []))
    merge_stats = await ingester.merge(nodes)
    logger.info("Merge (%s): %s", source_id, merge_stats)

    after = await _baseline(driver, source_id)
    logger.info("After (%s): %s", source_id, after)

    if after["orphan_vt"] != 0:
        logger.error("Post-ingest orphan VarietyTrials (%s): %d", source_id, after["orphan_vt"])
        return 1

    if expected and after["vt_source"] < len(expected):
        logger.error(
            "VarietyTrial nodes %d < expected %d for %s",
            after["vt_source"],
            len(expected),
            source_id,
        )
        return 1

    if expected and after["trial_at"] < len(expected):
        logger.error(
            "TRIAL_AT links %d < expected %d for %s",
            after["trial_at"],
            len(expected),
            source_id,
        )
        return 1

    return 0


async def run(
    bundle_path: str,
    *,
    dry_run: bool,
    execute: bool,
    driver=None,
) -> int:
    path = Path(bundle_path)
    if not path.is_file():
        logger.error("Bundle not found: %s", path)
        return 1

    raw = json.loads(path.read_text(encoding="utf-8"))
    approved = prepare_approved_graph(raw)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonld", delete=False, encoding="utf-8"
    ) as tmp:
        json.dump(approved, tmp, ensure_ascii=False)
        approved_path = tmp.name

    try:
        report = validate_bundle(approved_path)
        logger.info(
            "Gate (approved): %s — errors=%d warnings=%d stats=%s",
            "PASS" if report.ok else "FAIL",
            len(report.errors()),
            len(report.warnings()),
            report.stats,
        )
        if not report.ok:
            for err in report.errors()[:10]:
                logger.error("%s %s", err.code, err.message)
            return 1

        graph = approved["@graph"]

        if dry_run and not execute:
            for source_id in SOURCE_INGESTERS:
                sub_graph = subgraph_for_source(graph, source_id)
                vt_count = sum(
                    1 for n in sub_graph if n.get("@type") == "VarietyTrial"
                )
                if vt_count == 0:
                    logger.info("Skip %s — no VarietyTrial rows", source_id)
                    continue
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".jsonld", delete=False, encoding="utf-8"
                ) as sub_tmp:
                    json.dump({**approved, "@graph": sub_graph}, sub_tmp, ensure_ascii=False)
                    sub_path = sub_tmp.name
                try:
                    ingester = SOURCE_INGESTERS[source_id]()
                    nodes = await ingester.transform(sub_path)
                    logger.info(
                        "Transform (%s): %s",
                        source_id,
                        {k: len(v) for k, v in nodes.items()},
                    )
                finally:
                    Path(sub_path).unlink(missing_ok=True)
            logger.info("DRY RUN — no Neo4j writes (pass --execute to merge)")
            return 0

        owns_driver = driver is None
        if owns_driver:
            driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        try:
            for source_id in SOURCE_INGESTERS:
                sub_graph = subgraph_for_source(graph, source_id)
                vt_count = sum(
                    1
                    for n in sub_graph
                    if n.get("@type") == "VarietyTrial"
                )
                if vt_count == 0:
                    logger.info("Skip %s — no VarietyTrial rows", source_id)
                    continue

                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".jsonld", delete=False, encoding="utf-8"
                ) as sub_tmp:
                    json.dump({**approved, "@graph": sub_graph}, sub_tmp, ensure_ascii=False)
                    sub_path = sub_tmp.name

                try:
                    sub_report = validate_bundle(sub_path)
                    if not sub_report.ok:
                        logger.error("Sub-bundle gate FAIL for %s", source_id)
                        for err in sub_report.errors()[:5]:
                            logger.error("%s %s", err.code, err.message)
                        return 1

                    code = await _merge_source(
                        driver,
                        source_id,
                        sub_path,
                        execute=execute and not dry_run,
                    )
                    if code != 0:
                        return code
                finally:
                    Path(sub_path).unlink(missing_ok=True)

            if execute and not dry_run:
                nav_vt = sum(
                    1
                    for n in graph
                    if n.get("@type") == "VarietyTrial"
                    and n.get("source_id") == NavarraIngester.SOURCE_ID
                )
                if nav_vt > 0:
                    logger.info(
                        "Post-merge Navarra hygiene (%d bundle VT)", nav_vt
                    )
                    purge_code = await purge_navarra_precanonical(
                        execute=True, driver=driver
                    )
                    if purge_code != 0:
                        return purge_code

            if not execute:
                logger.info("DRY RUN — no writes (pass --execute to merge)")
            else:
                logger.info("Perennial canonical re-ingest OK")
            return 0
        finally:
            if owns_driver:
                await driver.close()
    finally:
        Path(approved_path).unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Gate-first perennial multi-source re-ingest")
    parser.add_argument("bundle", help="Path to perennial candidate JSON-LD")
    parser.add_argument("--dry-run", action="store_true", help="Validate + transform only")
    parser.add_argument("--execute", action="store_true", help="Write to Neo4j")
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        args.dry_run = True

    code = asyncio.run(run(args.bundle, dry_run=args.dry_run, execute=args.execute))
    sys.exit(code)


if __name__ == "__main__":
    main()
