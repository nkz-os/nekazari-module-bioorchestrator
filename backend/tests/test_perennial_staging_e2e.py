"""Staging e2e: perennial candidate bundle (approved-filter simulation).

Full bundle remains ``skip_ingestion: true`` / ``review_status: pending`` in
source. This test validates gate + merge on the subset that would ingest after
owner approval (nodes without ``skip_ingestion``).
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.ingestion.navarra_ingester import NavarraIngester
from app.ingestion.validate_ingest_bundle import validate_bundle

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="docker unavailable for testcontainers",
)

BUNDLE = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "internal-docs-local",
        "perennial-extractions-review",
        "ingest_candidate_no_ctifl.jsonld",
    )
)

_loop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


def _approved_subset(bundle_path: str) -> dict:
    """Simulate post-owner approval: drop skip_ingestion flag for staging test."""
    data = json.loads(Path(bundle_path).read_text(encoding="utf-8"))
    graph = []
    for n in data.get("@graph", []):
        if n.get("@type") not in ("VarietyTrial", "ManagementTrial"):
            graph.append(n)
            continue
        if n.get("skip_ingestion"):
            node = {k: v for k, v in n.items() if k != "skip_ingestion"}
            graph.append(node)
        else:
            graph.append(n)
    return {**data, "@graph": graph}


@pytest.fixture(scope="module")
def driver():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = AsyncGraphDatabase.driver(
            n.get_connection_url(), auth=(n.username, n.password)
        )
        yield d
        _run(d.close())


@pytest.mark.skipif(not os.path.isfile(BUNDLE), reason="perennial candidate missing")
def test_perennial_candidate_gate_passes():
    report = validate_bundle(BUNDLE)
    assert report.ok, [e.code for e in report.errors()[:10]]


@pytest.mark.skipif(not os.path.isfile(BUNDLE), reason="perennial candidate missing")
def test_perennial_navarra_subset_staging_merge(driver, tmp_path):
    """Navarra perennial rows only — IFAPA_ALMOND needs AlmondIfapaIngester path."""
    subset = _approved_subset(BUNDLE)
    nav_trials = [
        n
        for n in subset["@graph"]
        if n.get("@type") == "VarietyTrial" and n.get("source_id") == "NAVARRA-AGRARIA"
    ]
    if not nav_trials:
        pytest.skip("no NAVARRA-AGRARIA trials without skip_ingestion")

    staging_path = tmp_path / "perennial_navarra_staging.jsonld"
    staging_graph = [
        n
        for n in subset["@graph"]
        if n.get("@type") in ("TrialSite", "ArticleSource")
        or (n.get("@type") == "VarietyTrial" and n.get("source_id") == "NAVARRA-AGRARIA")
    ]
    staging_path.write_text(
        json.dumps({**subset, "@graph": staging_graph}), encoding="utf-8"
    )
    report = validate_bundle(str(staging_path))
    assert report.ok, [e.message for e in report.errors()[:5]]

    ing = NavarraIngester(driver=driver)

    async def _run_merge():
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
        nodes = await ing.transform(str(staging_path))
        return await ing.merge(nodes)

    stats = _run(_run_merge())
    assert stats["variety_trials"] >= len(nav_trials)
