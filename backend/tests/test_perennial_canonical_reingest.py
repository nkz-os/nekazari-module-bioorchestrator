"""Unit + e2e tests for perennial multi-source canonical re-ingest."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.ingestion.almond_ifapa_ingester import AlmondIfapaIngester
from scripts.perennial_canonical_reingest import (
    prepare_approved_graph,
    subgraph_for_source,
)

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


@pytest.mark.skipif(not os.path.isfile(BUNDLE), reason="perennial candidate missing")
def test_prepare_approved_drops_scientia_and_flags():
    raw = json.loads(Path(BUNDLE).read_text(encoding="utf-8"))
    approved = prepare_approved_graph(raw)
    sources = {n.get("source_id") for n in approved["@graph"] if n.get("source_id")}
    assert "SCIENTIA-PIAVE" not in sources
    assert all("skip_ingestion" not in n for n in approved["@graph"])
    vt = [n for n in approved["@graph"] if n.get("@type") == "VarietyTrial"]
    assert len(vt) == 119


@pytest.mark.skipif(not os.path.isfile(BUNDLE), reason="perennial candidate missing")
def test_subgraph_isolates_sources():
    approved = prepare_approved_graph(json.loads(Path(BUNDLE).read_text(encoding="utf-8")))
    graph = approved["@graph"]
    nav = subgraph_for_source(graph, "NAVARRA-AGRARIA")
    nav_vt = [n for n in nav if n.get("@type") == "VarietyTrial"]
    assert len(nav_vt) == 91
    assert all(n.get("source_id") == "NAVARRA-AGRARIA" for n in nav_vt)
    almond = subgraph_for_source(graph, "IFAPA_ALMOND")
    almond_vt = [n for n in almond if n.get("@type") == "VarietyTrial"]
    assert len(almond_vt) == 16
    assert not any(n.get("source_id") == "NAVARRA-AGRARIA" for n in almond)


def test_almond_honors_bundle_mergekey():
    ing = AlmondIfapaIngester()
    node = {
        "@type": "VarietyTrial",
        "crop_eppo": "eppo:PRNDU",
        "variety": "Guara",
        "rootstock": "Garnem",
        "trial_location": "Alcalá del Río",
        "year": 2019,
        "mergeKey": "ifapa_almond|eppo:PRNDU|guara|garnem|alcalá del río|2019",
    }
    out = ing._convert_trial(node)
    assert out["mergeKey"] == node["mergeKey"]


@pytest.fixture(scope="module")
def driver():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = AsyncGraphDatabase.driver(
            n.get_connection_url(), auth=(n.username, n.password)
        )
        yield d
        _run(d.close())


@pytest.mark.skipif(not os.path.isfile(BUNDLE), reason="perennial candidate missing")
def test_perennial_full_bundle_staging_merge(driver, tmp_path):
    """Full approved bundle — all three sources, empty graph."""
    from scripts.perennial_canonical_reingest import run

    approved = prepare_approved_graph(json.loads(Path(BUNDLE).read_text(encoding="utf-8")))
    staging = tmp_path / "perennial_approved.jsonld"
    staging.write_text(json.dumps(approved), encoding="utf-8")

    async def _empty_then_ingest():
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
        return await run(str(staging), dry_run=False, execute=True, driver=driver)

    code = _run(_empty_then_ingest())
    assert code == 0

    async def _counts():
        async with driver.session() as s:
            vt = (await (await s.run("MATCH (v:VarietyTrial) RETURN count(v) AS c")).single())[
                "c"
            ]
            orphan = (
                await (
                    await s.run(
                        "MATCH (v:VarietyTrial) "
                        "WHERE NOT (v)-[:TRIAL_AT]->(:TrialSite) RETURN count(v) AS c"
                    )
                ).single()
            )["c"]
            return vt, orphan

    vt, orphan = _run(_counts())
    assert vt == 119
    assert orphan == 0
