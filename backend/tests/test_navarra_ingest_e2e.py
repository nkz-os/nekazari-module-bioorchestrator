"""E2E: Navarra adequate bundle → Neo4j via BaseIngester.merge."""

from __future__ import annotations

import asyncio
import os
import shutil

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO
from app.ingestion.navarra_ingester import NavarraIngester

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
        "nkz-navarra-agraria",
        "data",
        "jsonld",
        "all_trials_adequate.jsonld",
    )
)

_loop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def driver():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = AsyncGraphDatabase.driver(
            n.get_connection_url(), auth=(n.username, n.password)
        )
        yield d
        _run(d.close())


@pytest.mark.skipif(not os.path.isfile(BUNDLE), reason="adequate bundle missing")
def test_navarra_adequate_ingest_links_and_idempotent(driver):
    ing = NavarraIngester(driver=driver)

    async def _counts():
        async with driver.session() as s:
            vt = (await (await s.run("MATCH (v:VarietyTrial) RETURN count(v) AS c")).single())["c"]
            mt = (await (await s.run("MATCH (m:ManagementTrial) RETURN count(m) AS c")).single())["c"]
            ts = (await (await s.run("MATCH (t:TrialSite) RETURN count(t) AS c")).single())["c"]
            ta = (await (
                await s.run(
                    "MATCH (:VarietyTrial)-[r:TRIAL_AT]->(:TrialSite) RETURN count(r) AS c"
                )
            ).single())["c"]
            orphan = (await (
                await s.run(
                    "MATCH (v:VarietyTrial {source_id: $sid}) "
                    "WHERE NOT (v)-[:TRIAL_AT]->(:TrialSite) RETURN count(v) AS c",
                    sid="NAVARRA-AGRARIA",
                )
            ).single())["c"]
            with_geo = (await (
                await s.run(
                    "MATCH (t:TrialSite {source_id: $sid}) "
                    "WHERE t.latitude IS NOT NULL RETURN count(t) AS c",
                    sid="NAVARRA-AGRARIA",
                )
            ).single())["c"]
        return vt, mt, ts, ta, orphan, with_geo

    async def _scenario():
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
        nodes = await ing.transform(BUNDLE)
        assert len(nodes["variety_trials"]) == 505
        assert len(nodes["trial_sites"]) == 32
        stats = await ing.merge(nodes)
        assert stats["variety_trials"] == 505
        assert stats["relationships"] > 0
        c1 = await _counts()
        await ing.merge(nodes)
        c2 = await _counts()
        return c1, c2, stats

    c1, c2, stats = _run(_scenario())
    vt, mt, ts, ta, orphan, with_geo = c1
    assert c1 == c2

    assert vt == 505
    assert mt == 55  # 224 source rows → 55 distinct management mergeKeys
    assert ts == 32
    assert ta == 505
    assert orphan == 0
    assert with_geo == 32
    assert stats["variety_trials"] == 505
    assert stats["management_trials"] == 224
    assert stats["sites"] == 32

    dao = GraphDAO(driver)
    trials = _run(dao.get_variety_trials(crop="TRZAX", limit=5))
    assert trials
