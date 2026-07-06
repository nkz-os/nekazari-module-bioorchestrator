"""E2E: Navarra Wave 3 adequate bundle → Neo4j via NavarraIngester.merge."""

from __future__ import annotations

import asyncio
import os
import shutil

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

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
        "wave3_adequate.jsonld",
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


@pytest.mark.skipif(not os.path.isfile(BUNDLE), reason="wave3 adequate bundle missing")
def test_wave3_adequate_ingest_links_and_idempotent(driver):
    ing = NavarraIngester(driver=driver)

    async def _counts():
        async with driver.session() as s:
            vt = (await (await s.run("MATCH (v:VarietyTrial) RETURN count(v) AS c")).single())["c"]
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
        return vt, ts, ta, orphan

    async def _scenario():
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
        nodes = await ing.transform(BUNDLE)
        assert len(nodes["variety_trials"]) == 1669
        assert len(nodes["trial_sites"]) == 40
        stats = await ing.merge(nodes)
        assert stats["variety_trials"] == 1669
        assert stats["relationships"] > 0
        c1 = await _counts()
        await ing.merge(nodes)
        c2 = await _counts()
        return c1, c2, stats, len(nodes["variety_trials"])

    c1, c2, stats, transform_vt = _run(_scenario())
    vt, ts, ta, orphan = c1
    assert c1 == c2
    assert vt <= transform_vt
    assert ts == 40
    assert ta >= vt
    assert orphan == 0
    assert stats["variety_trials"] == 1669
    assert stats["sites"] == 40
