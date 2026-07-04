"""E2E: INIAV adequate bundle → Neo4j via BaseIngester.merge."""

from __future__ import annotations

import asyncio
import os
import shutil

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.ingestion.base_ingester import BaseIngester
from app.ingestion.iniav_ingester import IniavIngester

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
        "nkz-iniav-scraper",
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
def test_iniav_adequate_ingest_links_and_idempotent(driver):
    ing = IniavIngester(driver=driver)
    sid = IniavIngester.SOURCE_ID

    async def _counts():
        async with driver.session() as s:
            vt = (await (await s.run("MATCH (v:VarietyTrial) RETURN count(v) AS c")).single())["c"]
            ta = (await (
                await s.run(
                    "MATCH (:VarietyTrial {source_id: $sid})-[r:TRIAL_AT]->(:TrialSite) "
                    "RETURN count(r) AS c",
                    sid=sid,
                )
            ).single())["c"]
            orphan = (await (
                await s.run(
                    "MATCH (v:VarietyTrial {source_id: $sid}) "
                    "WHERE NOT (v)-[:TRIAL_AT]->(:TrialSite) RETURN count(v) AS c",
                    sid=sid,
                )
            ).single())["c"]
            with_geo = (await (
                await s.run(
                    "MATCH (t:TrialSite {source_id: $sid}) "
                    "WHERE t.latitude IS NOT NULL RETURN count(t) AS c",
                    sid=sid,
                )
            ).single())["c"]
        return vt, ta, orphan, with_geo

    async def _scenario():
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
        nodes = await ing.transform(BUNDLE)
        assert len(nodes["variety_trials"]) == 409
        assert len(nodes["trial_sites"]) == 2
        expected_unique = {
            uk
            for v in nodes["variety_trials"]
            for uk in [BaseIngester._variety_unique_key(v)]
            if uk
        }
        stats = await ing.merge(nodes)
        c1 = await _counts()
        await ing.merge(nodes)
        c2 = await _counts()
        return nodes, stats, expected_unique, c1, c2

    _, stats, expected_unique, c1, c2 = _run(_scenario())
    assert c1 == c2
    vt1, ta1, orphan1, geo1 = c1
    assert vt1 == len(expected_unique)
    assert ta1 == len(expected_unique)
    assert orphan1 == 0
    assert geo1 == 2
    assert stats["variety_trials"] == 409
    assert stats["relationships"] == len(expected_unique)
