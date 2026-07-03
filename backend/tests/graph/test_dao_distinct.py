"""Task 0.2 — DISTINCT audit of dao.py aggregations.

Two defects this guards against, both surfaced while auditing the trials queries:

1. A trial multi-linked to duplicate same-name TrialSites (the live G2/G9 problem)
   must be counted ONCE in extrapolate aggregations, not once per matching site.
   Without DISTINCT, avg/count are inflated and 0.4's E2E verification would get a
   false positive.
2. Regression from P0.1 (#50): `get_variety_trials` RETURN clause had duplicate
   column names (`yield_kg_ha`, `yield_note_s1` twice) → CypherSyntaxError →
   the /agriculture/variety-trials endpoint 500s in prod.
"""
from __future__ import annotations

import asyncio
import shutil

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker unavailable for testcontainers"
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


_PW = "testpassword"


@pytest.fixture(scope="module")
def neo4j_container():
    with Neo4jContainer("neo4j:5.26-community", password=_PW) as n:
        yield n


@pytest.fixture(scope="module")
def dao(neo4j_container):
    driver = AsyncGraphDatabase.driver(
        neo4j_container.get_connection_url(),
        auth=(neo4j_container.username, neo4j_container.password),
    )
    yield GraphDAO(driver)
    _run(driver.close())


def _seed_multilink(dao):
    """Two same-name sites; trial A double-linked to both, trial B to one."""
    async def _s():
        async with dao._driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
            await s.run(
                """
                CREATE (s1:TrialSite {name:'DupSite', climateClass:'Csa'})
                CREATE (s2:TrialSite {name:'DupSite', climateClass:'Csa'})
                CREATE (a:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'DUPVAR', variety:'DupVar', year:2020, yieldKgHa:9000.0, confidence:'high'})
                CREATE (b:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'DUPVAR', variety:'DupVar', year:2021, yieldKgHa:3000.0, confidence:'high'})
                CREATE (a)-[:TRIAL_AT]->(s1)
                CREATE (a)-[:TRIAL_AT]->(s2)
                CREATE (b)-[:TRIAL_AT]->(s1)
                """
            )
    _run(_s())


def test_extrapolate_counts_multilinked_trial_once(dao):
    _seed_multilink(dao)
    res = _run(dao.extrapolate_varieties(crop="TRZAX", climate_class="Csa", top_n=5))
    ranked = res.get("ranked_varieties", [])
    v = next(x for x in ranked if x["variety"] == "DUPVAR")
    # A(9000) counted once + B(3000) => mean 6000, not (9000+9000+3000)/3 = 7000.
    assert v["trial_count"] == 2
    assert v["numeric_yield_count"] == 2
    assert v["mean_yield_kg_ha"] == 6000.0
    # total_trials_analyzed must not be inflated by the phantom double-link.
    assert res["data_quality"]["total_trials_analyzed"] == 2


def test_variety_trials_no_duplicate_columns(dao):
    """get_variety_trials must not raise CypherSyntaxError and must expose
    the split yield fields (P0.1 kept yield_kg_ha + yield_note_s1)."""
    _seed_multilink(dao)
    rows = _run(dao.get_variety_trials(crop="TRZAX", climate_class="Csa"))
    assert rows, "expected trial rows"
    assert "yield_kg_ha" in rows[0]
    assert "yield_note_s1" in rows[0]
