"""C.4 — temporal recency weighting + soft water-regime comparability.

Extrapolate must not pool a 2005 trial with a 2023 trial at equal weight (genetics
and climate move), nor silently average German rainfed with peninsular irrigated.
Temporal: a recency half-life down-weights old trials. Comparability: when a target
water regime is given, opposite-regime trials are down-weighted (soft, not a hard
split — 40% of trials have no regime and must not be dropped).
"""
from __future__ import annotations

import asyncio
import datetime
import shutil

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker unavailable for testcontainers"
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
_NOW = datetime.date.today().year

_RAINFED = "http://aims.fao.org/aos/agrovoc/c_6436"
_IRRIGATED = "http://aims.fao.org/aos/agrovoc/c_3954"


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def neo4j_container():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        yield n


@pytest.fixture(scope="module")
def dao(neo4j_container):
    driver = AsyncGraphDatabase.driver(
        neo4j_container.get_connection_url(),
        auth=(neo4j_container.username, neo4j_container.password),
    )
    yield GraphDAO(driver)
    _run(driver.close())


def _seed(dao, cypher):
    async def _s():
        async with dao._driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
            await s.run(cypher)
    _run(_s())


def test_recent_trial_outweighs_old_one(dao):
    # Same variety, same site: an OLD high-yield trial vs a RECENT low-yield one.
    _seed(dao, f"""
        CREATE (s:TrialSite {{name:'S', climateClass:'Csa'}})
        CREATE (o:VarietyTrial {{cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:{_NOW - 19}, yieldKgHa:9000.0}})
        CREATE (n:VarietyTrial {{cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:{_NOW - 1}, yieldKgHa:6000.0}})
        CREATE (o)-[:TRIAL_AT]->(s)
        CREATE (n)-[:TRIAL_AT]->(s)
    """)
    res = _run(dao.extrapolate_varieties(crop="TRZAX", climate_class="Csa", top_n=5))
    mv = next(v for v in res["ranked_varieties"] if v["variety"] == "V")["mean_yield_kg_ha"]
    # Flat mean would be 7500; recency pulls it toward the recent 6000.
    assert 6000.0 <= mv < 7500.0


def test_opposite_regime_downweighted_when_target_given(dao):
    _seed(dao, f"""
        CREATE (s:TrialSite {{name:'S', climateClass:'Csa'}})
        CREATE (r:VarietyTrial {{cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:{_NOW - 1}, yieldKgHa:9000.0, irrigationRegime:'{_RAINFED}'}})
        CREATE (i:VarietyTrial {{cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:{_NOW - 1}, yieldKgHa:6000.0, irrigationRegime:'{_IRRIGATED}'}})
        CREATE (r)-[:TRIAL_AT]->(s)
        CREATE (i)-[:TRIAL_AT]->(s)
    """)
    # Target = rainfed: the irrigated 6000 trial is down-weighted → mean toward 9000.
    res = _run(dao.extrapolate_varieties(crop="TRZAX", climate_class="Csa", irrigation_regime="secano", top_n=5))
    mv = next(v for v in res["ranked_varieties"] if v["variety"] == "V")["mean_yield_kg_ha"]
    assert 7500.0 < mv <= 9000.0
