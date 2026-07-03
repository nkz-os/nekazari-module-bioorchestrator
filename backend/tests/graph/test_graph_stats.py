"""Task 0.1 — graph_quality_stats: a live quality-metrics snapshot of the graph.

This is the regression gate for every later hygiene/canonicalization task: run it
before/after a mutation and diff. Counts must use DISTINCT so a trial multi-linked
to duplicate sites is never double-counted.
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


def _seed(dao):
    async def _s():
        async with dao._driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
            # Sites: two share a name (dup group of 1), one has no climate.
            # Trials: A measured+linked (BSL), B no-yield+linked to no-climate site (CREA),
            #         C orphan measured (BSL) — never TRIAL_AT anything.
            await s.run(
                """
                CREATE (s1:TrialSite {name:'Sevilla', climateClass:'Csa'})
                CREATE (s2:TrialSite {name:'sevilla ', climateClass:'Csa'})
                CREATE (s3:TrialSite {name:'Lugo'})
                CREATE (a:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'VARA', yieldKgHa:9000.0, source_id:'BSL'})
                CREATE (b:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'VARB', source_id:'CREA'})
                CREATE (c:VarietyTrial {cropEppo:'HORVX', varietyNormalized:'VARC', yieldKgHa:5000.0, source_id:'BSL'})
                CREATE (a)-[:TRIAL_AT]->(s1)
                CREATE (b)-[:TRIAL_AT]->(s3)
                """
            )
    _run(_s())


def test_basic_counts(dao):
    """Plan Step 1: 3 trials, 2 with yield, pct rounded."""
    _seed(dao)
    st = _run(dao.graph_quality_stats())
    assert st["trials"] == 3
    assert st["trials_with_yield"] == 2
    assert st["yield_pct"] == pytest.approx(66.7)


def test_hygiene_fields(dao):
    """Orphans, dup-name groups, no-climate sites, source ids, TRIAL_AT ratio."""
    _seed(dao)
    st = _run(dao.graph_quality_stats())
    assert st["trial_sites"] == 3
    assert st["orphan_trials"] == 1            # C never TRIAL_AT
    assert st["dup_name_sites"] == 1           # 'Sevilla'/'sevilla ' collapse to 1 dup group
    assert st["sites_without_climate"] == 1    # Lugo
    assert st["source_ids"] == ["BSL", "CREA"]
    assert st["trial_at_ratio"] == pytest.approx(round(2 / 3, 2))


def test_trials_per_climate(dao):
    """Only climate-bearing sites contribute; counted DISTINCT per trial."""
    _seed(dao)
    st = _run(dao.graph_quality_stats())
    # A links Csa; B links Lugo (no climate → excluded).
    assert st["trials_per_climate"] == {"Csa": 1}
