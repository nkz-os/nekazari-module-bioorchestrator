"""C.2 — evidence gate + degradation basis on extrapolate.

A professional advisor must not present a 1-trial ranking as if it were robust.
Below N numeric trials in the (crop, climate) cell the response carries an explicit
``lowEvidence`` flag and an ``evidence.basis`` label, so the synthesis layer can
down-weight thin recommendations instead of trusting them blindly.
"""
from __future__ import annotations

import asyncio
import shutil

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO, EVIDENCE_THRESHOLD

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker unavailable for testcontainers"
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()


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


def _trials(crop, variety, n, start_year=2015):
    return "\n".join(
        f"CREATE (:VarietyTrial {{cropEppo:'{crop}', varietyNormalized:'{variety}', "
        f"variety:'{variety}', year:{start_year + i}, yieldKgHa:{8000 + i * 10}.0}})"
        f"-[:TRIAL_AT]->(site)"
        for i in range(n)
    )


def test_low_evidence_flag_below_threshold(dao):
    # Only 2 numeric trials in the (TRZAX, Csa) cell — below N.
    _seed(dao, "CREATE (site:TrialSite {name:'S', climateClass:'Csa'})\n" + _trials("TRZAX", "V", 2))
    res = _run(dao.extrapolate_varieties(crop="TRZAX", climate_class="Csa", top_n=5))
    ev = res["evidence"]
    assert ev["numericTrials"] == 2
    assert ev["threshold"] == EVIDENCE_THRESHOLD
    assert ev["lowEvidence"] is True
    assert ev["basis"] == "sparse"


def test_direct_basis_at_or_above_threshold(dao):
    _seed(dao, "CREATE (site:TrialSite {name:'S', climateClass:'Csa'})\n" + _trials("TRZAX", "V", EVIDENCE_THRESHOLD))
    res = _run(dao.extrapolate_varieties(crop="TRZAX", climate_class="Csa", top_n=5))
    ev = res["evidence"]
    assert ev["numericTrials"] == EVIDENCE_THRESHOLD
    assert ev["lowEvidence"] is False
    assert ev["basis"] == "direct"


def test_no_evidence_when_no_trials_for_crop(dao):
    _seed(dao, "CREATE (site:TrialSite {name:'S', climateClass:'Csa'})\n" + _trials("TRZAX", "V", 3))
    res = _run(dao.extrapolate_varieties(crop="ZEAMX", climate_class="Csa", top_n=5))
    ev = res["evidence"]
    assert ev["numericTrials"] == 0
    assert ev["lowEvidence"] is True
    assert ev["basis"] == "none"
