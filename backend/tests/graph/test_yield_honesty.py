"""P0.1 — extrapolate/list must NOT coerce the qualitative 1-9 yieldNoteS1 into kg/ha.

Live defect (5th review, verified 2026-07-03): the ranking aggregations used
`COALESCE(vt.yieldKgHa, vt.yieldNoteS1 * 1000)`, so a note of 9 became 9,000 kg/ha
and was averaged/ranked against measured yields. Numeric stats must be computed
over measured `yieldKgHa` only; the note is a separate relative signal.
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
            # One Csa site, one variety, three trials:
            #  A=9000 kg/ha, B=8000 kg/ha (measured), C=note 9 only (no kg/ha).
            await s.run(
                """
                CREATE (site:TrialSite {name:'HonestSite', climateClass:'Csa', annualRainfallMm:500})
                CREATE (a:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'HONESTVAR', variety:'HonestVar', year:2020, yieldKgHa:9000.0, confidence:'high'})
                CREATE (b:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'HONESTVAR', variety:'HonestVar', year:2021, yieldKgHa:8000.0, confidence:'high'})
                CREATE (c:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'HONESTVAR', variety:'HonestVar', year:2022, yieldNoteS1:9, confidence:'high'})
                CREATE (a)-[:TRIAL_AT]->(site)
                CREATE (b)-[:TRIAL_AT]->(site)
                CREATE (c)-[:TRIAL_AT]->(site)
                """
            )
    _run(_s())


def test_extrapolate_mean_excludes_note_coerced_yield(dao):
    _seed(dao)
    res = _run(dao.extrapolate_varieties(crop="TRZAX", climate_class="Csa", top_n=5))
    ranked = res.get("ranked_varieties", [])
    assert ranked, "expected a ranking for HONESTVAR"
    v = next(x for x in ranked if x["variety"] == "HONESTVAR")
    # Mean over measured kg/ha only: (9000+8000)/2 = 8500.
    # The bug coerces note 9 -> 9000 and yields (9000+8000+9000)/3 = 8666.7.
    assert v["mean_yield_kg_ha"] == 8500.0
    # The note-only trial must never surface as a fabricated 9000 kg/ha in the stats.
    assert v["min_yield_kg_ha"] == 8000.0
    assert v["max_yield_kg_ha"] == 9000.0
