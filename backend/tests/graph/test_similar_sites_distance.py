"""C.1 — get_similar_sites ranks by agro-climatic distance, Köppen as soft fallback.

Replaces the old hard `climateClass =` filter + alphabetical order. Given a target
agro-climatic vector, enriched sites are ranked by ascending distance (nearer analog
first, distance exposed); a same-Köppen site *without* the numeric vector is not
dropped — it is included with a penalty distance so coverage is preserved.
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


# NEAR and FAR are both Köppen 'Csa'; DRYNOVEC is 'Csa' but has no numeric vector.
_SEED = """
CREATE (:TrialSite {name:'NEAR', climateClass:'Csa', annualRainfallMm:520, annualET0Mm:1000, frostDaysPerYear:38, elevationM:320})
CREATE (:TrialSite {name:'FAR',  climateClass:'Csa', annualRainfallMm:1000, annualET0Mm:1000, frostDaysPerYear:5,  elevationM:300})
CREATE (:TrialSite {name:'NOVEC', climateClass:'Csa'})
"""

_TARGET = {"rainfall": 500, "et0": 1000, "frost": 40, "elevation": 300}


def test_ranks_by_distance_not_alphabetical(dao):
    _seed(dao, _SEED)
    sites = _run(dao.get_similar_sites(climate_class="Csa", target_features=_TARGET, limit=10))
    names = [s["name"] for s in sites]
    # NEAR (aridity 0.52, frost 38) is agro-climatically closest to the target;
    # FAR (humid, frost-free) is farther, though both are 'Csa'.
    assert names.index("NEAR") < names.index("FAR")
    near = next(s for s in sites if s["name"] == "NEAR")
    far = next(s for s in sites if s["name"] == "FAR")
    assert near["distance"] < far["distance"]


def test_koppen_only_site_kept_with_penalty(dao):
    _seed(dao, _SEED)
    sites = _run(dao.get_similar_sites(climate_class="Csa", target_features=_TARGET, limit=10))
    names = [s["name"] for s in sites]
    # The vector-less same-Köppen site is NOT dropped (coverage), but ranks last.
    assert "NOVEC" in names
    novec = next(s for s in sites if s["name"] == "NOVEC")
    far = next(s for s in sites if s["name"] == "FAR")
    assert novec["distance"] > far["distance"]


def test_no_target_vector_falls_back_to_koppen_filter(dao):
    _seed(dao, _SEED)
    # Legacy path (no target_features): behave as a Köppen filter, all 'Csa' returned.
    sites = _run(dao.get_similar_sites(climate_class="Csa", limit=10))
    assert {s["name"] for s in sites} == {"NEAR", "FAR", "NOVEC"}


# ── extrapolate weights trials by site distance ──────────────────────────────

_WEIGHT_SEED = """
CREATE (near:TrialSite {name:'NEAR', climateClass:'Csa', annualRainfallMm:520, annualET0Mm:1000, frostDaysPerYear:38, elevationM:320})
CREATE (far:TrialSite  {name:'FAR',  climateClass:'Csa', annualRainfallMm:1000, annualET0Mm:1000, frostDaysPerYear:5,  elevationM:300})
CREATE (n:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2020, yieldKgHa:6000.0})
CREATE (f:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2020, yieldKgHa:9000.0})
CREATE (n)-[:TRIAL_AT]->(near)
CREATE (f)-[:TRIAL_AT]->(far)
"""


def test_extrapolate_weights_by_site_distance(dao):
    _seed(dao, _WEIGHT_SEED)
    # Flat average (legacy, no target) = (6000+9000)/2 = 7500.
    flat = _run(dao.extrapolate_varieties(crop="TRZAX", climate_class="Csa", top_n=5))
    assert next(v for v in flat["ranked_varieties"] if v["variety"] == "V")["mean_yield_kg_ha"] == 7500.0

    # With a target near NEAR, NEAR's 6000 must dominate → weighted mean below 7500.
    weighted = _run(dao.extrapolate_varieties(
        crop="TRZAX", climate_class="Csa", top_n=5,
        target_features={"rainfall": 500, "et0": 1000, "frost": 40, "elevation": 300},
    ))
    mv = next(v for v in weighted["ranked_varieties"] if v["variety"] == "V")["mean_yield_kg_ha"]
    assert 6000.0 <= mv < 7500.0
