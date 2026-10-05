"""Prefilter query runs on real Neo4j and mirrors extrapolate's crop/site semantics."""
from __future__ import annotations

import asyncio
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker unavailable for testcontainers"
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
_PW = "testpassword"


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def dao():
    with Neo4jContainer("neo4j:5.26-community", password=_PW) as n:
        driver = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        yield GraphDAO(driver)
        _run(driver.close())


def _seed(dao):
    async def _s():
        async with dao._driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
            await s.run(
                """
                CREATE (a:TrialSite {name:'site-a'}), (b:TrialSite {name:'site-b'})
                CREATE (t1:VarietyTrial {cropEppo:'TRZAX', yieldKgHa:5000.0})
                CREATE (t2:VarietyTrial {cropEppo:'HORVX', yieldKgHa:4000.0, rankingEligible:false})
                CREATE (t3:VarietyTrial {cropEppo:'ZEAMX', yieldNoteS1:'good'})
                CREATE (t4:VarietyTrial {cropEppo:'AVESA', yieldKgHa:3000.0})
                CREATE (t5:VarietyTrial {cropScientific:'Triticum aestivum', yieldKgHa:4500.0})
                CREATE (t1)-[:TRIAL_AT]->(a)
                CREATE (t2)-[:TRIAL_AT]->(a)
                CREATE (t3)-[:TRIAL_AT]->(a)
                CREATE (t4)-[:TRIAL_AT]->(b)
                CREATE (t5)-[:TRIAL_AT]->(a)
                """
            )
    _run(_s())


def test_prefilter_matches_extrapolate_semantics(dao):
    _seed(dao)
    out = _run(dao._crops_with_analog_trials(
        ["TRZAX", "HORVX", "ZEAMX", "AVESA", "triticum aestivum", "ZZZZZ"], ["site-a"]))
    # HORVX: not ranking-eligible; AVESA: other site. ZEAMX (note-only) is eligible, like extrapolate.
    assert out == {"TRZAX", "triticum aestivum", "ZEAMX"}


def test_prefilter_other_site(dao):
    _seed(dao)
    assert _run(dao._crops_with_analog_trials(["TRZAX", "AVESA"], ["site-b"])) == {"AVESA"}


_SECANO = "http://aims.fao.org/aos/agrovoc/c_6436"
_REGADIO = "http://aims.fao.org/aos/agrovoc/c_3954"


def _seed_regimes(dao):
    async def _s():
        async with dao._driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
            await s.run(
                """
                CREATE (a:TrialSite {name:'site-a'}), (x:TrialSite {name:'site-x'})
                CREATE (t1:VarietyTrial {cropEppo:'SECAN', varietyNormalized:'V', yieldKgHa:5000.0, irrigationRegime:$sec})
                CREATE (t2:VarietyTrial {cropEppo:'REGAD', varietyNormalized:'V', yieldKgHa:5000.0, irrigationRegime:$reg})
                CREATE (t3:VarietyTrial {cropEppo:'NOREG', varietyNormalized:'V', yieldKgHa:5000.0})
                CREATE (t4:VarietyTrial {cropEppo:'INELI', varietyNormalized:'V', yieldKgHa:5000.0, irrigationRegime:$sec, rankingEligible:false})
                CREATE (t5:VarietyTrial {cropEppo:'HELDO', varietyNormalized:'V', yieldKgHa:5000.0, irrigationRegime:$sec})
                CREATE (t8:VarietyTrial {cropScientific:'Hordeum vulgare', varietyNormalized:'V', yieldKgHa:3000.0, irrigationRegime:$sec})
                CREATE (t8)-[:TRIAL_AT]->(a)
                CREATE (t6:VarietyTrial {cropEppo:'MIXED', varietyNormalized:'V', yieldKgHa:5000.0, irrigationRegime:$reg})
                CREATE (t7:VarietyTrial {cropEppo:'MIXED', varietyNormalized:'W', yieldKgHa:4000.0, irrigationRegime:$sec})
                CREATE (t1)-[:TRIAL_AT]->(a) CREATE (t2)-[:TRIAL_AT]->(a) CREATE (t3)-[:TRIAL_AT]->(a)
                CREATE (t4)-[:TRIAL_AT]->(a) CREATE (t5)-[:TRIAL_AT]->(a) CREATE (t5)-[:TRIAL_AT]->(x)
                CREATE (t6)-[:TRIAL_AT]->(a) CREATE (t7)-[:TRIAL_AT]->(a)
                """,
                sec=_SECANO, reg=_REGADIO,
            )
    _run(_s())


@pytest.mark.parametrize("regime_uri", [None, _SECANO, _REGADIO])
@pytest.mark.parametrize("exclude", [None, ["site-x"]])
def test_prefilter_membership_equals_extrapolate_non_emptiness(dao, regime_uri, exclude):
    _seed_regimes(dao)
    crops = ["SECAN", "REGAD", "NOREG", "INELI", "HELDO", "MIXED", "ABSNT", "Hordeum", "hordeum VULGARE", "hordeum x"]
    sites = [{"name": "site-a", "distance": None}, {"name": "site-x", "distance": None}]
    regime = {None: None, _SECANO: "secano", _REGADIO: "regadío"}[regime_uri]
    members = _run(dao._crops_with_analog_trials(crops, ["site-a", "site-x"], irrigation_uri=regime_uri,
                                                 exclude_sites=exclude))
    non_empty = set()
    for c in crops:
        out = _run(dao.extrapolate_varieties(c, irrigation_regime=regime, exclude_sites=exclude,
                                             similar_sites_override=sites))
        if out["ranked_varieties"]:
            non_empty.add(c)
    assert members == non_empty
