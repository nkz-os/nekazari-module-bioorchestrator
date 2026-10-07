"""A requested irrigation regime never pools a trial whose SOURCE states the opposite regime (no
down-weighting); a trial whose source states none stays and is labelled unknown. Real Neo4j."""
from __future__ import annotations

import asyncio
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph import evidence_policy as ep
from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

pytestmark = [
    pytest.mark.real_batch,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable for testcontainers"),
]

_loop = asyncio.new_event_loop()
_RAINFED = ep.IRRIGATION_URIS[ep.REGIME_RAINFED]
_IRRIGATED = ep.IRRIGATION_URIS[ep.REGIME_IRRIGATED]
_SITE = "Zona X"
# (variety, kg/ha, regime stated by the source)
_TRIALS = [
    ("V-RAINFED", 4000.0, _RAINFED),
    ("V-IRRIGATED", 12000.0, _IRRIGATED),
    ("V-UNKNOWN", 8000.0, None),
    ("V-LITERAL", 5000.0, "secano"),
]


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def dao():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        driver = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        d = GraphDAO(driver)

        async def seed():
            async with driver.session() as s:
                await s.run("CREATE INDEX trial_site_name IF NOT EXISTS FOR (ts:TrialSite) ON (ts.name)")
                await s.run("CREATE (:TrialSite {name: $n, siteKey: 'Z', siteKind: 'aggregate', country: 'ES'})",
                            n=_SITE)
                await s.run(
                    """UNWIND $rows AS r MATCH (ts:TrialSite {name: $site})
                    CREATE (vt:VarietyTrial {cropEppo: 'HORVX', varietyNormalized: r.v, yieldKgHa: r.kg, year: 2020,
                                             aggregationScope: 'site', source_id: 'S', mergeKey: r.v})
                    SET vt.irrigationRegime = r.reg
                    CREATE (vt)-[:TRIAL_AT]->(ts)""",
                    rows=[{"v": v, "kg": k, "reg": r} for v, k, r in _TRIALS], site=_SITE)
        _run(seed())
        yield d
        _run(driver.close())


def _varieties(dao, regime):
    out = _run(dao.extrapolate_varieties(
        "HORVX", similar_sites_override=[{"name": _SITE, "site_kind": "aggregate", "distance": None}],
        irrigation_regime=regime, top_n=10, tier="regional"))
    return {v["variety"]: v for v in out["ranked_varieties"]}


def test_rainfed_request_drops_stated_irrigated_and_keeps_unknown(dao):
    got = _varieties(dao, "secano")
    assert set(got) == {"V-RAINFED", "V-LITERAL", "V-UNKNOWN"}
    assert got["V-UNKNOWN"]["irrigation_unknown_trial_count"] == 1
    assert got["V-RAINFED"]["irrigation_unknown_trial_count"] == 0


def test_irrigated_request_drops_stated_rainfed_literal_included(dao):
    got = _varieties(dao, "regadío")
    assert set(got) == {"V-IRRIGATED", "V-UNKNOWN"}


def test_no_request_pools_everything(dao):
    assert set(_varieties(dao, None)) == {"V-RAINFED", "V-IRRIGATED", "V-UNKNOWN", "V-LITERAL"}


def test_contradicting_trial_is_not_down_weighted_into_the_mean(dao):
    # one variety, two trials of opposite stated regimes: a rainfed request sees only the rainfed one
    async def seed():
        async with dao._driver.session() as s:
            await s.run(
                """MATCH (ts:TrialSite {name: $site})
                UNWIND [['M', 3000.0, $r, 'a'], ['M', 9000.0, $i, 'b']] AS t
                CREATE (vt:VarietyTrial {cropEppo: 'HORVX', varietyNormalized: t[0], yieldKgHa: t[1], year: 2020,
                                         aggregationScope: 'site', source_id: 'S', mergeKey: 'M' + t[3],
                                         irrigationRegime: t[2]})
                CREATE (vt)-[:TRIAL_AT]->(ts)""", site=_SITE, r=_RAINFED, i=_IRRIGATED)
    _run(seed())
    assert _varieties(dao, "secano")["M"]["mean_yield_kg_ha"] == 3000.0
    assert _varieties(dao, "regadío")["M"]["mean_yield_kg_ha"] == 9000.0


def test_evidence_page_excludes_contradiction_and_labels_unknown(dao):
    page = _run(dao.list_trial_evidence(
        crop="HORVX", similar_sites=[_SITE], variety=None, irrigation_uri=_RAINFED,
        page=1, page_size=50, purpose="main", tier="regional"))
    status = {i["variety"]: i["irrigation_status"] for i in page["items"]}
    assert status["V-UNKNOWN"] == "unknown" and status["V-RAINFED"] == "stated"
    assert "V-IRRIGATED" not in status
    plain = _run(dao.list_trial_evidence(
        crop="HORVX", similar_sites=[_SITE], variety=None, irrigation_uri=None,
        page=1, page_size=50, purpose="main", tier="regional"))
    assert {i["irrigation_status"] for i in plain["items"]} == {None}
