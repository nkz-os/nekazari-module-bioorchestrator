# backend/tests/test_almond_ingest_e2e.py
from __future__ import annotations
import asyncio
import os
import shutil
import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer
from app.ingestion.almond_ifapa_ingester import AlmondIfapaIngester
from app.graph.dao import GraphDAO

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")
FIX = os.path.join(os.path.dirname(__file__), "fixtures", "ifapa_almond_lastorres.jsonld")
_loop = asyncio.new_event_loop()
def _run(c): return _loop.run_until_complete(c)


@pytest.fixture(scope="module")
def driver():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        yield d
        _run(d.close())


def test_ingest_links_and_is_idempotent(driver):
    ing = AlmondIfapaIngester(driver=driver)

    async def _counts():
        async with driver.session() as s:
            vt = (await (await s.run("MATCH (v:VarietyTrial) RETURN count(v) AS c")).single())["c"]
            ta = (await (await s.run("MATCH (:VarietyTrial)-[r:TRIAL_AT]->(:TrialSite) RETURN count(r) AS c")).single())["c"]
            ur = (await (await s.run("MATCH (:VarietyTrial)-[r:USES_ROOTSTOCK]->(:Rootstock) RETURN count(r) AS c")).single())["c"]
            return vt, ta, ur

    nodes = _run(ing.transform(FIX))
    _run(ing.merge(nodes))
    vt1, ta1, ur1 = _run(_counts())
    assert vt1 == 3           # 3 distinct trials (Guara×2 rootstocks stay distinct)
    assert ta1 == 3           # all linked to Las Torres (source_id aligned, no orphans)
    assert ur1 == 3           # all carry a rootstock

    # Re-run: idempotent
    _run(ing.merge(_run(ing.transform(FIX))))
    vt2, ta2, ur2 = _run(_counts())
    assert (vt2, ta2, ur2) == (vt1, ta1, ur1)   # zero new nodes/edges

    # Consumer sees perennial fields + no fabricated yield
    dao = GraphDAO(driver)
    trials = _run(dao.get_variety_trials(crop="PRNDU", limit=50))
    guara = [t for t in trials if t["variety"] == "Guara" and t["rootstock"] == "Garnem"][0]
    assert guara["planting_year"] == 2013
    assert guara["orchard_age_years"] == 6
    lauranne = [t for t in trials if t["variety"] == "Lauranne"][0]
    assert lauranne["yield_kg_ha"] is None      # note-only, never fabricated
