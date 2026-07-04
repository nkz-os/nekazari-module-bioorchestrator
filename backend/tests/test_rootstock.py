# backend/tests/test_rootstock.py
from __future__ import annotations
import asyncio
import shutil
import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer
from app.ingestion.almond_ifapa_ingester import AlmondIfapaIngester  # Task 4

pytestmark = pytest.mark.skipif(shutil.which("docker") is None,
                                reason="docker unavailable")
_loop = asyncio.new_event_loop()
def _run(c): return _loop.run_until_complete(c)


@pytest.fixture(scope="module")
def container():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        yield n


def test_rootstock_node_and_edge(container):
    driver = AsyncGraphDatabase.driver(container.get_connection_url(),
                                       auth=(container.username, container.password))
    ing = AlmondIfapaIngester(driver=driver)
    nodes = {
        "trial_sites": [{"name": "Las Torres", "municipality": "Alcalá del Río",
                         "climateClass": "Csa", "mergeKey": "ifapa_almond|las torres|alcalá del río"}],
        "article_sources": [],
        "variety_trials": [{
            "cropEppo": "PRNDU", "cropScientific": "Prunus dulcis",
            "variety": "Guara", "rootstock": "Garnem", "year": 2019,
            "yieldKgHa": 2100.0, "trialLocation": "Las Torres",
            "source_id": "IFAPA_ALMOND",
            "mergeKey": "ifapa_almond|prndu|guara|las torres|2019"}],
        "management_trials": [],
    }
    stats = _run(ing.merge(nodes))
    assert stats["rootstocks"] == 1

    async def _check():
        async with driver.session() as s:
            r = await s.run(
                "MATCH (vt:VarietyTrial)-[:USES_ROOTSTOCK]->(rs:Rootstock) "
                "RETURN rs.name AS name, rs.mergeKey AS mk")
            return await r.single()
    row = _run(_check())
    assert row["name"] == "Garnem"
    assert row["mk"] == "rootstock|garnem|prndu"
    _run(driver.close())
