"""Capability DAO upsert + query semantics (async Neo4j driver)."""
from __future__ import annotations
import asyncio
import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.graph.capability_dao import CapabilityDao


# One event loop reused for all tests so the Neo4j async driver stays bound
# to the same loop it was created on.
_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def neo4j_container():
    with Neo4jContainer("neo4j:5.26-community") as n:
        yield n


@pytest.fixture(scope="module")
def dao(neo4j_container):
    uri = neo4j_container.get_connection_url()
    username = neo4j_container.username
    password = neo4j_container.password

    async def _build():
        driver = AsyncGraphDatabase.driver(uri, auth=(username, password))
        return CapabilityDao(driver)

    instance = _run(_build())
    yield instance
    _run(instance.close())


def test_upsert_module_inserts_module_node(dao):
    _run(dao.upsert_module(module_id="soil", version="0.2.0"))

    async def _check():
        async with dao._driver.session() as s:
            r = await s.run("MATCH (m:Module {id:'soil'}) RETURN m.version AS v")
            rec = await r.single()
            return rec["v"] if rec else None

    assert _run(_check()) == "0.2.0"


def test_upsert_capability_creates_publishes_relationship(dao):
    _run(dao.upsert_module(module_id="soil", version="0.2.0"))
    _run(dao.upsert_capability(
        module_id="soil",
        entity_type="AgriSoilExtended",
        attribute_name="clayContent",
        unit_code="P1",
        temporal="static",
        spatial="polygon-aggregated",
        sources=["LUCAS", "ESDB-Raster"],
        entitlement="open",
        sdm_status="draft-proposal",
        sdm_proposal="SDM-001",
    ))

    async def _check():
        async with dao._driver.session() as s:
            r = await s.run(
                "MATCH (m:Module {id:'soil'})-[:PUBLISHES]->(c:Capability "
                "{entityType:'AgriSoilExtended', attributeName:'clayContent'}) "
                "RETURN c.entitlement AS ent, c.sdmProposal AS sdm"
            )
            rec = await r.single()
            return (rec["ent"], rec["sdm"]) if rec else None

    assert _run(_check()) == ("open", "SDM-001")


def test_upsert_capability_is_idempotent(dao):
    _run(dao.upsert_module(module_id="soil", version="0.2.0"))
    for _ in range(3):
        _run(dao.upsert_capability(
            module_id="soil", entity_type="X", attribute_name="y",
            unit_code=None, temporal="static", spatial="scalar-parcel",
            sources=["A"], entitlement="open", sdm_status="official", sdm_proposal=None,
        ))

    async def _count():
        async with dao._driver.session() as s:
            r = await s.run("MATCH (:Capability {entityType:'X', attributeName:'y'}) RETURN count(*) AS n")
            rec = await r.single()
            return rec["n"]

    assert _run(_count()) == 1
