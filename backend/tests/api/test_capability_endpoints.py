"""HTTP endpoints for the Capability Registry: /catalog, /parcel/{id}, /attribute/{type}/{name}.

Strategy: share the Neo4j container across tests (module scope, sync) but
create a fresh AsyncDriver + CapabilityDao inside each async test so the driver
is always bound to the same anyio event loop as the test coroutine.
Seed data once per module via a session-level seed flag.
"""
from __future__ import annotations

import os

import httpx
import pytest
import respx
from fastapi import FastAPI
from httpx import ASGITransport
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.api.v1.capability import router as capability_router, get_capability_dao
from app.graph.capability_dao import CapabilityDao


pytestmark = pytest.mark.anyio

# Container is started once for the whole module (sync fixture — no loop involved).
_container: Neo4jContainer | None = None
_seeded = False


@pytest.fixture(scope="module", autouse=True)
def neo4j_container():
    """Start a Neo4j container for the module; populate _container global."""
    global _container
    with Neo4jContainer("neo4j:5.26-community") as n:
        _container = n
        yield n
    _container = None


async def _make_dao() -> CapabilityDao:
    """Create a fresh AsyncDriver + CapabilityDao on the current event loop."""
    assert _container is not None, "neo4j_container fixture must be active"
    driver = AsyncGraphDatabase.driver(
        _container.get_connection_url(),
        auth=(_container.username, _container.password),
    )
    return CapabilityDao(driver)


async def _seed_if_needed(dao: CapabilityDao) -> None:
    """Seed exactly once across all tests in this module."""
    global _seeded
    if _seeded:
        return
    await dao.upsert_module(module_id="soil", version="0.2.0")
    await dao.upsert_capability(
        module_id="soil", entity_type="AgriSoilExtended", attribute_name="clayContent",
        unit_code="P1", temporal="static", spatial="polygon-aggregated",
        sources=["LUCAS"], entitlement="open",
        sdm_status="draft-proposal", sdm_proposal="SDM-001",
    )
    await dao.upsert_capability(
        module_id="soil", entity_type="AgriSoilExtended", attribute_name="organicCarbon",
        unit_code="P1", temporal="static", spatial="polygon-aggregated",
        sources=["LUCAS", "SoilGrids"], entitlement="open",
        sdm_status="draft-proposal", sdm_proposal="SDM-001",
    )
    await dao.upsert_capability(
        module_id="soil", entity_type="AgriSoilExtended", attribute_name="esdbOnly",
        unit_code=None, temporal="static", spatial="polygon-aggregated",
        sources=["ESDB-vector"], entitlement="esdb-noncommercial",
        sdm_status="draft-proposal", sdm_proposal="SDM-001",
    )
    _seeded = True


def _make_test_app(dao: CapabilityDao) -> FastAPI:
    """Minimal FastAPI app with only the capability router; no auth middleware."""
    mini = FastAPI()
    mini.dependency_overrides[get_capability_dao] = lambda: dao
    mini.include_router(capability_router, prefix="/api/capability")
    return mini


async def test_catalog_returns_grouped_capabilities(neo4j_container):
    dao = await _make_dao()
    await _seed_if_needed(dao)
    try:
        app = _make_test_app(dao)
        async with httpx.AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            r = await client.get("/api/capability/catalog")
        assert r.status_code == 200
        body = r.json()
        assert "AgriSoilExtended" in body
        attrs = {a["attributeName"] for a in body["AgriSoilExtended"]}
        assert {"clayContent", "organicCarbon", "esdbOnly"} <= attrs
    finally:
        await dao.close()


async def test_attribute_detail_returns_full_metadata(neo4j_container):
    dao = await _make_dao()
    await _seed_if_needed(dao)
    try:
        app = _make_test_app(dao)
        async with httpx.AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            r = await client.get("/api/capability/attribute/AgriSoilExtended/organicCarbon")
        assert r.status_code == 200
        body = r.json()
        cap = body["capability"]
        assert cap["entityType"] == "AgriSoilExtended"
        assert cap["attributeName"] == "organicCarbon"
        assert cap["sdmProposal"] == "SDM-001"
        assert "LUCAS" in cap["sources"]
    finally:
        await dao.close()


async def test_attribute_detail_returns_404_for_unknown(neo4j_container):
    dao = await _make_dao()
    await _seed_if_needed(dao)
    try:
        app = _make_test_app(dao)
        async with httpx.AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            r = await client.get("/api/capability/attribute/Unknown/foo")
        assert r.status_code == 404
    finally:
        await dao.close()


@respx.mock
async def test_parcel_endpoint_aggregates_capabilities_and_entities(neo4j_container, monkeypatch):
    monkeypatch.setenv("ORION_BASE_URL", "http://orion-mock:1026")
    monkeypatch.setenv("CONTEXT_URL", "http://ctx/context.jsonld")

    # Orion returns one AgriSoilExtended entity for the parcel.
    respx.get("http://orion-mock:1026/ngsi-ld/v1/entities").mock(
        return_value=httpx.Response(200, json=[
            {"id": "urn:ngsi-ld:AgriSoilExtended:p-1", "type": "AgriSoilExtended"}
        ])
    )

    dao = await _make_dao()
    await _seed_if_needed(dao)
    try:
        app = _make_test_app(dao)
        async with httpx.AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            r = await client.get("/api/capability/parcel/p-1")
        assert r.status_code == 200
        body = r.json()
        assert body["parcelId"] == "p-1"
        assert len(body["capabilities"]) >= 3  # 3 seeded
        assert "AgriSoilExtended" in body["currentEntities"]
        assert body["currentEntities"]["AgriSoilExtended"][0]["id"] == "urn:ngsi-ld:AgriSoilExtended:p-1"
    finally:
        await dao.close()
