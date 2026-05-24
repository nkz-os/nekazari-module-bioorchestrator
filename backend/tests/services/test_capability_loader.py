"""Loader fetches a capabilities.yaml URL and upserts into Neo4j."""
from __future__ import annotations
import asyncio

import httpx
import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.graph.capability_dao import CapabilityDao
from app.services.capability_loader import CapabilityLoader


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
    user = neo4j_container.username
    password = neo4j_container.password

    async def _build():
        driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
        return CapabilityDao(driver)

    instance = _run(_build())
    yield instance
    _run(instance.close())


SAMPLE_YAML = """
moduleId: testmod
version: 0.1.0
publishes:
  - entityType: TestEntity
    sdmStatus: official
    refRelationship: refX
    attributes:
      - name: a
        unitCode: P1
        temporal: static
        spatial: scalar-parcel
        sources: [src1]
        entitlement: open
      - name: b
        unitCode: GL3
        temporal: timeseries
        spatial: point
        sources: [src2]
        entitlement: tier-pro
      - name: c
        unitCode: null
        temporal: static
        spatial: raster
        sources: [derived]
        entitlement: esdb-noncommercial
"""


def test_loader_upserts_three_attributes(dao, monkeypatch):
    class _MockResp:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    monkeypatch.setattr(httpx, "get", lambda url, timeout=10: _MockResp(SAMPLE_YAML))

    loader = CapabilityLoader(dao)
    n = _run(loader.load_from_url("http://testmod/capabilities.yaml"))
    assert n == 3

    catalog = _run(dao.list_catalog())
    names = {c["attributeName"] for c in catalog if c["entityType"] == "TestEntity"}
    assert names == {"a", "b", "c"}


def test_loader_marks_stale_capabilities_as_deprecated(dao, monkeypatch):
    """After re-loading with fewer attributes, the missing ones get :DEPRECATED label."""
    smaller_yaml = """
moduleId: testmod
version: 0.2.0
publishes:
  - entityType: TestEntity
    sdmStatus: official
    refRelationship: refX
    attributes:
      - name: a
        unitCode: P1
        temporal: static
        spatial: scalar-parcel
        sources: [src1]
        entitlement: open
"""

    class _MockResp:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            pass

    monkeypatch.setattr(httpx, "get", lambda url, timeout=10: _MockResp(smaller_yaml))

    loader = CapabilityLoader(dao)
    n = _run(loader.load_from_url("http://testmod/capabilities.yaml"))
    assert n == 1

    # The previously-active b and c should now carry the DEPRECATED label.
    async def _count_deprecated():
        async with dao._driver.session() as s:
            r = await s.run(
                "MATCH (c:Capability) WHERE c.deprecatedAt IS NOT NULL "
                "AND c.entityType = 'TestEntity' RETURN count(c) AS n"
            )
            rec = await r.single()
            return rec["n"]

    assert _run(_count_deprecated()) >= 2  # at least b and c
