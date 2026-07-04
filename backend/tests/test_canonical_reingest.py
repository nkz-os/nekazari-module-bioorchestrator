"""Unit tests for canonical_reingest helpers."""

from __future__ import annotations

import asyncio
import shutil

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from scripts.canonical_reingest import LEGACY_MERGEKEY_MARKER, _baseline, _purge_source

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="docker unavailable for testcontainers",
)

_loop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def driver():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = AsyncGraphDatabase.driver(
            n.get_connection_url(), auth=(n.username, n.password)
        )
        yield d
        _run(d.close())


def test_baseline_detects_legacy_mergekeys(driver):
    sid = "TEST-SOURCE"

    async def _seed():
        async with driver.session() as s:
            await s.run(
                "CREATE (:VarietyTrial {source_id: $sid, mergeKey: 'short|no-eppo'})",
                sid=sid,
            )
            await s.run(
                "CREATE (:VarietyTrial {source_id: $sid, mergeKey: 'ok|eppo:TRZAX|x'})",
                sid=sid,
            )

    _run(_seed())
    stats = _run(_baseline(driver, sid))
    assert stats["vt_source"] == 2
    assert stats["legacy_mergekeys"] == 1
    assert LEGACY_MERGEKEY_MARKER in "ok|eppo:TRZAX|x"


def test_purge_source_removes_trials(driver):
    sid = "PURGE-ME"

    async def _seed():
        async with driver.session() as s:
            await s.run(
                "CREATE (:VarietyTrial {source_id: $sid, mergeKey: 'x'})",
                sid=sid,
            )
            await s.run(
                "CREATE (:ManagementTrial {source_id: $sid, mergeKey: 'y'})",
                sid=sid,
            )

    _run(_seed())
    purged = _run(_purge_source(driver, sid))
    assert purged["purged_variety_trials"] == 1
    assert purged["purged_management_trials"] == 1
    after = _run(_baseline(driver, sid))
    assert after["vt_source"] == 0
