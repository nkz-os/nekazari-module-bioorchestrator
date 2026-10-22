"""Extrapolation memory must not scale with ``top_n`` (real Neo4j, small transaction limit)."""
from __future__ import annotations

import asyncio
import random
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

pytestmark = [
    pytest.mark.real_batch,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable for testcontainers"),
]

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
_PW = "testpassword"
_SITES = [f"site-{i}" for i in range(20)]
_CROPS = ["ZEAMX", "TRZAX"]
_VARIETIES = 1500


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def dao():
    # A transaction limit far below production's: a query whose memory grows with top_n
    # (a sort buffering rows that each carry the crop's whole trial list) fails here.
    with Neo4jContainer("neo4j:5.26-community", password=_PW).with_env(
            "NEO4J_db_memory_transaction_max", "96m") as n:
        driver = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        d = GraphDAO(driver)
        _run(_seed(d))
        yield d
        _run(driver.close())
    _loop.close()


async def _seed(dao):
    rnd = random.Random(3)
    trials = [{"id": i, "crop": rnd.choice(_CROPS), "variety": f"V{rnd.randrange(_VARIETIES)}",
               "yield": rnd.choice([4000.0, 5200.5, 6100.0, 7300.0, 8800.0]),
               "year": rnd.choice([2015, 2019, 2022]),
               "sites": sorted({rnd.randrange(len(_SITES)) for _ in range(rnd.choice([1, 2]))})}
              for i in range(9000)]
    async with dao._driver.session() as s:
        await s.run("UNWIND range(0, $n - 1) AS i CREATE (:TrialSite {name: 'site-' + i, idx: i})",
                    n=len(_SITES))
        for k in range(0, len(trials), 1500):
            await s.run(
                """
                UNWIND $trials AS t
                CREATE (vt:VarietyTrial {cropEppo: t.crop, varietyNormalized: t.variety,
                        yieldKgHa: t.yield, year: t.year, source_id: 'S'})
                WITH vt, t UNWIND t.sites AS si
                MATCH (ts:TrialSite {idx: si}) CREATE (vt)-[:TRIAL_AT]->(ts)
                """,
                trials=trials[k:k + 1500],
            )


_SITE_LIST = [{"name": n, "distance": None} for n in _SITES]


def test_single_crop_large_top_n(dao):
    full = _run(dao.extrapolate_varieties("ZEAMX", similar_sites_override=_SITE_LIST, top_n=5000))
    ranked = full["ranked_varieties"]
    assert len(ranked) > 1000
    for n in (10, 100, 500):
        cut = _run(dao.extrapolate_varieties("ZEAMX", similar_sites_override=_SITE_LIST, top_n=n))
        assert cut["ranked_varieties"] == ranked[:n]


def test_batch_large_top_n(dao):
    full = _run(dao.extrapolate_varieties_batch(_CROPS, _SITE_LIST, top_n=5000))
    cut = _run(dao.extrapolate_varieties_batch(_CROPS, _SITE_LIST, top_n=500))
    assert {c: v[:500] for c, v in full.items()} == cut
    assert all(len(v) > 1000 for v in full.values())
