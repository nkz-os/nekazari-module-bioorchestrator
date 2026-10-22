import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app.graph.dao import GraphDAO

CELL = {"koppen": "Cfb", "annual_temp_c": 12.3, "annual_rainfall_mm": 828.0, "annual_et0_mm": 955.2,
        "coldest_month_min_c": 0.85, "monthly_tas_c": [5.0] * 12, "monthly_pr_mm": [69.0] * 12,
        "source": "CHELSA v2.1 1981-2010"}


import pytest


@pytest.fixture(autouse=True)
def _clear_negative_cache():
    from app.graph import dao as dao_mod

    dao_mod._climate_negative.clear()
    yield
    dao_mod._climate_negative.clear()


def test_migration_statement_survives_runner_parsing():
    text = (Path(__file__).parents[1] / "cypher_migrations" / "008_climate_cell.cypher").read_text()
    stmts = [s.strip() for s in text.split(";") if s.strip() and not s.strip().startswith("//")]
    assert stmts == ["CREATE CONSTRAINT climate_cell_key IF NOT EXISTS FOR (c:ClimateCell) REQUIRE c.key IS UNIQUE"]


class _FakeGraph:
    """In-memory stand-in for the ClimateCell queries (MATCH by key / MERGE)."""

    def __init__(self):
        self.rows, self.queries = {}, []

    def driver(self):
        graph = self

        class _Res:
            def __init__(self, row):
                self._row = row

            async def single(self):
                return {"c": self._row} if self._row else None

        class _Session:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return None

            async def run(self, query, **params):
                graph.queries.append(query)
                if "MERGE" in query:
                    graph.rows[params["key"]] = params
                    return _Res(None)
                return _Res(graph.rows.get(params["key"]))

        class _Driver:
            def session(self):
                return _Session()

        return _Driver()


async def test_second_call_uses_cache():
    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    with patch("app.services.chelsa_climate.read_cell", AsyncMock(return_value=CELL)) as rc:
        first = await dao.parcel_climate(42.81, -1.65, wait=True)
        second = await dao.parcel_climate(42.81, -1.65, wait=True)
    assert first == second == CELL
    assert rc.await_count == 1


async def test_read_failure_is_not_cached():
    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    with patch("app.services.chelsa_climate.read_cell", AsyncMock(return_value=None)):
        assert await dao.parcel_climate(42.81, -1.65, wait=True) is None
    assert not any("MERGE" in q for q in graph.queries)


async def test_save_uses_merge_and_camelcase():
    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    await dao.save_climate_cell("5115:-251", CELL)
    merge = next(q for q in graph.queries if "MERGE" in q)
    assert "MERGE (c:ClimateCell {key: $key})" in merge
    saved = graph.rows["5115:-251"]
    assert saved["annualRainfallMm"] == 828.0 and saved["coldestMonthMinC"] == 0.85
    assert "computedAt" in saved


async def test_miss_without_wait_returns_none_and_schedules_one_task():
    from app.graph import dao as dao_mod

    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    gate = asyncio.Event()

    async def slow_read(lat, lon, **kw):
        await gate.wait()
        return CELL

    with patch("app.services.chelsa_climate.read_cell", side_effect=slow_read) as rc:
        assert await dao.parcel_climate(42.81, -1.65) is None
        assert await dao.parcel_climate(42.81, -1.65) is None  # in flight: no second task
        assert len(dao_mod._climate_tasks) == 1
        await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(*list(dao_mod._climate_tasks.values()))
    assert rc.call_count == 1
    assert dao_mod._climate_tasks == {}
    assert await dao.parcel_climate(42.81, -1.65) == CELL  # now cached


async def test_background_failure_is_logged_not_unretrieved():
    from app.graph import dao as dao_mod

    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    # logger patched: test_logging_setup's dictConfig disables app loggers for caplog
    with patch("app.services.chelsa_climate.read_cell", AsyncMock(side_effect=RuntimeError("boom"))), \
         patch.object(dao_mod, "logger") as log:
        assert await dao.parcel_climate(42.81, -1.65) is None
        await asyncio.gather(*list(dao_mod._climate_tasks.values()), return_exceptions=True)
        await asyncio.sleep(0)
    assert dao_mod._climate_tasks == {}
    assert log.warning.call_count == 1


async def test_wait_true_awaits_read():
    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    with patch("app.services.chelsa_climate.read_cell", AsyncMock(return_value=CELL)):
        assert await dao.parcel_climate(42.81, -1.65, wait=True) == CELL


async def test_concurrent_cell_reads_are_bounded_to_two():
    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    running = peak = 0
    gate = asyncio.Event()

    async def tracked_read(lat, lon, **kw):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await gate.wait()
        running -= 1
        return CELL

    with patch("app.services.chelsa_climate.read_cell", side_effect=tracked_read):
        jobs = [
            asyncio.create_task(dao.parcel_climate(40.0 + i, -2.0, wait=True)) for i in range(5)
        ]
        for _ in range(20):
            await asyncio.sleep(0)
        assert peak == 2
        gate.set()
        results = await asyncio.gather(*jobs)
    assert all(r == CELL for r in results)
    assert peak == 2


async def test_timeout_is_forwarded_to_read_cell():
    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    with patch("app.services.chelsa_climate.read_cell", AsyncMock(return_value=CELL)) as rc:
        await dao.parcel_climate(42.81, -1.65, wait=True, timeout_s=90)
        assert rc.await_args.kwargs == {"timeout_s": 90}
        await dao.parcel_climate(10.0, -1.65, wait=True)
        assert rc.await_args.kwargs == {"timeout_s": 30.0}


async def test_negative_cache_suppresses_background_retry():
    from app.graph import dao as dao_mod

    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    with patch("app.services.chelsa_climate.read_cell", AsyncMock(return_value=None)) as rc:
        assert await dao.parcel_climate(42.81, -1.65) is None
        await asyncio.gather(*list(dao_mod._climate_tasks.values()))
        assert rc.await_count == 1
        assert await dao.parcel_climate(42.81, -1.65) is None
        assert dao_mod._climate_tasks == {}
        assert rc.await_count == 1


async def test_negative_cache_expires_after_ttl():
    from app.graph import dao as dao_mod

    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    with patch("app.services.chelsa_climate.read_cell", AsyncMock(return_value=None)) as rc:
        await dao.parcel_climate(42.81, -1.65, wait=True)
        (key,) = dao_mod._climate_negative
        assert dao_mod._climate_negative[key] > dao_mod.time.monotonic() + 590
        dao_mod._climate_negative[key] = dao_mod.time.monotonic() - 1  # expired
        assert await dao.parcel_climate(42.81, -1.65) is None
        await asyncio.gather(*list(dao_mod._climate_tasks.values()))
        assert rc.await_count == 2


async def test_wait_true_ignores_negative_cache():
    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    with patch("app.services.chelsa_climate.read_cell", AsyncMock(side_effect=[None, CELL])) as rc:
        assert await dao.parcel_climate(42.81, -1.65, wait=True) is None
        assert await dao.parcel_climate(42.81, -1.65, wait=True) == CELL
        assert rc.await_count == 2


async def test_save_with_none_month_stores_null_list():
    graph = _FakeGraph()
    dao = GraphDAO(graph.driver())
    cell = {**CELL, "monthly_tas_c": [5.0] * 11 + [None], "monthly_pr_mm": [None] + [69.0] * 11}
    await dao.save_climate_cell("k1", cell)
    assert graph.rows["k1"]["monthlyTasC"] is None
    assert graph.rows["k1"]["monthlyPrMm"] is None
    await dao.save_climate_cell("k2", CELL)
    assert graph.rows["k2"]["monthlyTasC"] == [5.0] * 12


def test_trial_site_name_index_survives_runner_parsing():
    text = (Path(__file__).parents[1] / "cypher_migrations" / "009_trial_site_name_index.cypher").read_text()
    stmts = [s.strip() for s in text.split(";") if s.strip() and not s.strip().startswith("//")]
    assert stmts == ["CREATE INDEX trial_site_name IF NOT EXISTS FOR (ts:TrialSite) ON (ts.name)"]
