"""Startup warm-up of the recommender: runs once per common climate class after startup,
is switchable by env, never raises, and does not block readiness."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import main as main_mod


@pytest.mark.parametrize(("value", "enabled"), [
    (None, True), ("1", True), ("true", True), ("", True), ("0", False), ("false", False),
    ("No", False), (" off ", False),
])
def test_warmup_is_on_by_default_and_togglable(monkeypatch, value, enabled):
    if value is None:
        monkeypatch.delenv("RECOMMEND_WARMUP", raising=False)
    else:
        monkeypatch.setenv("RECOMMEND_WARMUP", value)
    assert main_mod._recommend_warmup_enabled() is enabled


def test_warmup_covers_the_common_climate_classes():
    assert set(main_mod._WARMUP_CLIMATE_CLASSES) == {"Csa", "Cfb", "Dfb", "BSk"}


async def test_warmup_runs_the_recommender_once_per_class_in_order():
    calls: list[dict] = []

    async def fake(self_, conditions):
        calls.append(conditions)
        return {"status": "ok"}

    with patch.object(main_mod, "_WARMUP_DELAY_S", 0), \
         patch.object(main_mod, "get_driver", return_value=MagicMock()), \
         patch.object(main_mod.GraphDAO, "recommend_for_conditions", fake):
        await main_mod._warm_recommend()
    assert [c["climate_class"] for c in calls] == list(main_mod._WARMUP_CLIMATE_CLASSES)
    assert all(c["purpose"] == "main" and c["season"] == "all" and c["management"] == "any" for c in calls)
    assert all(c["irrigation_regime"] is None and c["top_n"] == main_mod._WARMUP_TOP_N for c in calls)


async def test_a_failing_class_does_not_stop_the_others_nor_raise():
    seen: list[str] = []

    async def flaky(self_, conditions):
        seen.append(conditions["climate_class"])
        if conditions["climate_class"] == "Cfb":
            raise RuntimeError("neo4j hiccup")
        return {"status": "ok"}

    with patch.object(main_mod, "_WARMUP_DELAY_S", 0), \
         patch.object(main_mod, "get_driver", return_value=MagicMock()), \
         patch.object(main_mod.GraphDAO, "recommend_for_conditions", flaky):
        await main_mod._warm_recommend()
    assert seen == list(main_mod._WARMUP_CLIMATE_CLASSES)


async def test_no_driver_means_no_warmup_and_no_error():
    recommend = AsyncMock()
    with patch.object(main_mod, "_WARMUP_DELAY_S", 0), \
         patch.object(main_mod, "get_driver", side_effect=RuntimeError("no driver")), \
         patch.object(main_mod.GraphDAO, "recommend_for_conditions", recommend):
        await main_mod._warm_recommend()
    recommend.assert_not_awaited()


async def test_warmup_waits_before_starting_so_startup_is_not_delayed():
    started = asyncio.Event()

    async def fake(self_, conditions):
        started.set()
        return {}

    with patch.object(main_mod, "_WARMUP_DELAY_S", 0.2), \
         patch.object(main_mod, "get_driver", return_value=MagicMock()), \
         patch.object(main_mod.GraphDAO, "recommend_for_conditions", fake):
        task = asyncio.create_task(main_mod._warm_recommend())
        await asyncio.sleep(0)  # the scheduling step returns at once: the task is only waiting
        assert not started.is_set() and not task.done()
        await asyncio.wait_for(task, timeout=5)
    assert started.is_set()


async def test_background_tasks_schedule_the_warmup_only_when_enabled(monkeypatch):
    scheduled: list[str] = []

    def fake_warm():
        scheduled.append("warm")
        return asyncio.sleep(0)

    queue = MagicMock()
    queue.run_loop = AsyncMock()
    queue.register = MagicMock()
    for enabled in ("1", "0"):
        monkeypatch.setenv("RECOMMEND_WARMUP", enabled)
        scheduled.clear()
        with patch.object(main_mod.asyncio, "sleep", AsyncMock()), \
             patch("app.workers.queue.background_queue", queue), \
             patch.object(main_mod, "_warm_recommend", fake_warm), \
             patch.object(main_mod, "_ensure_catalog_subscription", AsyncMock()), \
             patch.object(main_mod, "_reconcile_catalog", AsyncMock()):
            await main_mod._start_background_tasks()
        assert scheduled == (["warm"] if enabled == "1" else [])
    for task in list(main_mod._BG_TASKS):
        task.cancel()
    main_mod._BG_TASKS.clear()
