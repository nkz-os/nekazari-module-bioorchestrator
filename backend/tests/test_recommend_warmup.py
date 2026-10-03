"""Startup warm-up of the reference-median cache."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app import main as main_mod
from app.graph.dao import GraphDAO

_SECANO = "http://aims.fao.org/aos/agrovoc/c_6436"
_REGADIO = "http://aims.fao.org/aos/agrovoc/c_3954"


async def test_warmup_requests_every_crop_for_three_regimes():
    crops = [{"eppo_code": "TRZAX"}, {"eppo_code": "HORVX"}, {"eppo_code": None}, {"eppo_code": "TRZAX"}]
    medians = AsyncMock(return_value={})
    with patch.object(GraphDAO, "get_available_crops", AsyncMock(return_value=crops)), \
            patch.object(GraphDAO, "get_crop_yield_medians", medians), \
            patch.object(main_mod, "get_driver", MagicMock(return_value=MagicMock())):
        await main_mod._warm_recommend_caches()
    assert [c.args[1] for c in medians.await_args_list] == [None, _SECANO, _REGADIO]
    assert all(c.args[0] == ["TRZAX", "HORVX"] for c in medians.await_args_list)


async def test_warmup_failure_is_swallowed(caplog):
    with patch.object(GraphDAO, "get_available_crops", AsyncMock(side_effect=RuntimeError("down"))), \
            patch.object(main_mod, "get_driver", MagicMock(return_value=MagicMock())):
        await main_mod._warm_recommend_caches()
    assert "warm-up failed" in caplog.text


async def _run_lifespan(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("RECOMMEND_WARMUP", raising=False)
    else:
        monkeypatch.setenv("RECOMMEND_WARMUP", value)
    warm = AsyncMock()
    with patch.object(main_mod, "init_driver", AsyncMock()), \
            patch.object(main_mod, "close_driver", AsyncMock()), \
            patch.object(main_mod, "_start_background_tasks", AsyncMock()), \
            patch.object(main_mod, "_warm_recommend_caches", warm):
        async with main_mod.lifespan(main_mod.app):
            await asyncio.sleep(0.05)
    return warm


@pytest.mark.parametrize("value", [None, "1"])
async def test_lifespan_schedules_warmup_when_on(monkeypatch, value):
    warm = await _run_lifespan(monkeypatch, value)
    assert warm.await_count == 1


@pytest.mark.parametrize("value", ["0", "false"])
async def test_lifespan_skips_warmup_when_off(monkeypatch, value):
    warm = await _run_lifespan(monkeypatch, value)
    assert warm.await_count == 0
