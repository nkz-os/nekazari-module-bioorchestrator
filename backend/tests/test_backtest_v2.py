"""Backtester.run(strategy=...) wiring and fold-query CHELSA columns."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.eval.backtest import Backtester

_FOLD = {
    "site": "S1", "climate": "Csa", "crop": "TRZAX",
    "rainfall": 500, "et0": 1000, "frost": 30, "elevation": 300,
    "coldest_min": -3.0, "annual_temp": 13.0,
    "observed": [{"variety": "V", "obs": 5000.0}],
}
_HIT = {"ranked_varieties": [{"variety": "V", "mean_yield_kg_ha": 5100.0}]}
_MISS = {"ranked_varieties": []}
_V1 = {"rainfall": 500, "et0": 1000, "frost": 30, "elevation": 300}
_V2 = {"rainfall": 500, "et0": 1000, "coldest_min": -3.0, "annual_temp": 13.0}


def _bt(results, fold=_FOLD):
    dao = MagicMock()
    dao.extrapolate_varieties = AsyncMock(side_effect=results)
    bt = Backtester(dao)
    bt._folds = AsyncMock(return_value=[fold])
    return bt, dao


async def test_koppen_has_no_target_features():
    bt, dao = _bt([_HIT])
    await bt.run(strategy="koppen")
    kw = dao.extrapolate_varieties.await_args.kwargs
    assert "target_features" not in kw
    assert kw.get("vector_version", "v1") == "v1"


async def test_v1_passes_v1_vector():
    bt, dao = _bt([_HIT])
    await bt.run(strategy="v1")
    kw = dao.extrapolate_varieties.await_args.kwargs
    assert kw["target_features"] == _V1
    assert kw.get("vector_version", "v1") == "v1"


async def test_v2_passes_version_and_features():
    bt, dao = _bt([_HIT])
    await bt.run(strategy="v2")
    kw = dao.extrapolate_varieties.await_args.kwargs
    assert kw["vector_version"] == "v2" and kw["target_features"] == _V2


async def test_default_is_hybrid_koppen_hit_one_call():
    bt, dao = _bt([_HIT])
    await bt.run()
    assert dao.extrapolate_varieties.await_count == 1
    assert "target_features" not in dao.extrapolate_varieties.await_args.kwargs


async def test_hybrid_uncovered_falls_back_to_v2():
    bt, dao = _bt([_MISS, _HIT])
    report = await bt.run(strategy="hybrid")
    calls = dao.extrapolate_varieties.await_args_list
    assert len(calls) == 2
    assert "target_features" not in calls[0].kwargs
    assert calls[1].kwargs["vector_version"] == "v2"
    assert calls[1].kwargs["target_features"] == _V2
    assert report["overall"]["coverage"] == 1.0


async def test_hybrid_uncovered_without_chelsa_no_second_call():
    bt, dao = _bt([_MISS], fold={**_FOLD, "coldest_min": None})
    report = await bt.run(strategy="hybrid")
    assert dao.extrapolate_varieties.await_count == 1
    assert report["overall"]["coverage"] == 0.0


async def test_unknown_strategy_raises():
    bt, _ = _bt([_HIT])
    with pytest.raises(ValueError):
        await bt.run(strategy="v3")


async def test_fold_query_returns_chelsa_cold_and_temp():
    session = MagicMock()
    result = MagicMock()

    async def _iter():
        return
        yield

    result.__aiter__ = lambda self: _iter()
    session.run = AsyncMock(return_value=result)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    dao = MagicMock()
    dao._driver.session = MagicMock(return_value=session)
    await Backtester(dao)._folds()
    q = session.run.await_args.args[0]
    assert "t.coldestMonthMinCChelsa AS coldest_min" in q
    assert "t.annualTempCChelsa AS annual_temp" in q


async def test_hybrid_et0_zero_no_second_call():
    bt, dao = _bt([_MISS], fold={**_FOLD, "et0": 0})
    await bt.run(strategy="hybrid")
    assert dao.extrapolate_varieties.await_count == 1


@pytest.mark.parametrize("strategy", ["koppen", "v1", "v2", "hybrid"])
async def test_report_records_similarity(strategy):
    bt, _ = _bt([_HIT, _HIT])
    report = await bt.run(strategy=strategy)
    assert report["similarity"] == strategy
