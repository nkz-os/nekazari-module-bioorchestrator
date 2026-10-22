from unittest.mock import AsyncMock, MagicMock

from app.eval.backtest import Backtester


async def test_fold_query_coalesces_chelsa_over_legacy():
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
    assert "coalesce(t.climateClassChelsa, t.climateClass) IS NOT NULL" in q
    assert "coalesce(t.climateClassChelsa, t.climateClass) AS climate" in q
    assert "coalesce(t.annualRainfallMmChelsa, t.annualRainfallMm) AS rainfall" in q
    assert "coalesce(t.annualET0MmChelsa, t.annualET0Mm) AS et0" in q
