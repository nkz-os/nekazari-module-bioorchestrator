"""BalanceDao Cypher and shaping, against a fake async driver."""
from unittest.mock import AsyncMock, MagicMock

from app.graph.balance_dao import BalanceDao


def _dao(single=None, data=None):
    result = MagicMock()
    result.single = AsyncMock(return_value=single)
    result.data = AsyncMock(return_value=data or [])
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    calls = []

    async def run(query, **params):
        calls.append((query, params))
        return result

    session.run = run
    driver = MagicMock()
    driver.session.return_value = session
    return BalanceDao(driver), calls


async def test_crop_coefficients_by_eppo():
    dao, calls = _dao(single={"props": {"eppo": "TRZAX", "nProductPct": 2.0}})
    assert await dao.get_crop_coefficients("TRZAX") == {"eppo": "TRZAX", "nProductPct": 2.0}
    query, params = calls[0]
    assert "CropBalanceCoefficients" in query and params == {"eppo": "TRZAX"}


async def test_crop_coefficients_missing_is_none():
    dao, _ = _dao(single=None)
    assert await dao.get_crop_coefficients("XXXXX") is None


async def test_regional_yields_ordered_rows():
    rows = [{"props": {"year": 2023}}, {"props": {"year": 2024}}]
    dao, calls = _dao(data=rows)
    assert await dao.get_regional_yields("TRZAX", "ES22") == [{"year": 2023}, {"year": 2024}]
    query, params = calls[0]
    assert "RegionalYieldStat" in query and "ORDER BY n.year" in query
    assert params == {"eppo": "TRZAX", "nuts": "ES22"}


async def test_list_yield_regions_filters_country_and_nulls():
    rows = [{"nuts": "ES21", "eppos": ["HORVX", "TRZAX"], "latest_year": 2024},
            {"nuts": "ES22", "eppos": ["TRZAX"], "latest_year": 2023}]
    dao, calls = _dao(data=rows)
    assert await dao.list_yield_regions("ES") == rows
    query, params = calls[0]
    assert "RegionalYieldStat" in query
    assert "n.nutsLevel = 2" in query
    assert "yieldTHa IS NOT NULL" in query and "nuts STARTS WITH $country" in query
    assert "max(n.year) AS latest_year" in query
    assert params == {"country": "ES"}


async def test_batch_query_takes_region_and_country_in_one_read():
    dao, calls = _dao(data=[{"props": {"year": 2023}}])
    assert await dao.get_regional_yields_batch(["TRZAX", "HORVX"], ["ES24", "ES"]) == [{"year": 2023}]
    assert len(calls) == 1
    assert "n.eppo IN $eppos" in calls[0][0] and "n.nuts IN $nuts" in calls[0][0]
    assert calls[0][1] == {"eppos": ["TRZAX", "HORVX"], "nuts": ["ES24", "ES"]}
