from unittest.mock import AsyncMock, MagicMock

from app.graph.dao import GraphDAO


def _dao(rows):
    session = MagicMock()
    result = MagicMock()

    async def _iter():
        for r in rows:
            yield r

    result.__aiter__ = lambda self: _iter()
    result.single = AsyncMock(return_value=None)
    session.run = AsyncMock(return_value=result)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    driver = MagicMock()
    driver.session = MagicMock(return_value=session)
    return GraphDAO(driver), session


async def test_site_query_coalesces_chelsa_over_legacy():
    dao, session = _dao([])
    await dao.get_similar_sites(climate_class="Csa")
    query = session.run.await_args.args[0]
    assert "coalesce(ts.climateClassChelsa, ts.climateClass) AS climate_class" in query
    assert "coalesce(ts.annualRainfallMmChelsa, ts.annualRainfallMm) AS annual_rainfall_mm" in query
    assert "coalesce(ts.annualET0MmChelsa, ts.annualET0Mm) AS annual_et0_mm" in query
    assert "ts.frostDaysPerYear AS frost_days" in query


async def test_reference_query_coalesces_chelsa_over_legacy():
    dao, session = _dao([])
    await dao.get_similar_sites(reference_site="Olite")
    query = session.run.await_args_list[0].args[0]
    assert "coalesce(ts.climateClassChelsa, ts.climateClass) AS climate" in query
    assert "coalesce(ts.annualRainfallMmChelsa, ts.annualRainfallMm) AS rainfall" in query
    assert "coalesce(ts.annualET0MmChelsa, ts.annualET0Mm) AS et0" in query


async def test_extrapolate_reference_query_coalesces():
    dao, session = _dao([])
    await dao.extrapolate_varieties("TRZAX", reference_site="Olite")
    query = session.run.await_args_list[0].args[0]
    assert "coalesce(ts.climateClassChelsa, ts.climateClass) AS climate" in query
    assert "coalesce(ts.annualRainfallMmChelsa, ts.annualRainfallMm) AS rainfall" in query


async def test_legacy_only_site_still_matches_by_alias():
    rows = [{"name": "A", "climate_class": "Csa", "soil_type": None, "annual_rainfall_mm": None,
             "annual_et0_mm": None, "frost_days": None, "elevation_m": None}]
    dao, _ = _dao(rows)
    out = await dao.get_similar_sites(climate_class="Csa")
    assert [r["name"] for r in out] == ["A"]
