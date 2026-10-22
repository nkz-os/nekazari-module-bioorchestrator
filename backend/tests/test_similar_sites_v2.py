"""get_similar_sites vector_version='v2' (mock session rows, no Neo4j)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.graph.dao import GraphDAO


def _dao(rows, ref=None):
    session = MagicMock()
    result = MagicMock()

    async def _iter():
        for r in rows:
            yield dict(r)

    result.__aiter__ = lambda self: _iter()
    result.single = AsyncMock(return_value=ref)
    session.run = AsyncMock(return_value=result)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    driver = MagicMock()
    driver.session = MagicMock(return_value=session)
    return GraphDAO(driver), session


def _row(name, rain, et0, cold, temp, *, koppen="Csa", frost=None, elev=None):
    return {
        "name": name, "climate_class": koppen, "soil_type": None,
        "annual_rainfall_mm": rain, "annual_et0_mm": et0,
        "frost_days": frost, "elevation_m": elev,
        "coldest_min": cold, "annual_temp": temp,
    }


_TARGET = {"rainfall": 500, "et0": 1000, "coldest_min": -3.0, "annual_temp": 13.0}
_ROWS = [
    _row("FAR", 1100, 900, 4.0, 17.0),
    _row("NEAR", 520, 1000, -2.0, 13.5),
    _row("NOCHELSA", None, None, None, None),
]


async def test_site_query_returns_chelsa_props():
    dao, session = _dao([])
    await dao.get_similar_sites(climate_class="Csa")
    q = session.run.await_args.args[0]
    assert "ts.coldestMonthMinCChelsa AS coldest_min" in q
    assert "ts.annualTempCChelsa AS annual_temp" in q


async def test_reference_query_returns_chelsa_props():
    dao, session = _dao([])
    await dao.get_similar_sites(reference_site="X", vector_version="v2")
    q = session.run.await_args_list[0].args[0]
    assert "ts.coldestMonthMinCChelsa AS coldest_min" in q
    assert "ts.annualTempCChelsa AS annual_temp" in q


async def test_v2_ranks_by_distance_with_koppen_fallback():
    dao, _ = _dao(_ROWS)
    out = await dao.get_similar_sites(
        climate_class="Csa", target_features=_TARGET, vector_version="v2", limit=10,
    )
    assert [s["name"] for s in out] == ["NEAR", "FAR", "NOCHELSA"]
    assert out[0]["distance"] < out[1]["distance"] < 2.0
    assert out[2]["distance"] == 2.0  # KOPPEN_FALLBACK_DISTANCE


async def test_v2_site_without_chelsa_and_other_koppen_is_dropped():
    rows = [_row("NEAR", 520, 1000, -2.0, 13.5), _row("OTHER", None, None, None, None, koppen="Cfb")]
    dao, _ = _dao(rows)
    out = await dao.get_similar_sites(
        climate_class="Csa", target_features=_TARGET, vector_version="v2",
    )
    assert [s["name"] for s in out] == ["NEAR"]


async def test_v2_ignores_v1_only_props():
    # Site has frost/elevation (v1 data) but no CHELSA cold/temp: v2 must not use them.
    rows = [_row("V1ONLY", 520, 1000, None, None, frost=30, elev=300)]
    dao, _ = _dao(rows)
    out = await dao.get_similar_sites(
        climate_class="Csa", target_features=_TARGET, vector_version="v2",
    )
    assert out[0]["distance"] == 2.0


async def test_v2_reference_site_supplies_target():
    ref = {"climate": "Csa", "soil": None, "rainfall": 500, "et0": 1000, "frost": None,
           "elevation": None, "coldest_min": -3.0, "annual_temp": 13.0}
    dao, _ = _dao(_ROWS, ref=ref)
    out = await dao.get_similar_sites(reference_site="Ref", vector_version="v2")
    assert out[0]["name"] == "NEAR" and out[0]["distance"] is not None


async def test_v1_unchanged_ignores_chelsa_props():
    rows = [_row("A", 520, 1000, -2.0, 13.5, frost=38, elev=320),
            _row("B", 1000, 1000, 4.0, 17.0, frost=5, elev=300)]
    dao, _ = _dao(rows)
    out = await dao.get_similar_sites(
        climate_class="Csa",
        target_features={"rainfall": 500, "et0": 1000, "frost": 40, "elevation": 300},
    )
    assert [s["name"] for s in out] == ["A", "B"]


async def test_unknown_version_raises():
    dao, _ = _dao([])
    with pytest.raises(ValueError):
        await dao.get_similar_sites(climate_class="Csa", vector_version="v3")


async def test_extrapolate_passes_version_through():
    dao, _ = _dao([])
    dao.get_similar_sites = AsyncMock(return_value=[])
    await dao.extrapolate_varieties("TRZAX", climate_class="Csa", vector_version="v2",
                                    target_features=_TARGET)
    kw = dao.get_similar_sites.await_args.kwargs
    assert kw["vector_version"] == "v2"
    assert kw["target_features"] == _TARGET
