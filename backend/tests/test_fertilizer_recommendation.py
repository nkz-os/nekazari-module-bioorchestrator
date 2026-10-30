"""recommend_fertilizer dimensional fix: window demand + unknown_soil.

Contract: internal-docs-local/specs/2026-10-30-npk-and-penetrometer-contract.md §B.3
"""
from unittest.mock import AsyncMock, MagicMock

from app.graph.dao import GraphDAO


def _dao(data):
    result = MagicMock()
    result.data = AsyncMock(return_value=data)
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    async def run(query, **params):
        return result

    session.run = run
    driver = MagicMock()
    driver.session.return_value = session
    return GraphDAO(driver)


async def test_unknown_soil_never_declares_deficit():
    dao = _dao([
        {"element": "nitrogen", "uptake": 2.0},
        {"element": "phosphorus", "uptake": 0.8},
    ])
    out = await dao.recommend_fertilizer("TRZAX", "vegetative", None, None, None)
    assert out is not None
    by_element = {r["element"]: r for r in out["recommendations"]}
    for element in ("nitrogen", "phosphorus"):
        rec = by_element[element]
        assert rec["status"] == "unknown_soil"
        assert rec["soil_level"] is None
        assert "lab analysis required" in rec["action"]


async def test_window_demand_thresholds():
    # uptake 2.0 kg/ha/day -> window_demand 60 kg/ha.
    dao = _dao([{"element": "nitrogen", "uptake": 2.0}])
    out = await dao.recommend_fertilizer("TRZAX", "vegetative", soil_n=20.0)
    rec = out["recommendations"][0]
    assert rec["status"] == "deficient"  # 20 < 30

    out = await dao.recommend_fertilizer("TRZAX", "vegetative", soil_n=40.0)
    assert out["recommendations"][0]["status"] == "adequate"  # 30 <= 40 < 60

    out = await dao.recommend_fertilizer("TRZAX", "vegetative", soil_n=90.0)
    assert out["recommendations"][0]["status"] == "sufficient"  # 90 >= 60


async def test_mixed_present_and_unknown_levels():
    dao = _dao([
        {"element": "nitrogen", "uptake": 1.0},
        {"element": "potassium", "uptake": 3.0},
    ])
    out = await dao.recommend_fertilizer("TRZAX", "vegetative", soil_n=10.0, soil_k=None)
    by_element = {r["element"]: r for r in out["recommendations"]}
    assert by_element["nitrogen"]["soil_level"] == 10.0
    assert by_element["potassium"]["status"] == "unknown_soil"


async def test_no_records_returns_none():
    dao = _dao([])
    assert await dao.recommend_fertilizer("TRZAX", "vegetative", 0.0, 0.0, 0.0) is None
