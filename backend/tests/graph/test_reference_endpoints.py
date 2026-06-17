"""Test reference data endpoints (climate-classes, soil-types)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from neo4j import AsyncDriver

from app.graph.dao import GraphDAO


class _MockRecord:
    """Minimal mock record supporting __getitem__."""
    def __init__(self, data: dict):
        self._data = data
    def __getitem__(self, key):
        return self._data[key]


def _make_driver(data_result: list[dict] | None = None) -> AsyncDriver:
    """Build a mock AsyncDriver for list-returning queries."""
    driver = MagicMock(spec=AsyncDriver)
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    records = [_MockRecord(d) for d in (data_result or [])]

    class _AsyncIter:
        def __init__(self, items):
            self._items = items
        def __aiter__(self):
            return self
        async def __anext__(self):
            if not self._items:
                raise StopAsyncIteration
            return self._items.pop(0)

    mock_result = _AsyncIter(records)
    session.run = AsyncMock(return_value=mock_result)
    driver.session = MagicMock(return_value=session)
    return driver


class TestClimateClasses:

    def test_returns_sorted_classes(self):
        """get_climate_classes() returns distinct sorted climate classes."""
        driver = _make_driver([
            {"climate_class": "BSk"},
            {"climate_class": "Cfb"},
            {"climate_class": "Csa"},
        ])
        dao = GraphDAO(driver)
        result = asyncio.run(dao.get_climate_classes())
        assert result == ["BSk", "Cfb", "Csa"]

    def test_empty_when_no_trial_sites(self):
        """get_climate_classes() returns empty list when no data."""
        driver = _make_driver([])
        dao = GraphDAO(driver)
        result = asyncio.run(dao.get_climate_classes())
        assert result == []


class TestSoilTypes:

    def test_returns_sorted_types(self):
        """get_soil_types() returns distinct sorted soil types."""
        driver = _make_driver([
            {"soil_type": "Calcisol"},
            {"soil_type": "Cambisol"},
            {"soil_type": "Fluvisol"},
        ])
        dao = GraphDAO(driver)
        result = asyncio.run(dao.get_soil_types())
        assert result == ["Calcisol", "Cambisol", "Fluvisol"]

    def test_empty_when_no_data(self):
        driver = _make_driver([])
        dao = GraphDAO(driver)
        result = asyncio.run(dao.get_soil_types())
        assert result == []
