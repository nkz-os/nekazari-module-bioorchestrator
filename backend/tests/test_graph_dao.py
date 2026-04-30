"""Test GraphDAO with mock Neo4j driver."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from neo4j import AsyncDriver

from app.graph.dao import GraphDAO


@pytest.fixture
def dao(mock_driver: AsyncDriver) -> GraphDAO:
    return GraphDAO(mock_driver)


class TestHealthCheck:

    def test_connected(self, dao, mock_driver):
        session = MagicMock()
        session.run = AsyncMock()
        session.single = AsyncMock(return_value={"alive": 1})
        mock_driver.session.return_value.__aenter__ = AsyncMock(return_value=session)

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(dao.health_check())
        assert result["neo4j"] == "connected"
        assert result["alive"] == 1

    def test_error(self, dao, mock_driver):
        mock_driver.session.side_effect = Exception("connection refused")

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(dao.health_check())
        assert result["neo4j"] == "error"
        assert "connection refused" in result["detail"]


class TestPhenologyParams:

    def test_returns_params(self, dao, mock_driver):
        row = MagicMock()
        row.__getitem__ = lambda self, k: {
            "d1": 2.5,
            "d2": 7.0,
            "kc": 0.9,
            "mds_ref": 160.0,
            "species_name": "Olea europaea",
            "stage": "vegetative",
        }[k]

        session = MagicMock()
        session.run = AsyncMock()
        session.single = AsyncMock(return_value=row)
        mock_driver.session.return_value.__aenter__ = AsyncMock(return_value=session)

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            dao.get_phenology_params("olive", "vegetative")
        )
        assert result is not None
        assert result["d1"] == 2.5
        assert result["kc"] == 0.9
        assert result["species"] == "Olea europaea"

    def test_not_found_returns_none(self, dao, mock_driver):
        session = MagicMock()
        session.run = AsyncMock()
        session.single = AsyncMock(return_value=None)
        mock_driver.session.return_value.__aenter__ = AsyncMock(return_value=session)

        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            dao.get_phenology_params("unknown_species", "flowering")
        )
        assert result is None
