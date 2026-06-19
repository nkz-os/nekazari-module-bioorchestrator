"""Test GraphDAO.get_phenology_stages — full ordered phenology stage table."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from neo4j import AsyncDriver

from app.graph.dao import GraphDAO


class _FakeDataResult:
    """Minimal mock Neo4j result exposing only async .data() (real pattern,
    e.g. GraphDAO.get_crop_catalog / get_rotation_constraints)."""

    def __init__(self, rows: list[dict]):
        self._rows = rows

    async def data(self) -> list[dict]:
        return self._rows


def _make_driver(rows: list[dict]):
    """Build a mock AsyncDriver whose session.run() returns _FakeDataResult."""
    driver = MagicMock(spec=AsyncDriver)
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.run = AsyncMock(return_value=_FakeDataResult(rows))
    driver.session = MagicMock(return_value=session)
    return driver


@pytest.fixture
def dao():
    return GraphDAO(_make_driver([]))


class TestPhenologyStages:

    def test_get_phenology_stages_ordered(self, dao):
        """Stages come back sorted ascending by gddMin, regardless of graph order."""
        rows = [
            {"stage": "flowering", "gddMin": 800.0, "gddMax": 1100.0, "baseTemp": 10.0},
            {"stage": "emergence", "gddMin": 0.0, "gddMax": 90.0, "baseTemp": 10.0},
        ]
        dao._driver = _make_driver(rows)

        out = asyncio.run(dao.get_phenology_stages("Zea mays"))

        assert [s["stage"] for s in out] == ["emergence", "flowering"]
        assert out[0]["gddMin"] == 0.0
        assert out[1]["gddMin"] == 800.0

    def test_get_phenology_stages_empty_when_no_nodes(self, dao):
        """No PhenologyStage nodes for the species -> empty list (caller falls back)."""
        dao._driver = _make_driver([])

        out = asyncio.run(dao.get_phenology_stages("unknown_species"))

        assert out == []

    def test_get_phenology_stages_skips_incomplete_rows(self, dao):
        """Rows missing gddMin/gddMax are dropped (can't be ordered/used)."""
        rows = [
            {"stage": "dormant", "gddMin": None, "gddMax": None, "baseTemp": None},
            {"stage": "emergence", "gddMin": 0.0, "gddMax": 90.0, "baseTemp": 10.0},
        ]
        dao._driver = _make_driver(rows)

        out = asyncio.run(dao.get_phenology_stages("Zea mays"))

        assert [s["stage"] for s in out] == ["emergence"]
