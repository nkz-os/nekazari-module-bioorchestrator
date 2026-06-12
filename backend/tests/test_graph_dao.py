"""Test GraphDAO with mock Neo4j driver."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from neo4j import AsyncDriver

from app.graph.dao import GraphDAO


# ── Helper: build a mock Neo4j result that supports single() ───────────

class _MockResult:
    """Minimal mock result with single() returning a dict-like record."""
    def __init__(self, record: dict | None = None):
        self._record = record

    async def single(self) -> MagicMock | None:
        if self._record is None:
            return None
        # Return a MagicMock that supports __getitem__ (like a real Record)
        m = MagicMock()
        m.__getitem__ = lambda self, k: self._record[k]
        m._record = self._record
        m.get = lambda k, default=None: self._record.get(k, default)
        return m


def _make_driver(result_record: dict | None = None):
    """Build a mock AsyncDriver whose session.run() returns _MockResult."""
    driver = MagicMock(spec=AsyncDriver)
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    # session.run() is async — use AsyncMock so it can be awaited
    session.run = AsyncMock(return_value=_MockResult(result_record))
    driver.session = MagicMock(return_value=session)
    return driver


# ── Fixtures ───────────────────────────────────────────────────────────

@pytest.fixture
def mock_driver():
    return _make_driver()


@pytest.fixture
def dao(mock_driver):
    return GraphDAO(mock_driver)


# ── Tests ──────────────────────────────────────────────────────────────

class TestHealthCheck:

    def test_connected(self, dao):
        """health_check() returns 'connected' when Neo4j responds."""
        # Replace driver with one that returns a result with 'alive'=1
        dao._driver = _make_driver({"alive": 1})
        result = asyncio.get_event_loop().run_until_complete(dao.health_check())
        assert result["neo4j"] == "connected"
        assert result["alive"] == 1

    def test_error(self, dao):
        """health_check() returns 'error' when Neo4j raises."""
        dao._driver.session.return_value.__aenter__.return_value.run.side_effect = Exception("connection refused")
        result = asyncio.get_event_loop().run_until_complete(dao.health_check())
        assert result["neo4j"] == "error"
        assert "connection refused" in result["detail"]


class TestPhenologyParams:

    def test_returns_params(self, dao):
        """get_phenology_params() returns parsed params when data exists."""
        record = {
            "d1": 2.5, "d2": 7.0, "kc": 0.9, "ky": 1.1,
            "mds_ref": 160.0, "species": "Olea europaea",
            "stage": "vegetative", "stage_description": "Vegetative growth",
            "stage_base_temp": 10.0, "stage_gdd_min": 100.0, "stage_gdd_max": 500.0,
            "kc_ci_low": None, "kc_ci_high": None,
            "d1_ci_low": None, "d1_ci_high": None,
            "d2_ci_low": None, "d2_ci_high": None,
            "mds_ref_ci_low": None, "mds_ref_ci_high": None,
            "cultivar": None, "management": None, "climate_zone": None,
            "is_default": True, "match_level": "generic",
            "source_doi": "10.1234/example", "source_short": "Test",
            "source_author": "Tester", "source_year": 2024,
            "source_institution": None, "source_method": None, "source_conditions": None,
            "agrovoc_uri": None, "scientific_name": "Olea europaea",
            "alternatives": [],
        }
        dao._driver = _make_driver(record)
        result = asyncio.get_event_loop().run_until_complete(
            dao.get_phenology_params("olive", "vegetative")
        )
        assert result is not None
        assert result["d1"] == 2.5
        assert result["kc"] == 0.9
        assert result["species"] == "Olea europaea"

    def test_not_found_returns_none(self, dao):
        """get_phenology_params() returns None when no data."""
        dao._driver = _make_driver(None)  # None record → single() returns None
        result = asyncio.get_event_loop().run_until_complete(
            dao.get_phenology_params("unknown_species", "flowering")
        )
        assert result is None
