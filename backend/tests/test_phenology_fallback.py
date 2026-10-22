"""Test phenology fallback to CropHealthAssessment in Orion-LD."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.graph.dao import GraphDAO


@pytest.fixture
def dao():
    """DAO with a mock driver that returns no results (triggers fallback)."""
    driver = MagicMock()
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)

    mock_record = MagicMock()
    mock_record.__getitem__ = lambda s, k: None
    mock_record.get = lambda k, d=None: None

    first_result = MagicMock()
    first_result.single = AsyncMock(return_value=mock_record)

    session.run = AsyncMock(return_value=first_result)
    driver.session = MagicMock(return_value=session)
    return GraphDAO(driver)


def test_fallback_returns_none_when_orion_empty(dao):
    """When Orion has no CropHealthAssessment, fallback returns None."""
    mock_orion = MagicMock()
    mock_orion.query_entities = AsyncMock(return_value=[])
    mock_orion.close = AsyncMock()

    with patch("nkz_platform_sdk.orion.OrionClient", return_value=mock_orion):
        result = asyncio.run(dao._fallback_phenology_from_orion("olive"))
    assert result is None


def test_fallback_extracts_params(dao):
    """When Orion has assessment data, fallback extracts params correctly."""
    mock_assessment = {
        "id": "urn:ngsi-ld:CropHealthAssessment:olive-42",
        "kc": {"type": "Property", "value": 0.65},
        "ky": {"type": "Property", "value": 0.85},
        "d1": {"type": "Property", "value": 35.0},
        "d2": {"type": "Property", "value": 120.0},
        "mdsRef": {"type": "Property", "value": 85.0},
        "species": {"type": "Property", "value": "olive"},
        "phenologyStage": {"type": "Property", "value": "pit_hardening"},
    }
    mock_orion = MagicMock()
    mock_orion.query_entities = AsyncMock(return_value=[mock_assessment])
    mock_orion.close = AsyncMock()

    with patch("nkz_platform_sdk.orion.OrionClient", return_value=mock_orion):
        result = asyncio.run(dao._fallback_phenology_from_orion("olive"))

    assert result is not None
    assert result["kc"] == 0.65
    assert result["ky"] == 0.85
    assert result["d1"] == 35.0
    assert result["d2"] == 120.0
    assert result["mds_ref"] == 85.0
    assert result["match_level"] == "fallback_orion"
    assert result["is_default"] is True
    assert result["provenance"]["short"] == "CropHealthAssessment (fallback)"


def test_fallback_handles_no_matching_species(dao):
    """When no assessment matches the species, returns latest assessment anyway."""
    mock_assessment = {
        "id": "urn:ngsi-ld:CropHealthAssessment:wheat-1",
        "kc": {"type": "Property", "value": 0.30},
        "species": {"type": "Property", "value": "wheat"},
    }
    mock_orion = MagicMock()
    mock_orion.query_entities = AsyncMock(return_value=[mock_assessment])
    mock_orion.close = AsyncMock()

    with patch("nkz_platform_sdk.orion.OrionClient", return_value=mock_orion):
        result = asyncio.run(dao._fallback_phenology_from_orion("unknown_species"))
    assert result is not None
    assert result["kc"] == 0.30
