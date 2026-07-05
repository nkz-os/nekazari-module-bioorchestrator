"""TDD: get_crop_context must use SDK OrionClient, thread tenant correctly, and
unwrap normalized NGSI-LD (not keyValues) for the CropHealthAssessment block."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.graph.dao import GraphDAO

# ---------------------------------------------------------------------------
# Stub data — all in NORMALIZED NGSI-LD form (not keyValues)
# ---------------------------------------------------------------------------

PARCEL_ID = "urn:ngsi-ld:AgriParcel:test-parcel-001"
CROP_URI = "urn:ngsi-ld:AgriCrop:TRZAW"
TENANT = "test-tenant"

NORMALIZED_PARCEL = {
    "id": PARCEL_ID,
    "type": "AgriParcel",
    "hasAgriCrop": {"type": "Relationship", "object": CROP_URI},
    "hasAgriCropVariety": {"type": "Relationship", "object": "urn:ngsi-ld:AgriCropVariety:WinterWheat"},
    "management": {"type": "Property", "value": "conventional"},
    "cropSeasonStart": {"type": "Property", "value": "2025-10-01"},
    "cropSeasonEnd": {"type": "Property", "value": "2026-07-01"},
}

NORMALIZED_CROP = {
    "id": CROP_URI,
    "type": "AgriCrop",
    "name": {"type": "Property", "value": "Wheat"},
    "scientificName": {"type": "Property", "value": "Triticum aestivum"},
}

NORMALIZED_ASSESSMENT = {
    "id": "urn:ngsi-ld:CropHealthAssessment:001",
    "type": "CropHealthAssessment",
    "hasAgriParcel": {"type": "Relationship", "object": PARCEL_ID},
    "assessedAt": {"type": "Property", "value": "2026-06-01T00:00:00Z"},
    "soilPh": {"type": "Property", "value": 6.8},
    "soilEC": {"type": "Property", "value": 0.42},
    "soilMoisturePct": {"type": "Property", "value": 32.5},
    "soilTemperatureC": {"type": "Property", "value": 18.2},
}


# ---------------------------------------------------------------------------
# Stub OrionClient
# ---------------------------------------------------------------------------

class _FakeOrionClient:
    """Stub that records tenant and returns normalized entities."""

    _instances: list[str] = []

    def __init__(self, tenant_id: str) -> None:
        _FakeOrionClient._instances.append(tenant_id)
        self._tenant = tenant_id

    async def get_entity(self, entity_id: str) -> dict:
        if entity_id == PARCEL_ID:
            return NORMALIZED_PARCEL
        if entity_id == CROP_URI:
            return NORMALIZED_CROP
        # Simulate 404 → raise_for_status raises HTTPStatusError
        request = MagicMock()
        response = MagicMock()
        response.status_code = 404
        raise httpx.HTTPStatusError("Not Found", request=request, response=response)

    async def query_entities(self, *, type: str, q: str | None = None, limit: int = 100) -> list:
        if type == "CropHealthAssessment":
            return [NORMALIZED_ASSESSMENT]
        return []

    async def close(self) -> None:
        pass


class _FakeOrionClient404Parcel:
    """Returns 404 for the parcel entity."""

    _instances: list[str] = []

    def __init__(self, tenant_id: str) -> None:
        _FakeOrionClient404Parcel._instances.append(tenant_id)

    async def get_entity(self, entity_id: str) -> dict:
        request = MagicMock()
        response = MagicMock()
        response.status_code = 404
        raise httpx.HTTPStatusError("Not Found", request=request, response=response)

    async def query_entities(self, **kwargs) -> list:
        return []

    async def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset():
    _FakeOrionClient._instances = []
    _FakeOrionClient404Parcel._instances = []
    yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_get_crop_context_uses_sdk_and_correct_tenant(mock_driver):
    """OrionClient must be constructed with the passed tenant_id."""
    dao = GraphDAO(mock_driver)

    with patch("app.graph.dao.OrionClient", _FakeOrionClient), \
         patch.object(GraphDAO, "get_phenology_params", AsyncMock(return_value=None)), \
         patch.object(GraphDAO, "get_heat_tolerance", AsyncMock(return_value=None)), \
         patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)), \
         patch("app.services.soil_client.get_parcel_soil_properties", AsyncMock(return_value={"data_available": False})):
        result = asyncio.run(dao.get_crop_context(PARCEL_ID, tenant_id=TENANT))

    assert _FakeOrionClient._instances == [TENANT], (
        f"OrionClient constructed with wrong tenant: {_FakeOrionClient._instances}"
    )
    assert "error" not in result


def test_get_crop_context_reads_crop_name_from_normalized_entity(mock_driver):
    """crop.name must come from unwrapped normalized NGSI-LD Property, not keyValues."""
    dao = GraphDAO(mock_driver)

    with patch("app.graph.dao.OrionClient", _FakeOrionClient), \
         patch.object(GraphDAO, "get_phenology_params", AsyncMock(return_value=None)), \
         patch.object(GraphDAO, "get_heat_tolerance", AsyncMock(return_value=None)), \
         patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)), \
         patch("app.services.soil_client.get_parcel_soil_properties", AsyncMock(return_value={"data_available": False})):
        result = asyncio.run(dao.get_crop_context(PARCEL_ID, tenant_id=TENANT))

    assert result["crop"]["name"] == "Wheat", f"Expected 'Wheat', got: {result['crop']}"
    assert result["crop"]["scientific_name"] == "Triticum aestivum"
    assert result["crop"]["eppo"] == "TRZAW"


def test_get_crop_context_unwraps_soil_sensors_from_normalized(mock_driver):
    """soil_sensors block must unwrap normalized Property values, not read flat keyValues."""
    dao = GraphDAO(mock_driver)

    with patch("app.graph.dao.OrionClient", _FakeOrionClient), \
         patch.object(GraphDAO, "get_phenology_params", AsyncMock(return_value=None)), \
         patch.object(GraphDAO, "get_heat_tolerance", AsyncMock(return_value=None)), \
         patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)), \
         patch("app.services.soil_client.get_parcel_soil_properties", AsyncMock(return_value={"data_available": False})):
        result = asyncio.run(dao.get_crop_context(PARCEL_ID, tenant_id=TENANT))

    ss = result["soil_sensors"]
    assert ss["available"] is True, f"soil_sensors.available must be True: {ss}"
    assert ss["ph"] == 6.8, f"Expected ph=6.8, got: {ss}"
    assert ss["ec_ds_m"] == 0.42
    assert ss["moisture_pct"] == 32.5
    assert ss["temperature_c"] == 18.2
    assert ss["last_reading"] == "2026-06-01T00:00:00Z"


def test_get_crop_context_404_parcel_returns_error(mock_driver):
    """404 on the parcel entity must return the canonical error dict (no HTTPException)."""
    dao = GraphDAO(mock_driver)

    with patch("app.graph.dao.OrionClient", _FakeOrionClient404Parcel):
        result = asyncio.run(dao.get_crop_context(PARCEL_ID, tenant_id=TENANT))

    assert "error" in result
    assert "Parcel not found" in result["error"], f"Unexpected error: {result['error']}"


def test_get_crop_context_no_assessment_available(mock_driver):
    """When query_entities returns [] for CropHealthAssessment, soil_sensors.available=False."""

    class _NoAssessmentClient(_FakeOrionClient):
        async def query_entities(self, *, type: str, q: str | None = None, limit: int = 100) -> list:
            return []

    dao = GraphDAO(mock_driver)

    with patch("app.graph.dao.OrionClient", _NoAssessmentClient), \
         patch.object(GraphDAO, "get_phenology_params", AsyncMock(return_value=None)), \
         patch.object(GraphDAO, "get_heat_tolerance", AsyncMock(return_value=None)), \
         patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)), \
         patch("app.services.soil_client.get_parcel_soil_properties", AsyncMock(return_value={"data_available": False})):
        result = asyncio.run(dao.get_crop_context(PARCEL_ID, tenant_id=TENANT))

    assert result["soil_sensors"] == {"available": False}


def test_crop_context_soil_suitability_is_graded_verdict(monkeypatch):
    # assess (not compute) → the `soil.suitability` carries a graded `verdict`.
    from app.services import soil_client
    assert not hasattr(soil_client, "compute_soil_suitability"), \
        "compute_soil_suitability must be removed (0 callers after swap)"
    # unavailable parcel soil → honest 'unknown' (guard-drop is safe)
    verdict = soil_client.assess_soil_suitability(
        {"ph_min": 6.0, "ph_max": 7.5, "textures": ["loam"]},
        {"data_available": False, "source": "unavailable"},
    )
    assert verdict["verdict"] == "unknown"
