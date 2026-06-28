"""Tests for GET /agriculture/parcel-environment endpoint."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from neo4j import AsyncDriver

from app.graph.dao import GraphDAO


class _MockResult:
    """Minimal mock result with async iterator and single() support."""

    def __init__(self, records: list[dict] | None = None):
        self._records = records or []

    def __aiter__(self):
        self._idx = 0
        return self

    async def __anext__(self):
        if self._idx >= len(self._records):
            raise StopAsyncIteration
        rec = self._records[self._idx]
        self._idx += 1
        return self._make_record(rec)

    def _make_record(self, rec: dict):
        m = MagicMock()
        m.__getitem__ = lambda s, k: rec.get(k)
        m.get = rec.get
        m.keys = lambda: rec.keys()
        return m

    async def single(self):
        if not self._records:
            return None
        return self._make_record(self._records[0])

    async def data(self):
        return [self._make_record(r) for r in self._records]


def _make_driver(records: list[dict] | None = None):
    """Build a mock AsyncDriver whose session.run() yields _MockResult records."""
    driver = MagicMock(spec=AsyncDriver)
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.run = AsyncMock(return_value=_MockResult(records))
    driver.session.return_value = session
    return driver


class TestParcelEnvironment:
    """Verify the DAO method resolves parcel profile without assigned crop."""

    @pytest.mark.asyncio
    async def test_returns_profile_when_no_crop_assigned(self):
        """Core spec requirement: must NOT require hasAgriCrop."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:test-42",
                "type": "AgriParcel",
                "location": {
                    "type": "GeoProperty",
                    "value": {
                        "type": "Point",
                        "coordinates": [-1.8, 42.1],
                    },
                },
                "area": {"type": "Property", "value": 12.4},
            }
            mock_orion.close = AsyncMock()

            mock_soil.return_value = {
                "ph": 7.2,
                "texture": "loam",
                "awc_mm": 120,
                "data_available": True,
                "source": "soilgrids",
            }

            driver = _make_driver([])  # no trial sites nearby
            dao = GraphDAO(driver)
            result = await dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:test-42")

            assert result["parcel_id"] == "urn:ngsi-ld:AgriParcel:test-42"
            assert result["area_ha"] == 12.4
            assert result["centroid"] == {"lat": 42.1, "lon": -1.8}
            assert result["campaign"]["assigned"] is False
            assert result["soil"]["data_available"] is True
            assert result["soil"]["texture"] == "loam"
            assert result["inputs_used"]["soil"] == "soil_module"
            assert isinstance(result["irrigation"], dict)
            assert result["irrigation"]["overridable"] is True

    @pytest.mark.asyncio
    async def test_handles_missing_parcel(self):
        """Should return error dict when parcel not found in Orion."""
        import httpx
        from app.graph.dao import GraphDAO

        with patch("app.graph.dao.OrionClient") as mock_orion_cls:
            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.side_effect = httpx.HTTPStatusError(
                "404",
                request=MagicMock(),
                response=MagicMock(status_code=404),
            )
            mock_orion.close = AsyncMock()

            driver = _make_driver()
            dao = GraphDAO(driver)
            result = await dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:missing")

            assert "error" in result
            assert "not found" in result["error"].lower() or "missing" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_irrigation_from_system_type(self):
        """irrigationSystemType should map to secano/regadío."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:irr-1",
                "type": "AgriParcel",
                "location": {
                    "type": "GeoProperty",
                    "value": {"type": "Point", "coordinates": [-1.8, 42.1]},
                },
                "irrigationSystemType": {"type": "Property", "value": "drip_irrigation"},
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": False, "source": "unavailable"}

            driver = _make_driver([])
            dao = GraphDAO(driver)
            result = await dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:irr-1")

            assert result["irrigation"]["inferred"] == "regadío"
            assert result["irrigation"]["source"] == "irrigationSystemType"

    @pytest.mark.asyncio
    async def test_climate_from_nearest_trial_site(self):
        """Should resolve Köppen class from nearest TrialSite."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:clim-1",
                "type": "AgriParcel",
                "location": {
                    "type": "GeoProperty",
                    "value": {"type": "Point", "coordinates": [-1.8, 42.1]},
                },
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": False, "source": "unavailable"}

            # Nearby site within 50km: return Csa
            driver = _make_driver([
                {"cc": "Csa", "tlat": 42.2, "tlon": -1.9},
            ])
            dao = GraphDAO(driver)
            result = await dao.get_parcel_environment("urn:ngsi-ld:AgriParcel:clim-1")

            assert result["climate_class"] == "Csa"
            assert result["inputs_used"]["climate"] == "trial_proxy"
