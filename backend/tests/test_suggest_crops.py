"""Tests for GET /agriculture/suggest-crops endpoint and DAO."""

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
    driver = MagicMock(spec=AsyncDriver)
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.run = AsyncMock(return_value=_MockResult(records))
    driver.session.return_value = session
    return driver


class TestSuggestCrops:
    """Verify suggest_crops_for_parcel orchestrates existing DAO methods."""

    @pytest.mark.asyncio
    async def test_ranks_winter_cereals_for_parcel(self):
        """Full pipeline: parcel → environment → crops (winter) → rank."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            # Mock Orion: parcel with centroid, no crop
            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:test-42",
                "type": "AgriParcel",
                "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [-1.8, 42.1]}},
                "area": {"type": "Property", "value": 12.4},
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"ph": 7.2, "texture": "loam", "data_available": True, "source": "soilgrids"}

            # AVAILABLE_CROPS: trial site nearby → Csa
            # get_available_crops: list of crops
            # soil suitability: returns none → no warnings
            # heat tolerance: returns none → no risk
            # extrapolate: returns one variety

            driver = _make_driver()
            dao = GraphDAO(driver)

            # Mock dao methods with simpler return values
            with patch.object(dao, "get_parcel_environment", new_callable=AsyncMock) as mock_env, \
                 patch.object(dao, "get_available_crops", new_callable=AsyncMock) as mock_crops, \
                 patch.object(dao, "extrapolate_varieties", new_callable=AsyncMock) as mock_ext, \
                 patch.object(dao, "get_soil_suitability", new_callable=AsyncMock) as mock_soil_req, \
                 patch.object(dao, "get_heat_tolerance", new_callable=AsyncMock) as mock_heat:

                mock_env.return_value = {
                    "parcel_id": "urn:ngsi-ld:AgriParcel:test-42",
                    "area_ha": 12.4,
                    "climate_class": "Csa",
                    "soil": {"texture": "loam", "wrb_type": "Calcisol", "ph": 7.2, "data_available": True},
                    "irrigation": {"inferred": "secano", "source": "unknown", "overridable": True},
                    "campaign": {"assigned": False, "crop_eppo": None},
                    "inputs_used": {"soil": "soil_module", "climate": "trial_proxy"},
                }
                mock_crops.return_value = [
                    {"eppo_code": "TRZAX", "scientific_name": "Triticum aestivum", "trial_count": 15},
                    {"eppo_code": "HORVX", "scientific_name": "Hordeum vulgare", "trial_count": 10},
                ]
                mock_ext.return_value = {
                    "ranked_varieties": [{"variety": "LG_AURUS", "mean_yield_kg_ha": 6200, "confidence": "high",
                                           "trial_count": 12, "confidence_interval": [5100, 7300]}],
                    "similar_sites": ["Cadreita", "Olite"],
                    "data_quality": {"total_trials_analyzed": 12, "unique_varieties": 5},
                }
                mock_soil_req.return_value = {"ph_min": 6.0, "ph_max": 8.0, "textures": ["loam"]}
                mock_heat.return_value = {"frost_damage_c": -12}

                result = await dao.suggest_crops_for_parcel(
                    "urn:ngsi-ld:AgriParcel:test-42", season_slot="winter", top_n=5,
                )

                assert result["parcel_id"] == "urn:ngsi-ld:AgriParcel:test-42"
                assert result["data_quality"]["crops_evaluated"] >= 1
                assert len(result["suggestions"]) >= 1
                eppos = [s["crop_eppo"] for s in result["suggestions"]]
                assert "TRZAX" in eppos  # both winter cereals should be present
                s = result["suggestions"][0]
                assert s["best_variety"] == "LG_AURUS"
                assert s["agronomics"]["expected_yield_kg_ha"] == 6200
                assert "recommendation_trust" in s
                assert s["recommendation_trust"]["confidence"] == "high"
                assert s["suitability"]["overall"] == "suitable"
                assert "composite_score" in s

    @pytest.mark.asyncio
    async def test_filters_by_season(self):
        """Summer filter should exclude winter-only crops."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:test-42",
                "type": "AgriParcel",
                "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [-1.8, 42.1]}},
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": False, "source": "unavailable"}

            driver = _make_driver()
            dao = GraphDAO(driver)

            with patch.object(dao, "get_parcel_environment", new_callable=AsyncMock) as mock_env, \
                 patch.object(dao, "get_available_crops", new_callable=AsyncMock) as mock_crops, \
                 patch.object(dao, "extrapolate_varieties", new_callable=AsyncMock) as mock_ext, \
                 patch.object(dao, "get_soil_suitability", new_callable=AsyncMock) as mock_soil_req, \
                 patch.object(dao, "get_heat_tolerance", new_callable=AsyncMock) as mock_heat:

                mock_env.return_value = {
                    "parcel_id": "urn:ngsi-ld:AgriParcel:test-42",
                    "climate_class": "Csa",
                    "soil": {"data_available": False},
                    "irrigation": {"inferred": None, "source": "unknown", "overridable": True},
                    "campaign": {"assigned": False, "crop_eppo": None},
                    "inputs_used": {"soil": "unavailable", "climate": "trial_proxy"},
                }
                # TRZAX = winter, ZEAMX = summer
                mock_crops.return_value = [
                    {"eppo_code": "TRZAX", "scientific_name": "Triticum aestivum", "trial_count": 15},
                    {"eppo_code": "ZEAMX", "scientific_name": "Zea mays", "trial_count": 8},
                ]
                mock_ext.return_value = {
                    "ranked_varieties": [{"variety": "TEST", "mean_yield_kg_ha": 5000, "confidence": "medium",
                                           "trial_count": 5}],
                    "similar_sites": [],
                    "data_quality": {"total_trials_analyzed": 5, "unique_varieties": 3},
                }
                mock_soil_req.return_value = None
                mock_heat.return_value = None

                result = await dao.suggest_crops_for_parcel(
                    "urn:ngsi-ld:AgriParcel:test-42", season_slot="summer", top_n=5,
                )

                assert result["data_quality"]["season_filter_excluded"] >= 1

    @pytest.mark.asyncio
    async def test_organic_dual_yield(self):
        """When management=organic, should return both conventional and organic yield."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:test-42",
                "type": "AgriParcel",
                "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [-1.8, 42.1]}},
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": False, "source": "unavailable"}

            driver = _make_driver()
            dao = GraphDAO(driver)

            with patch.object(dao, "get_parcel_environment", new_callable=AsyncMock) as mock_env, \
                 patch.object(dao, "get_available_crops", new_callable=AsyncMock) as mock_crops, \
                 patch.object(dao, "extrapolate_varieties", new_callable=AsyncMock) as mock_ext, \
                 patch.object(dao, "get_soil_suitability", new_callable=AsyncMock) as mock_soil_req, \
                 patch.object(dao, "get_heat_tolerance", new_callable=AsyncMock) as mock_heat:

                mock_env.return_value = {
                    "parcel_id": "urn:ngsi-ld:AgriParcel:test-42",
                    "climate_class": "Csa",
                    "soil": {"data_available": False},
                    "irrigation": {"inferred": None, "source": "unknown", "overridable": True},
                    "campaign": {"assigned": False, "crop_eppo": None},
                    "inputs_used": {"soil": "unavailable", "climate": "trial_proxy"},
                }
                mock_crops.return_value = [
                    {"eppo_code": "TRZAX", "scientific_name": "Triticum aestivum", "trial_count": 15},
                ]
                mock_ext.return_value = {
                    "ranked_varieties": [{"variety": "LG_AURUS", "mean_yield_kg_ha": 6200, "confidence": "high",
                                           "trial_count": 12}],
                    "similar_sites": [],
                    "data_quality": {"total_trials_analyzed": 12, "unique_varieties": 5},
                }
                mock_soil_req.return_value = None
                mock_heat.return_value = None

                result = await dao.suggest_crops_for_parcel(
                    "urn:ngsi-ld:AgriParcel:test-42", management="organic", top_n=5,
                )

                s = result["suggestions"][0]
                assert s["yield_conventional_kg_ha"] == 6200
                assert s["yield_organic_kg_ha"] == 4960  # 6200 * 0.80
                assert s["recommendation_trust"]["organic_warning"] is not None
                assert "conventional_only_trials" in s["recommendation_trust"]["data_gaps"]
