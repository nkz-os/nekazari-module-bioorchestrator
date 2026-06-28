"""Tests for POST /agriculture/rotation-optimize endpoint."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.graph.dao import GraphDAO


class TestOptimizeRotation:
    """Verify optimize_rotation solves a greedy rotation by priorities."""

    @pytest.mark.asyncio
    async def test_basic_two_year_plan_with_priorities(self):
        """Two-year plan: year 1 N fixation, year 2 protein."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:test-42",
                "type": "AgriParcel",
                "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [-1.8, 42.1]}},
                "area": {"type": "Property", "value": 12.4},
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": True, "texture": "loam", "ph": 7.2, "source": "soilgrids"}

            driver = MagicMock()
            session = MagicMock()
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock(return_value=None)

            class MockResult:
                def __init__(self, data):
                    self._data = data
                async def data(self):
                    return self._data
                def __aiter__(self):
                    self._idx = 0
                    return self
                async def __anext__(self):
                    if self._idx >= len(self._data):
                        raise StopAsyncIteration
                    r = self._data[self._idx]; self._idx += 1
                    m = MagicMock(); m.__getitem__ = lambda s, k: r.get(k); m.get = r.get; m.keys = lambda: r.keys()
                    return m
                async def single(self):
                    r = self._data[0] if self._data else {}
                    m = MagicMock(); m.__getitem__ = lambda s, k: r.get(k); m.get = r.get; m.keys = lambda: r.keys()
                    return m

            session.run = AsyncMock(return_value=MockResult([{"cc": "Csa", "tlat": 42.2, "tlon": -1.9}]))
            driver.session.return_value = session

            dao = GraphDAO(driver)

            with patch.object(dao, "get_parcel_environment", new_callable=AsyncMock) as mock_env, \
                 patch.object(dao, "get_available_crops", new_callable=AsyncMock) as mock_crops, \
                 patch.object(dao, "extrapolate_varieties", new_callable=AsyncMock) as mock_ext, \
                 patch("app.services.crop_reference.get_crop_ref", new_callable=AsyncMock) as mock_ref, \
                 patch.object(dao, "recommend_next_crop", new_callable=AsyncMock) as mock_next, \
                 patch.object(dao, "get_regenerative_sequence", new_callable=AsyncMock) as mock_regen, \
                 patch.object(dao, "get_rotation_constraints", new_callable=AsyncMock) as mock_constraints, \
                 patch.object(dao, "get_shared_pests", new_callable=AsyncMock) as mock_pests, \
                 patch.object(dao, "_evaluate_pac_compliance", new_callable=AsyncMock) as mock_pac:

                mock_env.return_value = {
                    "parcel_id": "urn:ngsi-ld:AgriParcel:test-42",
                    "climate_class": "Csa",
                    "soil": {"texture": "loam", "wrb_type": "Calcisol", "data_available": True},
                    "irrigation": {"inferred": "secano", "source": "unknown"},
                    "centroid": {"lat": 42.1, "lon": -1.8},
                    "inputs_used": {"soil": "soil_module", "climate": "trial_proxy"},
                    "campaign": {"assigned": False},
                }
                mock_crops.return_value = [
                    {"eppo_code": "CIEAR", "scientific_name": "Cicer arietinum", "trial_count": 10},
                    {"eppo_code": "PIBSX", "scientific_name": "Pisum sativum", "trial_count": 8},
                    {"eppo_code": "PIBSX", "scientific_name": "Pisum sativum", "trial_count": 8},
                ]
                mock_ext.return_value = {
                    "ranked_varieties": [{"variety": "TEST", "mean_yield_kg_ha": 2000}],
                    "similar_sites": [],
                }
                mock_ref.return_value = {
                    "carbon_fixed_tco2e_ha": 0.9, "n_fixation_kg_ha": 80, "n_requirement_kg_ha": 30,
                    "growing_season_days": 150, "operations_count": 4,
                }
                mock_next.return_value = [{"name": "Cicer arietinum"}, {"name": "Pisum sativum"}]
                mock_regen.return_value = {
                    "cover_crop": "SECCE", "cover_crop_common": "Rye",
                    "cover_biomass_t_ha": 4.2, "n_cover_available_kg_ha": 45,
                    "termination_method": "roller_crimper",
                    "cover_crop_sowing_date": "2026-10-15",
                    "termination_date_estimate": "2027-04-20",
                }
                mock_constraints.return_value = []
                mock_pests.return_value = {"shared_pests": [], "shared_count": 0, "risk_level": "none"}
                mock_pac.return_value = {"score": 85, "max_score": 100, "rules": []}

                result = await dao.optimize_rotation(
                    parcel_id="urn:ngsi-ld:AgriParcel:test-42",
                    years=2,
                    constraints={"gluten_free_only": False, "management": "conventional"},
                    priorities=[
                        {"year": 1, "protein": 0, "carbon": 0, "n_fixation": 100, "margin": 0, "yield": 0},
                        {"year": 2, "protein": 100, "carbon": 0, "n_fixation": 0, "margin": 0, "yield": 0},
                    ],
                )

                assert "error" not in result
                assert result["years"] == 2
                assert len(result["plan"]) == 2
                assert result["plan"][0]["year"] == 1
                assert result["plan"][0]["cash_crop"]["eppo"] in ("CIEAR", "PIBSX")
                assert result["plan"][0]["cover_crop"] is not None
                assert "cumulative" in result

    @pytest.mark.asyncio
    async def test_gluten_free_excludes_triticeae(self):
        """Gluten-free constraint should exclude wheat but keep legumes."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:test-42",
                "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [-1.8, 42.1]}},
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": False}

            driver = MagicMock()
            session = MagicMock()
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock(return_value=None)
            session.run = AsyncMock(return_value=MagicMock())
            session.run.return_value.__aiter__.return_value = iter([{"cc": "Csa", "tlat": 42.2, "tlon": -1.9}])
            session.run.return_value.data = AsyncMock(return_value=[{"cc": "Csa", "tlat": 42.2, "tlon": -1.9}])
            driver.session.return_value = session

            dao = GraphDAO(driver)

            with patch.object(dao, "get_parcel_environment", new_callable=AsyncMock) as mock_env, \
                 patch.object(dao, "get_available_crops", new_callable=AsyncMock) as mock_crops, \
                 patch.object(dao, "extrapolate_varieties", new_callable=AsyncMock) as mock_ext, \
                 patch("app.services.crop_reference.get_crop_ref", new_callable=AsyncMock) as mock_ref, \
                 patch.object(dao, "recommend_next_crop", new_callable=AsyncMock) as mock_next, \
                 patch.object(dao, "get_regenerative_sequence", new_callable=AsyncMock) as mock_regen, \
                 patch.object(dao, "get_rotation_constraints", new_callable=AsyncMock) as mock_constraints, \
                 patch.object(dao, "get_shared_pests", new_callable=AsyncMock) as mock_pests, \
                 patch.object(dao, "_evaluate_pac_compliance", new_callable=AsyncMock) as mock_pac:

                mock_env.return_value = {
                    "parcel_id": "urn:ngsi-ld:AgriParcel:test-42",
                    "climate_class": "Csa",
                    "soil": {"data_available": False},
                    "irrigation": {"inferred": None},
                    "centroid": {"lat": 42.1, "lon": -1.8},
                    "inputs_used": {"soil": "unavailable", "climate": "trial_proxy"},
                    "campaign": {"assigned": False},
                }
                # TRZAX = wheat (contains gluten), CIEAR = chickpea (GF)
                mock_crops.return_value = [
                    {"eppo_code": "TRZAX", "scientific_name": "Triticum aestivum", "trial_count": 15},
                    {"eppo_code": "CIEAR", "scientific_name": "Cicer arietinum", "trial_count": 10},
                    {"eppo_code": "PIBSX", "scientific_name": "Pisum sativum", "trial_count": 8},
                ]
                mock_ext.return_value = {"ranked_varieties": [{"variety": "T", "mean_yield_kg_ha": 2000}]}
                mock_ref.return_value = {"carbon_fixed_tco2e_ha": 1.0, "n_fixation_kg_ha": 50, "n_requirement_kg_ha": 30, "operations_count": 4}
                mock_next.return_value = [{"name": "Cicer arietinum", "scientific_name": "Cicer arietinum"}, {"name": "Pisum sativum", "scientific_name": "Pisum sativum"}]
                mock_regen.return_value = {"cover_crop": "SECCE", "cover_crop_common": "Rye"}
                mock_constraints.return_value = []
                mock_pests.return_value = {"shared_pests": [], "shared_count": 0, "risk_level": "none"}
                mock_pac.return_value = {"score": 80, "max_score": 100, "rules": []}

                result = await dao.optimize_rotation(
                    parcel_id="urn:ngsi-ld:AgriParcel:test-42",
                    years=2,
                    constraints={"gluten_free_only": True, "management": "conventional"},
                    priorities=[{"year": 1, "protein": 0, "carbon": 0, "n_fixation": 100, "margin": 0, "yield": 0}],
                )

                # All cash crops must be gluten-free
                for year_entry in result["plan"]:
                    assert year_entry["cash_crop"]["gluten_status"] != "contains_gluten"
                    assert year_entry["cash_crop"]["eppo"] != "TRZAX"

    @pytest.mark.asyncio
    async def test_locked_year_preserved(self):
        """Locked year should use specified crop despite priorities."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:test-42",
                "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [-1.8, 42.1]}},
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": False}

            driver = MagicMock()
            session = MagicMock()
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock(return_value=None)
            session.run = AsyncMock(return_value=MagicMock())
            session.run.return_value.__aiter__.return_value = iter([{"cc": "Csa", "tlat": 42.2, "tlon": -1.9}])
            driver.session.return_value = session

            dao = GraphDAO(driver)

            with patch.object(dao, "get_parcel_environment", new_callable=AsyncMock) as mock_env, \
                 patch.object(dao, "get_available_crops", new_callable=AsyncMock) as mock_crops, \
                 patch.object(dao, "extrapolate_varieties", new_callable=AsyncMock) as mock_ext, \
                 patch("app.services.crop_reference.get_crop_ref", new_callable=AsyncMock) as mock_ref, \
                 patch.object(dao, "recommend_next_crop", new_callable=AsyncMock) as mock_next, \
                 patch.object(dao, "get_regenerative_sequence", new_callable=AsyncMock) as mock_regen, \
                 patch.object(dao, "get_rotation_constraints", new_callable=AsyncMock) as mock_constraints, \
                 patch.object(dao, "get_shared_pests", new_callable=AsyncMock) as mock_pests, \
                 patch.object(dao, "_evaluate_pac_compliance", new_callable=AsyncMock) as mock_pac:

                mock_env.return_value = {
                    "parcel_id": "urn:ngsi-ld:AgriParcel:test-42",
                    "climate_class": "Csa",
                    "soil": {"data_available": False},
                    "irrigation": {"inferred": None},
                    "centroid": {"lat": 42.1, "lon": -1.8},
                    "inputs_used": {"soil": "unavailable", "climate": "trial_proxy"},
                    "campaign": {"assigned": False},
                }
                mock_crops.return_value = [
                    {"eppo_code": "PIBSX", "scientific_name": "Pisum sativum", "trial_count": 8},
                    {"eppo_code": "CIEAR", "scientific_name": "Cicer arietinum", "trial_count": 10},
                    {"eppo_code": "PIBSX", "scientific_name": "Pisum sativum", "trial_count": 8},
                ]
                mock_ext.return_value = {"ranked_varieties": [{"variety": "T", "mean_yield_kg_ha": 2000}]}
                mock_ref.return_value = {"carbon_fixed_tco2e_ha": 1.0, "n_fixation_kg_ha": 50, "n_requirement_kg_ha": 30, "operations_count": 4}
                mock_next.return_value = [{"name": "Cicer arietinum", "scientific_name": "Cicer arietinum"}, {"name": "Pisum sativum", "scientific_name": "Pisum sativum"}]
                mock_regen.return_value = {"cover_crop": "SECCE", "cover_crop_common": "Rye"}
                mock_constraints.return_value = []
                mock_pests.return_value = {"shared_pests": [], "shared_count": 0, "risk_level": "none"}
                mock_pac.return_value = {"score": 80, "max_score": 100, "rules": []}

                result = await dao.optimize_rotation(
                    parcel_id="urn:ngsi-ld:AgriParcel:test-42",
                    years=2,
                    constraints={},
                    priorities=[{"year": 1, "protein": 0, "carbon": 0, "n_fixation": 100, "margin": 0, "yield": 0}],
                    locked_years={1: "PIBSX"},
                )

                assert result["plan"][0]["locked"] is True
                assert result["plan"][0]["cash_crop"]["eppo"] == "PIBSX"

    @pytest.mark.asyncio
    async def test_zero_weights_rejected(self):
        """All-zero priority weights should return 400 error."""
        with patch("app.graph.dao.OrionClient") as mock_orion_cls, \
             patch("app.services.soil_client.get_parcel_soil_properties") as mock_soil:

            mock_orion = AsyncMock()
            mock_orion_cls.return_value = mock_orion
            mock_orion.get_entity.return_value = {
                "id": "urn:ngsi-ld:AgriParcel:test-42",
                "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [-1.8, 42.1]}},
            }
            mock_orion.close = AsyncMock()
            mock_soil.return_value = {"data_available": False}

            driver = MagicMock()
            session = MagicMock()
            session.__aenter__ = AsyncMock(return_value=session)
            session.__aexit__ = AsyncMock(return_value=None)
            session.run = AsyncMock(return_value=MagicMock())
            session.run.return_value.__aiter__.return_value = iter([{"cc": "Csa", "tlat": 42.2, "tlon": -1.9}])
            driver.session.return_value = session

            dao = GraphDAO(driver)

            with patch.object(dao, "get_parcel_environment", new_callable=AsyncMock) as mock_env, \
                 patch.object(dao, "get_available_crops", new_callable=AsyncMock) as mock_crops:

                mock_env.return_value = {
                    "parcel_id": "test", "climate_class": "Csa",
                    "soil": {"data_available": False}, "irrigation": {"inferred": None},
                    "centroid": {"lat": 42.1, "lon": -1.8},
                    "inputs_used": {"soil": "unavailable", "climate": "trial_proxy"},
                    "campaign": {"assigned": False},
                }
                mock_crops.return_value = [{"eppo_code": "CIEAR", "scientific_name": "Cicer arietinum"}]

                result = await dao.optimize_rotation(
                    parcel_id="test", years=2,
                    priorities=[{"year": 1, "protein": 0, "carbon": 0, "n_fixation": 0, "margin": 0, "yield": 0}],
                )

                assert "error" in result
                assert "zero" in result["error"].lower()
