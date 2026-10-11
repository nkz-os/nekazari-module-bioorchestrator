from unittest.mock import AsyncMock

from app.graph.evidence_sites import evidence_sites
from app.graph.parcel_conditions import conditions_from_environment

ENV = {"climate_class": "Csa", "country": "ES", "centroid": {"lat": 42.0, "lon": -1.6},
       "soil": {"data_available": True, "wrb_type": "Calcisol", "ph": 8.1, "texture": "clay loam"},
       "irrigation": {"inferred": "secano"},
       "climate_detail": {"annual_rainfall_mm": 450.0, "annual_et0_mm": 1100.0,
                          "coldest_month_min_c": -1.0, "annual_temp_c": 13.0}}


def test_conditions_carry_parcel_values_in_the_recommender_order():
    c = conditions_from_environment(ENV, management="organic", crops=["TRZAX"], top_n=1)
    assert list(c)[:12] == ["climate_class", "soil_type", "soil_ph", "soil_texture", "irrigation_regime",
                            "management", "season", "purpose", "crops", "top_n", "frost_margin_c", "country"]
    assert (c["soil_type"], c["irrigation_regime"], c["lat"], c["annual_temp_c"]) == ("Calcisol", "secano", 42.0, 13.0)


def test_soil_values_dropped_when_soil_unavailable():
    env = {**ENV, "soil": {"data_available": False, "ph": 8.1}}
    c = conditions_from_environment(env)
    assert (c["soil_type"], c["soil_ph"], c["soil_texture"]) == (None, None, None)


async def test_regional_sites_include_aggregates_scoped_by_country():
    dao = AsyncMock()
    dao.get_similar_sites.return_value = [{"name": "Zona X"}]
    sites = await evidence_sites(dao, conditions_from_environment(ENV), similarity="koppen", tier="regional", country="ES")
    assert sites == [{"name": "Zona X"}]
    dao.get_similar_sites.assert_awaited_once_with(climate_class="Csa", soil_type=None, limit=None,
                                                   include_aggregate=True, country="ES")


async def test_field_sites_are_the_koppen_class_with_the_soil_type():
    dao = AsyncMock()
    dao.get_similar_sites.return_value = [{"name": "Campo 1"}]
    sites = await evidence_sites(dao, conditions_from_environment(ENV), similarity="koppen", tier="field", country="ES")
    assert sites == [{"name": "Campo 1"}]
    dao.get_similar_sites.assert_awaited_once_with(climate_class="Csa", soil_type="Calcisol", limit=None)


async def test_v2_sites_use_the_four_numeric_climate_inputs():
    dao = AsyncMock()
    dao.get_similar_sites.return_value = [{"name": "Campo 2"}]
    sites = await evidence_sites(dao, conditions_from_environment(ENV), similarity="vector_v2_fallback",
                                 tier="field", country="ES")
    assert sites == [{"name": "Campo 2"}]
    dao.get_similar_sites.assert_awaited_once_with(
        climate_class="Csa", soil_type="Calcisol", rainfall_min=None, rainfall_max=None, limit=50,
        target_features={"rainfall": 450.0, "et0": 1100.0, "coldest_min": -1.0, "annual_temp": 13.0},
        vector_version="v2")
