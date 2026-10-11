from app.planning.fit import (
    MULTI_YEAR,
    REGIME_MISSING,
    ParcelClimate,
    frost_reasons,
    soil_reasons,
    verdict_of,
    water_reasons,
)


def _gate(verdict, ph=None):
    return {"verdict": verdict, "ph": ph or {"value": None, "verdict": None},
            "texture": {"value": None, "verdict": None}, "drainage": {"value": None, "verdict": None}}


def test_soil_unsuitable_ph_is_a_blocker_with_values():
    reasons = soil_reasons(_gate("unsuitable", {"value": 8.6, "min": 5.5, "max": 7.5, "verdict": "unsuitable"}),
                           parcel_soil_available=True, tolerance_available=True)
    assert reasons == [{"code": "soil_ph_unsuitable", "gate": "soil",
                        "params": {"value": 8.6, "min": 5.5, "max": 7.5}}]
    assert verdict_of(reasons) == "no_fit"


def test_missing_parcel_soil_is_a_caveat_never_fits():
    reasons = soil_reasons(_gate("unknown"), parcel_soil_available=False, tolerance_available=True)
    assert reasons[0]["params"] == {"why": "no_parcel_soil"}
    assert verdict_of(reasons) == "caveats"


def test_spring_crop_ignores_the_winter_minimum():
    assert frost_reasons("spring", coldest_min=-6.0, frost_damage_c=0.0) == []


def test_autumn_crop_frost_risk_and_unknown_season():
    risk = frost_reasons("autumn", coldest_min=-12.0, frost_damage_c=-15.0)
    assert risk[0]["code"] == "frost_risk" and verdict_of(risk) == "no_fit"
    assert frost_reasons(None, coldest_min=-12.0, frost_damage_c=-15.0)[0]["params"] == {"why": "season_unknown"}


def test_water_only_for_rainfed_and_unknown_without_climate():
    dry = ParcelClimate(coldest_month_min_c=0.0, annual_rainfall_mm=300.0, annual_et0_mm=1300.0)
    assert water_reasons("regadío", dry, 180) == []
    assert water_reasons("secano", dry, 180)[0]["code"] == "water_deficit_high"
    assert water_reasons("secano", ParcelClimate(None, None, None), 180)[0]["code"] == "water_unknown"


def test_benefit_alone_still_fits_and_missing_regime_is_a_caveat():
    assert verdict_of([{"code": "rotation_benefit", "gate": "rotation", "params": {}}]) == "fits"
    assert verdict_of([MULTI_YEAR]) == "fits"
    assert verdict_of([REGIME_MISSING]) == "caveats"


def test_soil_without_a_known_overall_verdict_is_not_comparable_never_fits():
    reasons = soil_reasons({}, parcel_soil_available=True, tolerance_available=True)
    assert reasons == [{"code": "soil_unknown", "gate": "soil", "params": {"why": "not_comparable"}}]
    assert verdict_of(reasons) == "caveats"


def test_overall_unsuitable_without_dimension_detail_is_a_blocker():
    reasons = soil_reasons({"verdict": "unsuitable"}, parcel_soil_available=True, tolerance_available=True)
    assert reasons == [{"code": "soil_unsuitable", "gate": "soil", "params": {}}]
    assert verdict_of(reasons) == "no_fit"


def test_overall_marginal_without_dimension_detail_is_a_caveat():
    reasons = soil_reasons({"verdict": "marginal"}, parcel_soil_available=True, tolerance_available=True)
    assert reasons == [{"code": "soil_marginal", "gate": "soil", "params": {}}]
    assert verdict_of(reasons) == "caveats"


def test_unrecognised_frost_sowing_type_is_unknown_not_a_pass():
    reasons = frost_reasons("winterish", coldest_min=-6.0, frost_damage_c=0.0)
    assert reasons == [{"code": "frost_unknown", "gate": "frost", "params": {"why": "season_unknown"}}]
    assert verdict_of(reasons) == "caveats"


def test_summer_crop_is_not_exposed_to_winter_frost():
    assert frost_reasons("summer", coldest_min=-12.0, frost_damage_c=-15.0) == []


def test_non_positive_season_days_fall_back_to_the_default_and_say_so():
    dry = ParcelClimate(coldest_month_min_c=0.0, annual_rainfall_mm=300.0, annual_et0_mm=1300.0)
    out = water_reasons("secano", dry, 0)
    assert out[0]["code"] == "water_deficit_high"
    assert out[0]["params"]["season_days_default"] is True
