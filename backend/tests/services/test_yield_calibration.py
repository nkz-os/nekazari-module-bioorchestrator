"""Expected yield in the parcel's region (water-limited ceiling x calibrated gap factor)."""
import math

import pytest

from app.services import yield_calibration as yc

NAVARRA = {"scheme": "es_province", "code": "31", "name": "Navarra"}


def test_wheat_and_barley_are_reported_at_commercial_moisture():
    for slug in ("wheat", "durum_wheat", "barley"):
        b = yc.yield_basis(slug)
        assert b["moisture"] == "commercial" and b["grain_moisture_pct"] == 13.0 and b["source"]
    assert yc.yield_basis("maize") == {"moisture": "dry_matter", "grain_moisture_pct": None, "source": None}


def test_to_basis_converts_dry_yield_and_percentiles():
    commercial = yc.yield_basis("wheat")
    assert math.isclose(yc.to_basis(3.48, commercial), 4.0)
    assert yc.to_basis({"p10": 0.87, "p50": 1.74, "p90": 2.61}, commercial) == pytest.approx(
        {"p10": 1.0, "p50": 2.0, "p90": 3.0})
    assert yc.to_basis(3.48, yc.yield_basis("maize")) == 3.48
    assert yc.to_basis(None, commercial) is None


def test_calibrated_phenology_only_for_rainfed_wheat_in_a_calibrated_province():
    params = yc.calibrated_phenology("wheat", "rainfed", NAVARRA)
    assert set(params) == {"Senescence", "Maturity", "HIstart", "Flowering", "YldForm"}
    assert yc.calibrated_phenology("wheat", "full", NAVARRA) is None
    assert yc.calibrated_phenology("maize", "rainfed", NAVARRA) is None
    assert yc.calibrated_phenology("wheat", "rainfed", None) is None


@pytest.mark.parametrize("slug,irrigation,region,reason", [
    ("maize", "rainfed", NAVARRA, "crop_not_calibrated"),
    ("wheat", "full", NAVARRA, "irrigated"),
    ("wheat", "rainfed", None, "outside_calibrated_regions"),
    ("wheat", "rainfed", {"scheme": "es_province", "code": "52", "name": "Melilla"}, "region_not_calibrated"),
])
def test_unavailable_reasons(slug, irrigation, region, reason):
    assert yc.expected_yield(slug, irrigation, region, 5.0) == {"status": "unavailable", "reason": reason}


def test_expected_range_scales_the_ceiling_and_brackets_it_with_the_held_out_errors():
    out = yc.expected_yield("wheat", "rainfed", NAVARRA, 8.0)
    cal = yc._calibration("wheat")
    f = cal["provinces"]["31"]["gap_factor"]
    assert out["status"] == "ok" and out["basis"] == "gap_corrected" and out["gap_factor"] == f
    assert out["region"] == NAVARRA
    # documented method: prediction = f x ceiling (kg/ha); bin by rank edges; P10/P90 of residuals
    pred = f * 8000.0
    rng = cal["range"]
    b = sum(pred >= e for e in rng["edges"])
    assert out["range_t_ha"]["low"] == pytest.approx(max(0.0, pred + rng["r10"][b]) / 1000, abs=0.01)
    assert out["range_t_ha"]["high"] == pytest.approx((pred + rng["r90"][b]) / 1000, abs=0.01)
    assert 0.7 < out["coverage"] < 0.9
    assert out["calibration"]["version"] and out["calibration"]["validation"]["rmse_kg_ha"] > 0


def test_in_season_range_spans_the_ensemble():
    one = yc.expected_yield("wheat", "rainfed", NAVARRA, 5.0)["range_t_ha"]
    spread = yc.expected_yield("wheat", "rainfed", NAVARRA, {"p10": 3.0, "p50": 5.0, "p90": 7.0})["range_t_ha"]
    assert spread["low"] <= one["low"] and spread["high"] >= one["high"]


def test_range_never_below_zero():
    out = yc.expected_yield("wheat", "rainfed", NAVARRA, 0.0)
    assert out["range_t_ha"]["low"] >= 0.0
