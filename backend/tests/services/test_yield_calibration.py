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


BARLEY_PHENOLOGY_KEYS = {"Senescence", "Maturity", "HIstart", "Flowering", "YldForm"}


def test_barley_phenology_only_for_rainfed_in_a_province_of_the_file():
    assert yc.calibrated_phenology("barley", "rainfed", NAVARRA) == {
        "Senescence": 1199.1, "Maturity": 1927.4, "HIstart": 1003.0, "Flowering": 180.0, "YldForm": 757.1}
    assert yc.calibrated_phenology("barley", "full", NAVARRA) is None
    assert yc.calibrated_phenology("barley", "rainfed", None) is None
    assert yc.calibrated_phenology("barley", "rainfed", {"scheme": "es_province", "code": "52", "name": "x"}) is None


def test_barley_file_has_no_yield_figures_and_every_province_passes_the_engine_check():
    from app.services.engines import aquacrop_engine as aq
    cal = yc._calibration("barley")
    assert cal["crop"] == "barley" and cal["validation"]["status"] == "not_validated"
    assert "range" not in cal
    assert len(cal["provinces"]) == 45
    for code, prov in cal["provinces"].items():
        assert "gap_factor" not in prov and set(prov["phenology"]) == BARLEY_PHENOLOGY_KEYS, code
        assert aq._validate_crop_params("BarleyES", prov["phenology"]) == prov["phenology"], code


@pytest.mark.parametrize("irrigation,region", [
    ("rainfed", NAVARRA), ("full", NAVARRA), ("rainfed", None),
    ("rainfed", {"scheme": "es_province", "code": "52", "name": "Melilla"})])
def test_barley_expected_yield_stays_without_a_figure(irrigation, region):
    assert yc.expected_yield("barley", irrigation, region, 5.0) == {
        "status": "unavailable", "reason": "crop_not_calibrated"}


# ----------------------------------------------------- unreadable calibration files
@pytest.fixture
def broken_files(monkeypatch, tmp_path):
    """DATA_DIR with a missing barley file and a malformed wheat file; caches cleared around the test."""
    (tmp_path / "wheat_es_province_calibration.json").write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(yc, "DATA_DIR", tmp_path)
    monkeypatch.setattr(yc, "_FAILED_FILES", set())
    yc._calibration.cache_clear()
    yield
    yc._calibration.cache_clear()


@pytest.mark.parametrize("slug", ["barley", "wheat", "durum_wheat"])
def test_an_unreadable_file_means_no_calibration(broken_files, slug, caplog):
    from app.services import sim_jobs as sj
    assert yc._calibration(slug) is None
    assert yc.calibrated_phenology(slug, "rainfed", NAVARRA) is None
    assert yc.expected_yield(slug, "rainfed", NAVARRA, 5.0) == {
        "status": "unavailable", "reason": "crop_not_calibrated"}
    assert sj.crop_simulation_version(slug) == sj.crop_simulation_version("maize")


def test_an_unreadable_file_is_logged_once_per_file(broken_files, caplog):
    with caplog.at_level("ERROR", logger=yc.logger.name):
        for slug in ("wheat", "durum_wheat", "barley"):
            yc._calibration(slug)
            yc._calibration(slug)
    msgs = [r.getMessage() for r in caplog.records]
    assert len(msgs) == 2
    assert any("file=wheat_es_province_calibration.json" in m and "crop=wheat" in m for m in msgs)
    assert any("file=barley_es_province_calibration.json" in m and "crop=barley" in m for m in msgs)


def test_a_province_without_gap_factor_is_not_calibrated(monkeypatch):
    cal = {"version": "x", "region_scheme": "es_province", "range": {}, "provinces": {"31": {"phenology": {}}}}
    monkeypatch.setattr(yc, "_calibration", lambda slug: cal)
    assert yc.expected_yield("wheat", "rainfed", NAVARRA, 5.0) == {
        "status": "unavailable", "reason": "region_not_calibrated"}


def test_barley_maturity_keeps_the_lower_bound_of_its_rule():
    maturities = [p["phenology"]["Maturity"] for p in yc._calibration("barley")["provinces"].values()]
    assert min(maturities) == 1577.1 and max(maturities) > 1746.0
