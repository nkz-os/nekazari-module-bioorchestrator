import pytest

from app.planning import phenology_experimental as pe
from app.services.phenology_vp import VPParams

MADRID = (40.4, -3.7)
BERLIN = (52.5, 13.4)
PARIS = (48.9, 2.35)
ZURICH = (47.4, 8.5)
SEA = (43.9, -9.9)  # outside every coarse polygon


def test_spain_uses_es():
    s = pe.select_parameters("wheat_winter", *MADRID)
    assert isinstance(s, pe.Selection)
    assert s.set == "ES" and s.zone == "ES"
    assert isinstance(s.params, VPParams)
    assert (s.params.rp, s.params.rv, s.params.t2, s.params.mat_p) == (4.5, 5.0, 200.0, 620.0)
    assert s.params.t1_adj == 401.0
    assert s.at_grid_bound == ["rv"]


def test_germany_uses_de():
    s = pe.select_parameters("barley_winter", *BERLIN)
    assert s.set == "DE" and s.zone == "DE"
    assert s.params.t2 == 100.0 and s.at_grid_bound == ["t2"]


def test_france_eu_joint_unvalidated():
    s = pe.select_parameters("wheat_winter", *PARIS)
    assert s.set == "EU_joint" and s.zone is None
    block = pe.validation_block("wheat_winter", s.zone)
    assert block["flowering_start"] == {"status": "unvalidated_zone"}
    assert block["maturity"] == {"status": "unvalidated_zone"}
    assert block["emergence"] == {"status": "same_as_operative"}


def test_switzerland_outside_eu():
    r = pe.select_parameters("wheat_winter", *ZURICH)
    assert isinstance(r, pe.Unavailable) and r.reason == "outside_eu"


def test_country_unknown_is_not_outside_eu():
    r = pe.select_parameters("wheat_winter", *SEA)
    assert isinstance(r, pe.Unavailable) and r.reason == "country_unknown"


def test_durum_outside_spain_es_unvalidated():
    for pt in (BERLIN, PARIS):
        s = pe.select_parameters("durum", *pt)
        assert s.set == "ES" and s.zone is None
        assert pe.validation_block("durum", s.zone)["flowering_start"]["status"] == "unvalidated_zone"
    assert pe.select_parameters("durum", *MADRID).zone == "ES"


def test_de_spring_barley_not_tested_descriptive():
    b = pe.validation_block("barley_spring", "DE")
    for m in ("flowering_start", "maturity"):
        assert b[m]["status"] == "not_tested"
        assert b[m]["descriptive"] is True
        assert b[m]["mae_days"] is not None


def test_validation_has_three_baselines():
    b = pe.validation_block("wheat_winter", "ES")["flowering_start"]
    assert b["status"] == "measured" and b["verdict"] == {"vs": "m1", "result": "wins"}
    assert set(b["compared_with"]) == {"m0_mae_days", "m1_mae_days", "calendar_null_mae_days"}
    for k in ("n", "bias_days", "mae_days", "p10_days", "p90_days", "dataset", "partition", "report"):
        assert k in b
    assert b["partition"] == "es_campaign"
    mat = pe.validation_block("wheat_winter", "ES")["maturity"]
    assert mat["verdict"] is None and mat["compared_with"]["calendar_null_mae_days"] is None


def test_small_maturity_not_tested():
    assert pe.validation_block("durum", "ES")["maturity"]["status"] == "not_tested"


def test_parameters_block_and_sources():
    s = pe.select_parameters("barley_winter", *MADRID)
    p = pe.parameters_block(s)
    assert p["set"] == "ES" and p["maturity_gdd"] == s.params.mat_p and p["t1_adj"] == 401
    assert p["at_grid_bound"] == ["rv", "t2"] and p["fitted_on"]
    src = pe.experimental_sources()
    assert all(set(x) == {"short", "detail"} for x in src)
    text = " ".join(x["short"] + x["detail"] for x in src)
    assert "Deutscher Wetterdienst" in text and "not physiological" in text
    assert ".md" not in text and "/" not in text.replace("CC BY 4.0", "")


def test_eu_has_27_members():
    assert len(pe.EU_MEMBERS) == 27 and "CH" not in pe.EU_MEMBERS


def test_missing_file_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(pe, "PARAMS_PATH", tmp_path / "nope.json")
    pe._params_file.cache_clear()
    try:
        with pytest.raises(OSError):
            pe.select_parameters("wheat_winter", *MADRID)
    finally:
        pe._params_file.cache_clear()


def test_es_barley_winter_shows_the_m1_verdict_and_the_fixed_date_baseline():
    b = pe.validation_block("barley_winter", "ES")["flowering_start"]
    assert b["verdict"] == {"vs": "m1", "result": "wins"}
    assert b["compared_with"]["calendar_null_mae_days"] < b["mae_days"]


def test_verdict_mapping(caplog):
    assert pe._verdict(None) is None
    assert pe._verdict("gana") == {"vs": "m1", "result": "wins"}
    assert pe._verdict("no_se_pudo_comprobar") == {"vs": "m1", "result": "not_checkable"}
    with caplog.at_level("WARNING"):
        assert pe._verdict("pierde") == {"vs": "m1", "result": "unknown"}
    assert "phenology_vp_unknown_verdict" in caplog.text
