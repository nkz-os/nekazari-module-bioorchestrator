import copy

import pytest

from app.planning import provincial_normal as pn
from app.planning.models import CONTRACT_VERSION, Estimate

_LOADER = pn._normal_data
BARLEY_SCOPE = "normal of the province for rainfed barley; not a forecast of this season"
REGION = {"scheme": "es_province", "code": "02", "name": "Albacete"}


@pytest.fixture(autouse=True)
def _fresh_cache():
    _LOADER.cache_clear()
    yield
    _LOADER.cache_clear()


def _patched(monkeypatch, mutate, crop="TRZAX"):
    data = copy.deepcopy(_LOADER(crop))
    mutate(data)
    original = pn._normal_data
    monkeypatch.setattr(pn, "_normal_data", lambda c: data if c == crop else original(c))


def test_contract_version():
    assert CONTRACT_VERSION == "2.13"


def test_valid_province_gives_validated_estimate():
    out = pn.normal_estimate("TRZAX", "rainfed", "ES", REGION)
    est = Estimate.model_validate(out)
    data = _LOADER("TRZAX")
    mean = data["provinces"]["02"]["mean_t_ha"]
    assert est.provider_id == "provincial_normal" == pn.PROVIDER_ID
    assert est.provider_version == data["version"]
    assert est.basis == "provincial_normal"
    assert est.percentiles.p50 == round(mean, 2)
    assert est.range.low == est.percentiles.p10
    assert est.range.high == est.percentiles.p90
    assert est.percentiles.p10 == round(max(0.0, mean + data["band_t_ha"]["p10"]), 2)
    assert est.percentiles.p90 == round(mean + data["band_t_ha"]["p90"], 2)
    assert est.coverage == 0.78
    assert est.moisture.basis == "commercial" and est.moisture.pct == 13.0
    assert est.n == data["provinces"]["02"]["n_years"] and est.n_kind == "years"
    assert est.validation.status == "validated"
    assert est.validation.scope == "normal of the province for rainfed wheat, soft and durum together; not a forecast of this season"
    assert "RMSE 0.76 t/ha" in est.validation.metric
    assert est.graph_id is None
    assert [a["source_id"] for a in est.attributions] == ["mapa_anuario_estadistica"]


@pytest.mark.parametrize("args", [
    ("TRZDU", "rainfed", "ES", REGION),
    ("HORVX", "irrigated", "ES", REGION),
    ("HORVX", "rainfed", "FR", REGION),
    (None, "rainfed", "ES", REGION),
    ("ZEAMX", "rainfed", "ES", REGION),
    ("TRZAX", "irrigated", "ES", REGION),
    ("TRZAX", None, "ES", REGION),
    ("TRZAX", "rainfed", "FR", REGION),
    ("TRZAX", "rainfed", None, REGION),
    ("TRZAX", "rainfed", "ES", None),
    ("TRZAX", "rainfed", "ES", {"scheme": "other", "code": "02"}),
    ("TRZAX", "rainfed", "ES", {"scheme": "es_province", "code": "99"}),
    ("TRZAX", "rainfed", "ES", {"scheme": "es_province"}),
])
def test_out_of_scope_gives_none(args):
    assert pn.normal_estimate(*args) is None


def test_province_with_fewer_than_min_years_gives_none(monkeypatch):
    _patched(monkeypatch, lambda d: d["provinces"]["02"].update(n_years=4))
    assert pn.normal_estimate("TRZAX", "rainfed", "ES", REGION) is None


def test_lower_bound_never_negative(monkeypatch):
    _patched(monkeypatch, lambda d: d["provinces"]["02"].update(mean_t_ha=0.3))
    est = pn.normal_estimate("TRZAX", "rainfed", "ES", REGION)
    assert est["percentiles"]["p10"] == 0.0 and est["range"]["low"] == 0.0


def test_missing_file_gives_none(monkeypatch, tmp_path):
    monkeypatch.setattr(pn, "DATA_DIR", tmp_path)
    assert pn.normal_estimate("TRZAX", "rainfed", "ES", REGION) is None
    assert pn.normal_estimate("HORVX", "rainfed", "ES", REGION) is None


def test_missing_barley_file_leaves_wheat_working(monkeypatch, tmp_path):
    (tmp_path / pn.DATA_FILES["TRZAX"]).write_text((pn.DATA_DIR / pn.DATA_FILES["TRZAX"]).read_text())
    monkeypatch.setattr(pn, "DATA_DIR", tmp_path)
    assert pn.normal_estimate("HORVX", "rainfed", "ES", REGION) is None
    assert pn.normal_estimate("TRZAX", "rainfed", "ES", REGION)["n_kind"] == "years"


def test_malformed_wheat_file_leaves_barley_working(monkeypatch, tmp_path):
    (tmp_path / pn.DATA_FILES["TRZAX"]).write_text("{not json")
    (tmp_path / pn.DATA_FILES["HORVX"]).write_text((pn.DATA_DIR / pn.DATA_FILES["HORVX"]).read_text())
    monkeypatch.setattr(pn, "DATA_DIR", tmp_path)
    assert pn.normal_estimate("TRZAX", "rainfed", "ES", REGION) is None
    assert pn.normal_estimate("HORVX", "rainfed", "ES", REGION) is not None


def test_failing_file_is_logged_once(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(pn, "DATA_DIR", tmp_path)
    for _ in range(3):
        pn.normal_estimate("HORVX", "rainfed", "ES", REGION)
    assert sum("provincial_normal_unavailable" in r.message for r in caplog.records) == 1


def test_barley_rainfed_es_gives_validated_estimate_with_its_own_scope():
    out = pn.normal_estimate("HORVX", "rainfed", "ES", REGION)
    est = Estimate.model_validate(out)
    data = _LOADER("HORVX")
    mean = data["provinces"]["02"]["mean_t_ha"]
    assert est.basis == "provincial_normal" and est.provider_version == data["version"]
    assert est.percentiles.p50 == round(mean, 2)
    assert est.percentiles.p10 == round(max(0.0, mean + data["band_t_ha"]["p10"]), 2)
    assert est.percentiles.p90 == round(mean + data["band_t_ha"]["p90"], 2)
    assert est.coverage == 0.78 and est.moisture.pct == 13.0 and est.n_kind == "years"
    assert est.validation.status == "validated" and est.validation.scope == BARLEY_SCOPE
    assert est.validation.metric == (
        "RMSE 0.78 t/ha out of sample (provincial statistics, 699 province-years 2008-2022); band covers 78 %")


def test_wheat_and_barley_scopes_stay_apart():
    assert pn.normal_estimate("TRZAX", "rainfed", "ES", REGION)["validation"]["scope"] != BARLEY_SCOPE


def test_validation_metric_derived_from_file(monkeypatch):
    out = pn.normal_estimate("TRZAX", "rainfed", "ES", REGION)
    assert out["validation"]["metric"] == (
        "RMSE 0.76 t/ha out of sample (provincial statistics, 657 province-years 2008-2022); band covers 78 %")

    def mutate(d):
        d["validation"].update(rmse_t_ha=1.234, n=10, years="2010-2012")
        d["coverage"] = 0.5
    _patched(monkeypatch, mutate)
    out = pn.normal_estimate("TRZAX", "rainfed", "ES", REGION)
    assert out["validation"]["metric"] == (
        "RMSE 1.23 t/ha out of sample (provincial statistics, 10 province-years 2010-2012); band covers 50 %")
