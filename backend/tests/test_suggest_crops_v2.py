"""suggest_crops_for_parcel selects the v2 vector only when flag + CHELSA source agree."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.graph.dao import GraphDAO

_CHELSA = {
    "annual_temp_c": 13.0, "annual_rainfall_mm": 500.0, "annual_et0_mm": 1000.0,
    "coldest_month_min_c": -3.0, "frost_days_per_year": None, "source": "chelsa_v2.1",
}


async def _call(climate_detail, ext_results=None, want_trust=False):
    dao = GraphDAO(MagicMock())
    env = {
        "parcel_id": "urn:ngsi-ld:AgriParcel:t", "area_ha": 1.0, "climate_class": "Csa",
        "soil": {"data_available": False}, "irrigation": {}, "inputs_used": {},"climate_detail": climate_detail,
    }
    with patch.object(dao, "get_parcel_environment", AsyncMock(return_value=env)), \
         patch.object(dao, "get_available_crops", AsyncMock(return_value=[
             {"eppo_code": "TRZAX", "scientific_name": "x", "trial_count": 1}])), \
         patch.object(dao, "extrapolate_varieties", AsyncMock(
             return_value={"ranked_varieties": []}, side_effect=ext_results)) as ext, \
         patch.object(dao, "get_soil_suitability", AsyncMock(return_value=None)), \
         patch.object(dao, "get_heat_tolerance", AsyncMock(return_value=None)):
        res = await dao.suggest_crops_for_parcel("urn:ngsi-ld:AgriParcel:t")
    if want_trust:
        return ext, res
    return ext.await_args.kwargs


async def test_v2_with_chelsa_passes_target_features(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v2")
    kw = await _call(dict(_CHELSA))
    assert kw["vector_version"] == "v2"
    assert kw["target_features"] == {
        "rainfall": 500.0, "et0": 1000.0, "coldest_min": -3.0, "annual_temp": 13.0,
    }


async def test_v1_env_keeps_legacy_path(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v1")
    kw = await _call(dict(_CHELSA))
    assert "target_features" not in kw
    assert kw.get("vector_version", "v1") == "v1"


async def test_unset_env_defaults_to_v1(monkeypatch):
    monkeypatch.delenv("AGROCLIMATIC_VECTOR", raising=False)
    kw = await _call(dict(_CHELSA))
    assert "target_features" not in kw


@pytest.mark.parametrize("detail", [
    {**_CHELSA, "source": "trial_proxy"},
    None,
])
async def test_v2_without_chelsa_source_keeps_legacy_path(monkeypatch, detail):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v2")
    kw = await _call(detail)
    assert "target_features" not in kw
    assert kw.get("vector_version", "v1") == "v1"


_HIT = {"ranked_varieties": [{"variety": "V", "mean_yield_kg_ha": 5000.0, "trial_count": 4}],
        "similar_sites": ["S"]}
_MISS = {"ranked_varieties": [{"variety": "V", "mean_yield_kg_ha": None}]}


def _sim(res):
    return res["suggestions"][0]["recommendation_trust"]["similarity"]


async def test_hybrid_koppen_hit_single_call(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "hybrid")
    ext, res = await _call(dict(_CHELSA), [_HIT], want_trust=True)
    assert ext.await_count == 1
    assert "target_features" not in ext.await_args.kwargs
    assert _sim(res) == "koppen"


async def test_hybrid_koppen_empty_chelsa_falls_back_to_v2(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "hybrid")
    ext, res = await _call(dict(_CHELSA), [_MISS, _HIT], want_trust=True)
    assert ext.await_count == 2
    assert "target_features" not in ext.await_args_list[0].kwargs
    kw = ext.await_args_list[1].kwargs
    assert kw["vector_version"] == "v2"
    assert kw["target_features"]["coldest_min"] == -3.0
    assert _sim(res) == "vector_v2_fallback"


async def test_hybrid_koppen_empty_trial_proxy_no_second_call(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "hybrid")
    ext, _ = await _call({**_CHELSA, "source": "trial_proxy"}, [_MISS], want_trust=True)
    assert ext.await_count == 1


async def test_v1_similarity_koppen(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v1")
    ext, res = await _call(dict(_CHELSA), [_HIT], want_trust=True)
    assert ext.await_count == 1 and _sim(res) == "koppen"


async def test_v2_similarity_vector_v2(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v2")
    _, res = await _call(dict(_CHELSA), [_HIT], want_trust=True)
    assert _sim(res) == "vector_v2"


async def test_invalid_mode_behaves_as_v1_and_logs_critical_once(monkeypatch, caplog):
    import logging

    monkeypatch.setattr("app.graph.dao._INVALID_VECTOR_LOGGED", False, raising=False)
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v9")
    with caplog.at_level(logging.CRITICAL, logger="app.graph.dao"):
        ext, res = await _call(dict(_CHELSA), [_HIT], want_trust=True)
        await _call(dict(_CHELSA), [_HIT], want_trust=True)
    assert "target_features" not in ext.await_args.kwargs
    assert _sim(res) == "koppen"
    crit = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(crit) == 1


_INCOMPLETE = {**_CHELSA, "coldest_month_min_c": None}


async def test_hybrid_incomplete_chelsa_vector_no_second_call(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "hybrid")
    ext, _ = await _call(dict(_INCOMPLETE), [_MISS], want_trust=True)
    assert ext.await_count == 1


async def test_hybrid_incomplete_chelsa_vector_similarity_koppen(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "hybrid")
    _, res = await _call(dict(_INCOMPLETE), [_HIT], want_trust=True)
    assert _sim(res) == "koppen"


async def test_v2_incomplete_chelsa_vector_uses_legacy_path(monkeypatch):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "v2")
    ext, res = await _call(dict(_INCOMPLETE), [_HIT], want_trust=True)
    assert "target_features" not in ext.await_args.kwargs
    assert _sim(res) == "koppen"
