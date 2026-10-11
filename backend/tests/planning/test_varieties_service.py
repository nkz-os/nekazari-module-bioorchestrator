from unittest.mock import AsyncMock

import pytest

from app.core.knowledge import KnowledgeSource
from app.planning import service
from app.planning.models import VarietiesResponse

ENV = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "climate_class": "Csa", "country": "ES",
       "centroid": {"lat": None, "lon": None}, "soil": {"data_available": False}, "irrigation": {}, "climate_detail": {}}


async def test_no_recommendation_means_no_trials():
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    dao.recommend_for_conditions.return_value = {"status": "ok", "conditions": {"country": "ES"}, "recommendations": []}
    out = await service.parcel_varieties(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="wheat", regime="secano", management="conventional")
    VarietiesResponse.model_validate(out)
    assert out["status"] == "no_trials" and out["crop"] == "TRZAX"


async def test_ok_lists_stats_and_attributions():
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    dao.recommend_for_conditions.return_value = {
        "status": "ok", "conditions": {"country": "ES"}, "evidence_policy": "p",
        "recommendations": [{"crop": {"eppo": "TRZAX"}, "evidence": {"tier": "regional", "zone_match": None},
                             "trust": {"similarity": "koppen", "data_gaps": ["regional_evidence_only"]}}]}
    dao.get_similar_sites.return_value = [{"name": "Zona X"}]
    rows = [{"variety": v, "site": "Zona X", "year": 2024, "source_id": "GENVCE", "yield_kg_ha": y}
            for v, y in {"A": 6000, "B": 5000, "C": 4000}.items()]
    dao.list_trial_evidence.return_value = {"items": rows, "total": 3}
    out = await service.parcel_varieties(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="TRZAX", regime="secano", management="conventional")
    VarietiesResponse.model_validate(out)
    assert out["status"] == "ok" and out["tier"] == "regional" and out["varieties"][0]["variety"] == "A"
    assert any(a.get("source_id") == "GENVCE" for a in out["attributions"])


def _ok_dao(rows):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    dao.recommend_for_conditions.return_value = {
        "status": "ok", "conditions": {"country": "ES"}, "evidence_policy": "p",
        "recommendations": [{"crop": {"eppo": "TRZAX"}, "evidence": {"tier": "regional", "zone_match": None},
                             "trust": {"similarity": "koppen", "data_gaps": ["regional_evidence_only"]}}]}
    dao.get_similar_sites.return_value = [{"name": "Zona X"}]
    dao.list_trial_evidence.return_value = {"items": rows, "total": len(rows)}
    return dao


async def test_no_environment_with_enough_varieties_is_flagged_but_ok():
    rows = [{"variety": v, "site": "Zona X", "year": 2024, "source_id": "GENVCE", "yield_kg_ha": y}
            for v, y in {"A": 6000, "B": 5000}.items()]
    out = await service.parcel_varieties(dao=_ok_dao(rows), source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="TRZAX", regime="secano", management="conventional")
    VarietiesResponse.model_validate(out)
    assert out["status"] == "ok" and out["varieties"] == [] and out["n_environments"] == 0
    assert out["data_gaps"] == ["regional_evidence_only", "insufficient_environments"]


async def test_environment_without_climate_class_never_asks_the_recommender():
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = {**ENV, "climate_class": None}
    out = await service.parcel_varieties(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="wheat", regime="secano", management="conventional")
    VarietiesResponse.model_validate(out)
    assert out["status"] == "needs_climate"
    dao.recommend_for_conditions.assert_not_called()


def test_unknown_crop_is_its_own_error_type():
    with pytest.raises(service.UnknownCrop):
        service.crop_code("zzz")
    assert issubclass(service.UnknownCrop, ValueError)
