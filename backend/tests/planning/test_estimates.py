from datetime import date
from unittest.mock import AsyncMock

import pytest

from app.core.knowledge import KnowledgeSource
from app.planning import service
from app.planning.estimates import (
    aquacrop_status,
    disagreement,
    percentiles,
    trial_estimate,
    visible,
)
from app.planning.models import EstimatesResponse


def test_percentiles_of_one_and_many():
    assert percentiles([]) is None
    assert percentiles([4.0]) == {"p10": 4.0, "p50": 4.0, "p90": 4.0}
    p = percentiles([float(x) for x in range(1, 12)])
    assert p["p50"] == 6.0 and p["p10"] < p["p50"] < p["p90"]


def test_trial_estimate_in_tonnes_filtered_by_variety():
    rows = [{"variety": "A", "yield_kg_ha": 6000}, {"variety": "A", "yield_kg_ha": 7000}, {"variety": "B", "yield_kg_ha": 1000}]
    est = trial_estimate(rows, "A", [], "global", "2026-10-08.1")
    assert est["basis"] == "trial_evidence" and est["n"] == 2 and est["percentiles"]["p50"] == 6.5
    assert est["validation"] == {"status": "not_applicable"}
    assert trial_estimate(rows, "Z", [], "global", "p") is None


def test_disagreement_only_between_same_basis_models():
    def e(basis, p50):
        return {"basis": basis, "percentiles": {"p10": p50, "p50": p50, "p90": p50}, "validation": {"status": "experimental"}}
    assert disagreement([e("trial_evidence", 8.0), e("water_limited", 3.0)]) is False
    assert disagreement([e("water_limited", 8.0), e("water_limited", 3.0)]) is True


def test_unvalidated_is_hidden_and_aquacrop_is_honest_in_v1():
    assert visible([{"validation": {"status": "unvalidated"}}]) == []
    assert aquacrop_status() == {"provider_id": "aquacrop", "reason": "preseason_not_supported"}


ENV = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "climate_class": "Csa", "country": "ES",
       "centroid": {"lat": None, "lon": None}, "soil": {"data_available": False}, "irrigation": {}, "climate_detail": {}}
AQUACROP_UNAVAILABLE = {"provider_id": "aquacrop", "reason": "preseason_not_supported"}


def _dao(rows, recommendations=None):
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    dao.recommend_for_conditions.return_value = {
        "status": "ok", "conditions": {"country": "ES"}, "evidence_policy": "p",
        "recommendations": [{"crop": {"eppo": "TRZAX"}, "evidence": {"tier": "regional", "zone_match": None},
                             "trust": {"similarity": "koppen", "data_gaps": ["regional_evidence_only"]}}]
        if recommendations is None else recommendations}
    dao.get_similar_sites.return_value = [{"name": "Zona X"}]
    dao.list_trial_evidence.return_value = {"items": rows, "total": len(rows)}
    return dao


async def test_parcel_estimates_from_trials_is_validated_and_declares_aquacrop():
    rows = [{"variety": v, "site": "Zona X", "year": 2024, "source_id": "GENVCE", "yield_kg_ha": y}
            for v, y in [("A", 6000), ("A", 7000), ("B", 1000)]]
    out = await service.parcel_estimates(dao=_dao(rows), source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="wheat", variety="A", regime="secano",
                                         management="conventional", sowing_date=date(2027, 3, 1))
    EstimatesResponse.model_validate(out)
    assert out["status"] == "ok" and out["crop"] == "TRZAX" and out["variety"] == "A"
    assert out["unavailable"] == [AQUACROP_UNAVAILABLE] and out["disagreement"] is False
    assert out["inputs"]["sowing_source"] == "planned" and out["inputs"]["sowing_date"] == date(2027, 3, 1)
    (est,) = out["estimates"]
    assert est["basis"] == "trial_evidence" and est["percentiles"]["p50"] == 6.5 and est["n"] == 2
    assert est["warnings"] == ["regional_evidence_only"]
    assert any(a.get("source_id") == "GENVCE" for a in est["attributions"])


async def test_parcel_estimates_without_trials_is_no_trials_and_still_declares_aquacrop():
    out = await service.parcel_estimates(dao=_dao([], recommendations=[]), source=KnowledgeSource("global", None),
                                         parcel_id="p1", tenant_id="t", crop="TRZAX", variety=None,
                                         regime="secano", management="conventional", sowing_date=None)
    EstimatesResponse.model_validate(out)
    assert out["status"] == "no_trials" and out["estimates"] == [] and out["disagreement"] is False
    assert out["unavailable"] == [AQUACROP_UNAVAILABLE]
    assert out["inputs"]["sowing_source"] == "none"


async def test_unknown_crop_never_reaches_the_parcel():
    dao = _dao([])
    with pytest.raises(service.UnknownCrop):
        await service.parcel_estimates(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                       tenant_id="t", crop="zzz", variety=None, regime="secano",
                                       management="conventional", sowing_date=None)
    dao.get_parcel_environment.assert_not_called()
