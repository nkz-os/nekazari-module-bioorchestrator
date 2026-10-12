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


def _est(basis, p50):
    return {"basis": basis, "percentiles": {"p10": p50, "p50": p50, "p90": p50}, "validation": {"status": "experimental"}}


def test_disagreement_only_between_same_basis_models():
    assert disagreement([_est("trial_evidence", 8.0), _est("water_limited", 3.0)]) is False
    assert disagreement([_est("water_limited", 8.0), _est("water_limited", 3.0)]) is True


def test_disagreement_is_flagged_against_a_zero_estimate():
    assert disagreement([_est("water_limited", 0.0), _est("water_limited", 5.0)]) is True


def test_bool_and_non_finite_yields_never_count():
    rows = [{"variety": "A", "yield_kg_ha": 6000}, {"variety": "A", "yield_kg_ha": True},
            {"variety": "A", "yield_kg_ha": float("inf")}, {"variety": "A", "yield_kg_ha": float("nan")}]
    est = trial_estimate(rows, None, [], "global", "p")
    assert est["n"] == 1 and est["percentiles"]["p50"] == 6.0


def test_unvalidated_is_hidden_and_aquacrop_lists_async_or_its_gate_reason():
    assert visible([{"validation": {"status": "unvalidated"}}]) == []
    assert aquacrop_status(None) == {"provider_id": "aquacrop", "reason": "async"}
    assert aquacrop_status("not_validated") == {"provider_id": "aquacrop", "reason": "not_validated"}


ENV = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "climate_class": "Csa", "country": "ES",
       "centroid": {"lat": None, "lon": None}, "soil": {"data_available": False}, "irrigation": {}, "climate_detail": {}}
AQUACROP_ASYNC = {"provider_id": "aquacrop", "reason": "async"}
NEEDS_DATE = {"provider_id": "aquacrop", "reason": "needs_sowing_date"}
TODAY = date(2026, 10, 12)


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


def est_moisture(out):
    return out["estimates"][0]["moisture"]


async def test_parcel_estimates_from_trials_is_validated_and_declares_aquacrop():
    rows = [{"variety": v, "site": "Zona X", "year": 2024, "source_id": "GENVCE", "yield_kg_ha": y}
            for v, y in [("A", 6000), ("A", 7000), ("B", 1000)]]
    out = await service.parcel_estimates(dao=_dao(rows), source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="wheat", variety="A", regime="rainfed",
                                         management="conventional", sowing_date=date(2027, 3, 1), today=TODAY)
    EstimatesResponse.model_validate(out)
    assert out["status"] == "ok" and out["crop"] == "TRZAX" and out["variety"] == "A"
    assert out["unavailable"] == [AQUACROP_ASYNC] and out["disagreement"] is False
    assert est_moisture(out) == {"basis": "as_reported", "pct": None}
    assert out["inputs"]["sowing_source"] == "planned" and out["inputs"]["sowing_date"] == date(2027, 3, 1)
    (est,) = out["estimates"]
    assert est["basis"] == "trial_evidence" and est["percentiles"]["p50"] == 6.5 and est["n"] == 2
    assert est["warnings"] == ["regional_evidence_only"]
    assert any(a.get("source_id") == "GENVCE" for a in est["attributions"])


async def test_parcel_estimates_without_trials_is_no_trials_and_still_declares_aquacrop():
    out = await service.parcel_estimates(dao=_dao([], recommendations=[]), source=KnowledgeSource("global", None),
                                         parcel_id="p1", tenant_id="t", crop="TRZAX", variety=None,
                                         regime="rainfed", management="conventional", sowing_date=None)
    EstimatesResponse.model_validate(out)
    assert out["status"] == "no_trials" and out["estimates"] == [] and out["disagreement"] is False
    assert out["unavailable"] == [NEEDS_DATE, {"provider_id": "trial_evidence", "reason": "no_trials"}]
    assert out["inputs"]["sowing_source"] == "none"


async def test_variety_with_no_rows_says_why():
    rows = [{"variety": "A", "site": "Zona X", "year": 2024, "source_id": "GENVCE", "yield_kg_ha": 6000}]
    out = await service.parcel_estimates(dao=_dao(rows), source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="TRZAX", variety="Z", regime="rainfed",
                                         management="conventional", sowing_date=None)
    EstimatesResponse.model_validate(out)
    assert out["status"] == "ok" and out["estimates"] == []
    assert out["unavailable"] == [NEEDS_DATE,
                                  {"provider_id": "trial_evidence", "reason": "no_trials_for_variety"}]


async def test_truncated_evidence_and_variety_absent_says_so():
    rows = [{"variety": "A", "site": "Zona X", "year": 2024, "source_id": "GENVCE", "yield_kg_ha": 6000}]
    dao = _dao(rows)
    dao.list_trial_evidence.return_value = {"items": rows, "total": len(rows) + 1}
    out = await service.parcel_estimates(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="TRZAX", variety="Z", regime="rainfed",
                                         management="conventional", sowing_date=None)
    EstimatesResponse.model_validate(out)
    assert out["estimates"] == []
    assert out["unavailable"][1] == {"provider_id": "trial_evidence", "reason": "variety_not_in_evidence_sample"}


async def test_attributions_credit_only_the_rows_behind_the_number():
    rows = [{"variety": "A", "site": "Zona X", "year": 2024, "source_id": "GENVCE", "yield_kg_ha": 6000},
            {"variety": "B", "site": "Zona X", "year": 2024, "source_id": "CREA", "yield_kg_ha": 5000}]

    both =await service.parcel_estimates(dao=_dao(rows), source=KnowledgeSource("global", None), parcel_id="p1",
                                          tenant_id="t", crop="TRZAX", variety=None, regime="rainfed",
                                          management="conventional", sowing_date=None)
    assert {a["source_id"] for a in both["estimates"][0]["attributions"]} == {"GENVCE", "CREA"}
    only_a = await service.parcel_estimates(dao=_dao(rows), source=KnowledgeSource("global", None), parcel_id="p1",
                                            tenant_id="t", crop="TRZAX", variety="A", regime="rainfed",
                                            management="conventional", sowing_date=None)
    assert {a["source_id"] for a in only_a["estimates"][0]["attributions"]} == {"GENVCE"}


async def test_unknown_crop_never_reaches_the_parcel():
    dao = _dao([])
    with pytest.raises(service.UnknownCrop):
        await service.parcel_estimates(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                       tenant_id="t", crop="zzz", variety=None, regime="rainfed",
                                       management="conventional", sowing_date=None)
    dao.get_parcel_environment.assert_not_called()
