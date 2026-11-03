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
                                         tenant_id="t", crop="wheat", regime="rainfed", management="conventional")
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
                                         tenant_id="t", crop="TRZAX", regime="rainfed", management="conventional")
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
                                         tenant_id="t", crop="TRZAX", regime="rainfed", management="conventional")
    VarietiesResponse.model_validate(out)
    assert out["status"] == "ok" and out["varieties"] == [] and out["n_environments"] == 0
    assert out["data_gaps"] == ["regional_evidence_only", "insufficient_environments"]


async def test_environment_without_climate_class_never_asks_the_recommender():
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = {**ENV, "climate_class": None}
    out = await service.parcel_varieties(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="wheat", regime="rainfed", management="conventional")
    VarietiesResponse.model_validate(out)
    assert out["status"] == "needs_climate"
    dao.recommend_for_conditions.assert_not_called()


def test_unknown_crop_is_its_own_error_type():
    with pytest.raises(service.UnknownCrop):
        service.crop_code("zzz")
    assert issubclass(service.UnknownCrop, ValueError)


# --- evidence depth (additive) ----------------------------------------------------------------

def _rows_grid(n_sites, years, n_varieties=6, source="GENVCE-TRIALS"):
    rows = []
    for s in range(n_sites):
        for y in years:
            for v in range(n_varieties):
                rows.append({"variety": f"V{v}", "site": f"S{s}", "year": y, "source_id": source,
                             "yield_kg_ha": 5000 + 100 * v})
    return rows


async def _depth(rows, total=None):
    dao = _ok_dao(rows)
    if total is not None:
        dao.list_trial_evidence.return_value = {"items": rows, "total": total}
    out = await service.parcel_varieties(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="TRZAX", regime="rainfed", management="conventional")
    VarietiesResponse.model_validate(out)
    return out["evidence_depth"]


async def test_evidence_depth_is_computed_from_the_rows_behind_the_table():
    deep = await _depth(_rows_grid(60, range(2006, 2026), n_varieties=2))  # 60 sites x 20 years x 2 = 2400 rows
    assert deep == {"level": "deep", "n_trials": 2400, "n_sites": 60, "n_years": 20, "first_year": 2006, "last_year": 2025}
    moderate = await _depth(_rows_grid(5, range(2014, 2026), n_varieties=6))  # 5 x 12 x 6 = 360 rows, only 5 sites
    assert moderate["level"] == "moderate" and moderate["n_trials"] == 360
    pilot = await _depth(_rows_grid(2, range(2025, 2027), n_varieties=6))  # 24 rows, 2 sites
    assert pilot["level"] == "pilot" and (pilot["n_sites"], pilot["n_years"]) == (2, 2)


async def test_a_multi_site_network_of_a_few_campaigns_is_broad_not_pilot():
    # The field tier of the production graph (2026-11): 17 sites, 4 campaigns, ~730 trials.
    broad = await _depth(_rows_grid(17, range(2023, 2027), n_varieties=11))  # 17 x 4 x 11 = 748 rows
    assert broad["level"] == "broad"


async def test_every_depth_level_needs_sites_campaigns_and_trials_together():
    assert (await _depth(_rows_grid(10, range(2024, 2027), n_varieties=12)))["level"] == "broad"  # 10 x 3 x 12
    assert (await _depth(_rows_grid(10, range(2026, 2027), n_varieties=40)))["level"] == "pilot"  # one campaign
    assert (await _depth(_rows_grid(3, range(2023, 2027), n_varieties=6)))["level"] == "moderate"  # 3 x 4 x 6 = 72
    assert (await _depth(_rows_grid(1, range(2006, 2026), n_varieties=10)))["level"] == "pilot"  # one site, 200 rows


async def test_evidence_depth_counts_the_trials_behind_the_page_cap_not_the_page():
    depth = await _depth(_rows_grid(3, range(2023, 2027), n_varieties=6), total=7200)
    assert depth["n_trials"] == 7200


async def test_evidence_depth_uses_the_summary_behind_the_page_cap_when_the_dao_gives_one():
    rows = _rows_grid(3, range(2023, 2027), n_varieties=6)
    dao = _ok_dao(rows)
    dao.list_trial_evidence.return_value = {"items": rows, "total": 7200,
                                            "summary": {"n_sites": 86, "n_years": 21, "first_year": 2006, "last_year": 2026}}
    out = await service.parcel_varieties(dao=dao, source=KnowledgeSource("global", None), parcel_id="p1",
                                         tenant_id="t", crop="TRZAX", regime="rainfed", management="conventional")
    assert out["evidence_depth"] == {"level": "deep", "n_trials": 7200, "n_sites": 86, "n_years": 21,
                                     "first_year": 2006, "last_year": 2026}
