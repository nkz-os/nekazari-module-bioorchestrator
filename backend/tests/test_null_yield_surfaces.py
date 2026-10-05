"""compare_crops and rotation_plan: a crop without eligible numeric evidence has a null yield
(and null revenue, margin and score), never 0; ranked after the crops that have one."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.graph.dao import GraphDAO

_REF = {
    "carbon_fixed_tco2e_ha": 1.0, "operations_count": 0, "n_requirement_kg_ha": 0,
    "n_fixation_kg_ha": 0, "growing_season_days": 100,
}
_WITH = {"ranked_varieties": [{"variety": "v1", "mean_yield_kg_ha": 5000.0}]}
# a crop whose only evidence is BSL / forage / regional: the ranked list has a variety, no number
_WITHOUT = {"ranked_varieties": [{"variety": "v2", "mean_yield_kg_ha": None, "trial_count": 3}]}
_NOTHING = {"ranked_varieties": []}


def _extrapolate(by_crop):
    async def fn(self_, crop, **kw):
        return by_crop[crop]
    return fn


def _patches(by_crop):
    return [
        patch.object(GraphDAO, "extrapolate_varieties", _extrapolate(by_crop)),
        patch.object(GraphDAO, "get_crop_context", AsyncMock(return_value={"error": "n/a"})),
        patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value={"error": "n/a"})),
        patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)),
        patch.object(GraphDAO, "get_forage_value", AsyncMock(return_value=None)),
        patch.object(GraphDAO, "get_market_maturity", AsyncMock(return_value=None)),
        patch.object(GraphDAO, "recommend_next_crop", AsyncMock(return_value=[])),
        patch.object(GraphDAO, "get_rotation_constraints", AsyncMock(return_value=[])),
        patch.object(GraphDAO, "get_shared_pests", AsyncMock(return_value={})),
        patch.object(GraphDAO, "_evaluate_pac_compliance", AsyncMock(return_value={})),
        patch("app.services.crop_reference.get_crop_ref", AsyncMock(return_value=dict(_REF))),
    ]


@pytest.fixture
def patched():
    started = []

    def start(by_crop):
        for p in _patches(by_crop):
            p.start()
            started.append(p)
    yield start
    for p in started:
        p.stop()


async def test_compare_crops_null_yield_margin_and_score_without_evidence(patched):
    patched({"TRZAX": _WITH, "BSLONLY": _WITHOUT, "NODATA": _NOTHING})
    out = await GraphDAO(MagicMock()).compare_crops(
        parcel_id="urn:ngsi-ld:AgriParcel:p1", crops=["BSLONLY", "TRZAX", "NODATA"],
        seed_price=0, harvest_price=200, operation_cost=0, tenant_id="t")
    rows = {c["crop"]: c for c in out["comparisons"]}
    assert rows["TRZAX"]["agronomics"]["expected_yield_kg_ha"] == 5000.0
    assert rows["TRZAX"]["economic"]["net_margin_eur_ha"] == pytest.approx(1000.0)
    assert rows["TRZAX"]["composite_score"] is not None and "data_gaps" not in rows["TRZAX"]
    for crop in ("BSLONLY", "NODATA"):
        c = rows[crop]
        assert c["agronomics"]["expected_yield_kg_ha"] is None
        assert c["economic"]["gross_revenue_eur_ha"] is None and c["economic"]["net_margin_eur_ha"] is None
        assert c["composite_score"] is None and c["data_gaps"] == ["no_measured_yield"]
    # the crop with a number ranks first; the others follow, in request order, never as a zero
    assert out["ranking"]["by_margin"] == ["TRZAX", "BSLONLY", "NODATA"]
    assert out["ranking"]["by_score"] == ["TRZAX", "BSLONLY", "NODATA"]
    assert sorted(out["ranking"]["by_carbon"]) == ["BSLONLY", "NODATA", "TRZAX"]


async def test_compare_crops_with_no_crop_having_evidence_still_answers(patched):
    patched({"A": _WITHOUT, "B": _NOTHING})
    out = await GraphDAO(MagicMock()).compare_crops(
        parcel_id="urn:ngsi-ld:AgriParcel:p1", crops=["A", "B"], tenant_id="t")
    assert all(c["composite_score"] is None for c in out["comparisons"])
    assert out["ranking"]["by_score"] == ["A", "B"]


async def test_rotation_plan_null_yield_and_cumulative_over_the_years_with_evidence(patched):
    patched({"TRZAX": _WITH, "BSLONLY": _WITHOUT})
    dao = GraphDAO(MagicMock())
    with patch.object(GraphDAO, "recommend_next_crop", AsyncMock(return_value=[{"crop_eppo": "BSLONLY"}])):
        out = await dao.rotation_plan(
            parcel_id="urn:ngsi-ld:AgriParcel:p1", years=2, starting_crop="TRZAX",
            seed_price=0, harvest_price=200, operation_cost=0, tenant_id="t")
    first, second = out["plan"]
    assert first["expected_yield_kg_ha"] == 5000.0 and first["net_margin_eur_ha"] == pytest.approx(1000.0)
    assert "data_gaps" not in first
    assert second["crop"] == "BSLONLY"
    assert second["expected_yield_kg_ha"] is None and second["net_margin_eur_ha"] is None
    assert second["data_gaps"] == ["no_measured_yield"]
    cum = out["cumulative"]
    assert cum["years_with_yield"] == 1
    assert cum["total_yield_kg_ha"] == 5000.0 and cum["total_net_margin_eur_ha"] == pytest.approx(1000.0)


async def test_rotation_plan_totals_are_null_when_no_year_has_evidence(patched):
    patched({"BSLONLY": _WITHOUT})
    out = await GraphDAO(MagicMock()).rotation_plan(
        parcel_id="urn:ngsi-ld:AgriParcel:p1", years=2, starting_crop="BSLONLY", tenant_id="t")
    cum = out["cumulative"]
    assert cum["years_with_yield"] == 0
    assert cum["total_yield_kg_ha"] is None and cum["total_net_margin_eur_ha"] is None
    assert all(e["expected_yield_kg_ha"] is None for e in out["plan"])
