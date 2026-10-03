"""Revenue must convert kg/ha to t/ha before multiplying by a EUR/t price."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.graph.dao import GraphDAO

_REF = {
    "carbon_fixed_tco2e_ha": 0.0,
    "operations_count": 0,
    "n_requirement_kg_ha": 0,
    "n_fixation_kg_ha": 0,
    "growing_season_days": 100,
}
_EXTRAP = {"ranked_varieties": [{"variety": "v1", "mean_yield_kg_ha": 5000.0}]}


def _patches():
    async_none = {"return_value": None}
    return [
        patch.object(GraphDAO, "extrapolate_varieties", AsyncMock(return_value=_EXTRAP)),
        patch.object(GraphDAO, "get_crop_context", AsyncMock(return_value={"error": "n/a"})),
        patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value={"error": "n/a"})),
        patch.object(GraphDAO, "get_soil_suitability", AsyncMock(**async_none)),
        patch.object(GraphDAO, "get_forage_value", AsyncMock(**async_none)),
        patch.object(GraphDAO, "get_market_maturity", AsyncMock(**async_none)),
        patch.object(GraphDAO, "recommend_next_crop", AsyncMock(return_value=[])),
        patch.object(GraphDAO, "get_rotation_constraints", AsyncMock(return_value=[])),
        patch.object(GraphDAO, "get_shared_pests", AsyncMock(return_value={})),
        patch.object(GraphDAO, "_evaluate_pac_compliance", AsyncMock(return_value={})),
        patch("app.services.crop_reference.get_crop_ref", AsyncMock(return_value=dict(_REF))),
    ]


@pytest.mark.parametrize("method", ["compare_crops", "rotation_plan"])
async def test_gross_revenue_uses_tonnes(method):
    dao = GraphDAO(MagicMock())
    patches = _patches()
    for p in patches:
        p.start()
    try:
        if method == "compare_crops":
            out = await dao.compare_crops(
                parcel_id="urn:ngsi-ld:AgriParcel:p1", crops=["TRZAX"],
                seed_price=0, harvest_price=200, operation_cost=0, tenant_id="tenant-a")
            econ = out["comparisons"][0]["economic"]
            assert econ["gross_revenue_eur_ha"] == pytest.approx(1000.0)
        else:
            out = await dao.rotation_plan(
                parcel_id="urn:ngsi-ld:AgriParcel:p1", years=2, starting_crop="TRZAX",
                seed_price=0, harvest_price=200, operation_cost=0, tenant_id="tenant-a")
            assert out["plan"][0]["net_margin_eur_ha"] == pytest.approx(1000.0)
    finally:
        for p in patches:
            p.stop()
