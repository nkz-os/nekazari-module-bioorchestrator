"""Mandatory source attribution: the ``attributions`` of the API responses.

Each endpoint lists the attributions of the sources actually present in its response; the
public listing exposes every attribution. Registry fields and helper: ``test_source_attribution``.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.api.v1.attribution import (
    attach_attributions,
    recommendation_source_ids,
    rows_source_ids,
)
from app.auth import SKIP_AUTH_PREFIXES
from app.auth_policy import requires_identity
from app.core.dependencies import get_neo4j_driver
from app.graph.dao import GraphDAO
from app.main import app

GENVCE_TEXT = (
    "Fuente: Datos Abiertos GENVCE. Url: https://genvce.org/mapa-de-resultados/ "
    "(Descarga: 01/06/2026.)"
)
URN = "urn:ngsi-ld:AgriParcel:p1"
_ITEM_KEYS = {"source_id", "text", "url", "licence_id", "licence_url", "processing_note"}


def test_attach_attributions_returns_a_new_dict_and_always_sets_the_key():
    payload = {"x": 1}
    out = attach_attributions(payload, ["CREA"])
    assert "attributions" not in payload and out["x"] == 1
    assert [a["source_id"] for a in out["attributions"]] == ["CREA"]
    assert attach_attributions(payload, [])["attributions"] == []


def test_source_id_collectors():
    result = {"recommendations": [
        {"evidence": {"sources": ["GENVCE", "CREA"]}},
        {"evidence": {"sources": ["GENVCE"]}},
        {"evidence": {}},
        {},
    ]}
    assert recommendation_source_ids(result) == {"GENVCE", "CREA"}
    assert recommendation_source_ids({}) == set()
    assert rows_source_ids([{"source_id": "CREA"}, {"source_id": None}, {}]) == {"CREA"}
    assert rows_source_ids(None) == set()


# ── endpoints ───────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def client():
    mp = pytest.MonkeyPatch()
    mp.setenv("AUTH_DISABLED", "true")
    app.dependency_overrides[get_neo4j_driver] = lambda: MagicMock()
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_neo4j_driver, None)
        mp.undo()


def _rec(crop, sources):
    return {"crop": {"eppo": crop}, "evidence": {"sources": sources}}


def _ids(body):
    return [a["source_id"] for a in body["attributions"]]


def test_recommend_conditions_lists_attributions_of_sources_in_the_recommendations(client):
    cached = {"status": "ok", "recommendations": [_rec("ZEAMX", ["CREA"]), _rec("TRZAX", ["GENVCE"])]}
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=cached)):
        body = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb"}).json()
    assert _ids(body) == ["CREA", "GENVCE"]
    assert body["recommendations"] == cached["recommendations"]
    assert "attributions" not in cached  # a cached DAO result is never mutated


def test_recommend_conditions_only_the_sources_present(client):
    result = {"status": "ok", "recommendations": [_rec("TRZAX", ["GENVCE"])]}
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=result)):
        body = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb"}).json()
    assert _ids(body) == ["GENVCE"]
    assert set(body["attributions"][0]) == _ITEM_KEYS


def test_recommend_conditions_without_recommendations_has_an_empty_list(client):
    result = {"status": "ok", "recommendations": []}
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=result)):
        body = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb"}).json()
    assert body["attributions"] == []


def test_recommend_parcel_lists_attributions(client):
    env = {
        "climate_class": "Csa", "soil": {"data_available": False}, "irrigation": {"inferred": None},
        "climate_detail": None, "inputs_used": {},
    }
    result = {"status": "ok", "recommendations": [_rec("TRZAX", ["GENVCE"])]}
    with patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value=env)), \
         patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=result)), \
         patch("app.api.v1.recommend._require_tenant_id", return_value="t"):
        body = client.get(f"/api/graph/recommend/parcel/{URN}").json()
    assert _ids(body) == ["GENVCE"]
    assert body["parcel_environment"]["climate_class"] == "Csa"
    assert body["recommendations"] == result["recommendations"]


def test_recommend_evidence_lists_attributions_of_the_page_items(client):
    page = {"items": [{"source_id": "CREA"}, {"source_id": "CREA"}], "total": 2, "page": 1,
            "page_size": 20, "purpose": "main", "tier": "field"}
    with patch.object(GraphDAO, "get_similar_sites", AsyncMock(return_value=[{"name": "S"}])), \
         patch.object(GraphDAO, "list_trial_evidence", AsyncMock(return_value=page)):
        body = client.get("/api/graph/agriculture/recommend/evidence",
                          params={"climate_class": "Cfb", "crop": "ZEAMX"}).json()
    assert _ids(body) == ["CREA"]
    assert body["items"] == page["items"] and body["total"] == 2


def test_variety_trials_lists_attributions_of_the_trials(client):
    trials = [{"variety": "A", "source_id": "GENVCE"}, {"variety": "B", "source_id": "CREA"},
              {"variety": "C", "source_id": "GENVCE"}]
    with patch.object(GraphDAO, "get_variety_trials", AsyncMock(return_value=trials)):
        body = client.get("/api/graph/agriculture/variety-trials", params={"crop": "ZEAMX"}).json()
    assert _ids(body) == ["CREA", "GENVCE"]
    assert body["total"] == 3 and body["trials"] == trials


def test_variety_trials_without_trials_has_an_empty_list(client):
    with patch.object(GraphDAO, "get_variety_trials", AsyncMock(return_value=[])):
        body = client.get("/api/graph/agriculture/variety-trials", params={"crop": "ZEAMX"}).json()
    assert body["attributions"] == []


def test_yield_potential_lists_attributions_of_its_sources(client):
    result = {"variety": "V", "crop": "TRZAX", "expected_yield_kg_ha": 7000.0, "source_ids": ["GENVCE"]}
    with patch.object(GraphDAO, "get_yield_potential", AsyncMock(return_value=result)):
        body = client.get("/api/graph/agriculture/yield-potential",
                          params={"variety": "V", "crop": "TRZAX"}).json()
    assert _ids(body) == ["GENVCE"]
    assert body["expected_yield_kg_ha"] == 7000.0


def test_yield_potential_unknown_variety_has_no_attribution_and_still_404s_on_error(client):
    result = {"variety": "V", "crop": "TRZAX", "expected_yield_kg_ha": None, "source_ids": []}
    with patch.object(GraphDAO, "get_yield_potential", AsyncMock(return_value=result)):
        assert client.get("/api/graph/agriculture/yield-potential",
                          params={"variety": "V", "crop": "TRZAX"}).json()["attributions"] == []
    with patch.object(GraphDAO, "get_yield_potential", AsyncMock(return_value={"error": "x"})):
        assert client.get("/api/graph/agriculture/yield-potential",
                          params={"variety": "V", "crop": "TRZAX"}).status_code == 404


def test_compare_crops_lists_attributions_of_the_compared_crops(client):
    result = {"parcel_id": URN, "comparisons": [
        {"crop": "ZEAMX", "source_ids": ["CREA"]},
        {"crop": "TRZAX", "source_ids": ["GENVCE", "CREA"]},
        {"crop": "NODATA", "source_ids": []},
    ], "ranking": {}}
    with patch.object(GraphDAO, "compare_crops", AsyncMock(return_value=result)), \
         patch("app.api.v1.graph._require_tenant_id", return_value="t"):
        body = client.get("/api/graph/agriculture/compare-crops",
                          params={"parcel_id": URN, "crops": "ZEAMX,TRZAX,NODATA"}).json()
    assert _ids(body) == ["CREA", "GENVCE"]
    assert [c["crop"] for c in body["comparisons"]] == ["ZEAMX", "TRZAX", "NODATA"]


# ── round 2: the other surfaces that show source data ──────────────────────

_EXTRAPOLATED = {
    "target_environment": {}, "similar_sites": ["Lleida", "Bergamo"],
    "ranked_varieties": [
        {"variety": "A", "source_ids": ["GENVCE"]}, {"variety": "B", "source_ids": ["GENVCE"]},
    ],
}


def test_extrapolate_credits_only_the_sources_behind_the_ranked_varieties(client):
    """A wheat query whose analog sites include a CREA maize site must not credit CREA."""
    wheat = {**_EXTRAPOLATED, "similar_sites": ["Lleida", "Villafranca Piemonte (TO)"]}
    sites = AsyncMock(return_value={"Lleida": ["GENVCE"], "Villafranca Piemonte (TO)": ["CREA"]})
    with patch.object(GraphDAO, "extrapolate_varieties", AsyncMock(return_value=wheat)), \
         patch.object(GraphDAO, "get_site_source_ids", sites):
        body = client.get("/api/graph/agriculture/extrapolate", params={"crop": "TRZAX"}).json()
    assert _ids(body) == ["GENVCE"]
    assert "CREA" not in json.dumps(body["attributions"])
    assert body["similar_sites"] == ["Lleida", "Villafranca Piemonte (TO)"]
    sites.assert_not_called()  # the site sources play no part in the credit


def test_extrapolate_without_any_source_has_an_empty_list(client):
    empty = {"target_environment": {}, "similar_sites": [], "ranked_varieties": []}
    with patch.object(GraphDAO, "extrapolate_varieties", AsyncMock(return_value=empty)), \
         patch.object(GraphDAO, "get_site_source_ids", AsyncMock(return_value={})):
        body = client.get("/api/graph/agriculture/extrapolate", params={"crop": "ZEAMX"}).json()
    assert body["attributions"] == []


def test_similar_sites_lists_the_sources_of_each_site(client):
    sites = [{"name": "Lleida"}, {"name": "Bergamo"}, {"name": "Nowhere"}]
    with patch.object(GraphDAO, "get_similar_sites", AsyncMock(return_value=sites)), \
         patch.object(GraphDAO, "get_site_source_ids",
                      AsyncMock(return_value={"Lleida": ["GENVCE"], "Bergamo": ["CREA", "GENVCE"]})):
        body = client.get("/api/graph/agriculture/similar-sites", params={"climate_class": "Csa"}).json()
    assert [(s["name"], s["source_ids"]) for s in body["sites"]] == [
        ("Lleida", ["GENVCE"]), ("Bergamo", ["CREA", "GENVCE"]), ("Nowhere", [])]
    assert _ids(body) == ["CREA", "GENVCE"]
    assert body["total"] == 3
    assert "attributions" not in sites[0] and "source_ids" not in sites[0]  # the DAO rows are not mutated


def test_trial_sites_lists_attributions(client):
    sites = [{"name": "Lleida", "source_ids": ["GENVCE"]}, {"name": "Orphan", "source_ids": []}]
    with patch.object(GraphDAO, "get_trial_sites_summary", AsyncMock(return_value=sites)):
        body = client.get("/api/graph/agriculture/trial-sites").json()
    assert _ids(body) == ["GENVCE"] and body["total"] == 2


def test_crops_lists_attributions(client):
    crops = [{"eppo_code": "ZEAMX", "source_ids": ["CREA", "GENVCE"]}, {"eppo_code": "TRZAX", "source_ids": ["GENVCE"]}]
    with patch.object(GraphDAO, "get_available_crops", AsyncMock(return_value=crops)):
        body = client.get("/api/graph/agriculture/crops").json()
    assert _ids(body) == ["CREA", "GENVCE"] and body["total"] == 2


def test_graph_stats_lists_attributions_of_the_sources_in_the_graph(client):
    stats = {"trials": 3, "source_ids": ["GENVCE", "UNREGISTERED"]}
    with patch.object(GraphDAO, "graph_quality_stats", AsyncMock(return_value=stats)):
        body = client.get("/api/graph/agriculture/graph-stats").json()
    assert body["trials"] == 3 and _ids(body) == ["GENVCE"]  # a source without a credit is left out


def test_regenerative_sequence_lists_attributions_of_its_variety_ranking(client):
    result = {"variety_trials": [{"variety": "V", "source_ids": ["GENVCE"]}, {"variety": "W", "source_ids": ["CREA"]}]}
    with patch.object(GraphDAO, "get_regenerative_sequence", AsyncMock(return_value=result)):
        body = client.get("/api/graph/agriculture/regenerative-sequence", params={"climate_class": "Csa"}).json()
    assert _ids(body) == ["CREA", "GENVCE"]


def test_regenerative_sequence_without_a_ranking_has_an_empty_list(client):
    with patch.object(GraphDAO, "get_regenerative_sequence", AsyncMock(return_value={"variety_trials": []})):
        body = client.get("/api/graph/agriculture/regenerative-sequence", params={"climate_class": "Csa"}).json()
    assert body["attributions"] == []


def test_yield_projection_lists_attributions_of_its_potential_yield(client):
    result = {"parcel_id": URN, "potential_yield_kg_ha": 7000.0, "source_ids": ["GENVCE"]}
    with patch.object(GraphDAO, "get_yield_projection", AsyncMock(return_value=result)), \
         patch("app.api.v1.graph._require_tenant_id", return_value="t"):
        body = client.get("/api/graph/agriculture/yield-projection", params={"parcel_id": URN}).json()
    assert _ids(body) == ["GENVCE"] and body["potential_yield_kg_ha"] == 7000.0


def test_yield_projection_with_a_caller_supplied_yield_has_no_attribution(client):
    result = {"parcel_id": URN, "potential_yield_kg_ha": 5000.0, "source_ids": []}
    with patch.object(GraphDAO, "get_yield_projection", AsyncMock(return_value=result)), \
         patch("app.api.v1.graph._require_tenant_id", return_value="t"):
        body = client.get("/api/graph/agriculture/yield-projection",
                          params={"parcel_id": URN, "initial_yield_kg_ha": 5000}).json()
    assert body["attributions"] == []


def test_rotation_plan_lists_attributions_of_the_years_varieties(client):
    result = {"parcel_id": URN, "plan": [
        {"year": 1, "crop": "ZEAMX", "source_ids": ["CREA"]},
        {"year": 2, "crop": "TRZAX", "source_ids": ["GENVCE"]},
        {"year": 3, "crop": "NODATA", "source_ids": []},
    ]}
    with patch.object(GraphDAO, "rotation_plan", AsyncMock(return_value=result)), \
         patch("app.api.v1.graph._require_tenant_id", return_value="t"):
        body = client.get("/api/graph/agriculture/rotation-plan", params={"parcel_id": URN}).json()
    assert _ids(body) == ["CREA", "GENVCE"]
    assert [e["crop"] for e in body["plan"]] == ["ZEAMX", "TRZAX", "NODATA"]


# ── public listing ──────────────────────────────────────────────────────────

_LISTING = "/api/graph/agriculture/sources/attributions"


def test_attributions_listing_is_public_and_get_only(client):
    assert any(_LISTING.startswith(p) for p in SKIP_AUTH_PREFIXES)
    assert requires_identity(_LISTING, "GET", {}) is False
    r = client.get(_LISTING)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == len(body["attributions"]) >= 2
    by_id = {a["source_id"]: a for a in body["attributions"]}
    assert by_id["GENVCE"]["text"] == GENVCE_TEXT
    assert by_id["GENVCE"]["download_date"] == "2026-06-01"
    assert "CC BY 3.0" in by_id["CREA"]["text"]
    for method in ("post", "put", "patch", "delete"):
        assert getattr(client, method)(_LISTING).status_code == 405


def test_attributions_listing_needs_no_identity_with_auth_enabled(client, monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    assert client.get(_LISTING).status_code == 200


# ── DAO ─────────────────────────────────────────────────────────────────────

_REF = {
    "carbon_fixed_tco2e_ha": 1.0, "operations_count": 0, "n_requirement_kg_ha": 0,
    "n_fixation_kg_ha": 0, "growing_season_days": 100,
}


async def test_compare_crops_entries_carry_the_sources_of_their_best_variety():
    by_crop = {
        "TRZAX": {"ranked_varieties": [{"variety": "v", "mean_yield_kg_ha": 5000.0,
                                        "source_ids": ["GENVCE", "CREA"]}]},
        "NODATA": {"ranked_varieties": []},
    }

    async def extrapolate(self_, crop, **kw):
        return by_crop[crop]

    patches = [
        patch.object(GraphDAO, "extrapolate_varieties", extrapolate),
        patch.object(GraphDAO, "get_crop_context", AsyncMock(return_value={"error": "n/a"})),
        patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)),
        patch.object(GraphDAO, "get_forage_value", AsyncMock(return_value=None)),
        patch.object(GraphDAO, "get_market_maturity", AsyncMock(return_value=None)),
        patch("app.services.crop_reference.get_crop_ref", AsyncMock(return_value=dict(_REF))),
    ]
    for p in patches:
        p.start()
    try:
        out = await GraphDAO(MagicMock()).compare_crops(parcel_id=URN, crops=["TRZAX", "NODATA"], tenant_id="t")
    finally:
        for p in patches:
            p.stop()
    rows = {c["crop"]: c for c in out["comparisons"]}
    assert rows["TRZAX"]["source_ids"] == ["CREA", "GENVCE"]
    assert rows["NODATA"]["source_ids"] == []


async def test_rotation_plan_entries_carry_the_sources_of_their_best_variety():
    by_crop = {
        "ZEAMX": {"ranked_varieties": [{"variety": "v", "mean_yield_kg_ha": 9000.0, "source_ids": ["GENVCE", "CREA"]}]},
        "NODATA": {"ranked_varieties": []},
    }

    async def extrapolate(self_, crop, **kw):
        return by_crop[crop]

    patches = [
        patch.object(GraphDAO, "extrapolate_varieties", extrapolate),
        patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value={"error": "n/a"})),
        patch.object(GraphDAO, "recommend_next_crop", AsyncMock(return_value=[{"crop_eppo": "NODATA"}])),
        patch.object(GraphDAO, "get_rotation_constraints", AsyncMock(return_value=[])),
        patch.object(GraphDAO, "get_shared_pests", AsyncMock(return_value={})),
        patch.object(GraphDAO, "_evaluate_pac_compliance", AsyncMock(return_value={})),
        patch("app.services.crop_reference.get_crop_ref", AsyncMock(return_value=dict(_REF))),
    ]
    for p in patches:
        p.start()
    try:
        out = await GraphDAO(MagicMock()).rotation_plan(parcel_id=URN, years=2, starting_crop="ZEAMX", tenant_id="t")
    finally:
        for p in patches:
            p.stop()
    assert [e["source_ids"] for e in out["plan"]] == [["CREA", "GENVCE"], []]


async def _projection(season_start: str, initial_yield=None):
    ctx = {
        "phenology": {"stage": "vegetative"}, "season": {"start": season_start},
        "crop": {"eppo": "ZEAMX"}, "variety": {"name": "v"}, "target_environment": {"climate_class": "Cfa"},
    }
    extrapolated = {"ranked_varieties": [{"variety": "v", "mean_yield_kg_ha": 12000.0, "source_ids": ["GENVCE", "CREA"]}]}
    patches = [
        patch.object(GraphDAO, "get_crop_context", AsyncMock(return_value=ctx)),
        patch.object(GraphDAO, "extrapolate_varieties", AsyncMock(return_value=extrapolated)),
        patch.object(GraphDAO, "get_phenology_stages", AsyncMock(return_value=[])),
        patch.object(GraphDAO, "_fetch_weekly_eto", AsyncMock(return_value=120.0)),
    ]
    for p in patches:
        p.start()
    try:
        return await GraphDAO(MagicMock()).get_yield_projection(
            parcel_id=URN, tenant_id="t", initial_yield_kg_ha=initial_yield)
    finally:
        for p in patches:
            p.stop()


async def test_yield_projection_carries_the_sources_of_the_trials_behind_the_potential_yield():
    from datetime import datetime, timedelta, timezone
    out = await _projection((datetime.now(tz=timezone.utc).date() - timedelta(days=40)).isoformat())
    assert out["potential_yield_kg_ha"] == 12000.0 and out["source_ids"] == ["CREA", "GENVCE"]


async def test_yield_projection_before_planting_carries_the_sources_too():
    from datetime import datetime, timedelta, timezone
    out = await _projection((datetime.now(tz=timezone.utc).date() + timedelta(days=10)).isoformat())
    assert out["stage"] == "pre-emergence" and out["source_ids"] == ["CREA", "GENVCE"]


async def test_yield_projection_with_a_caller_supplied_yield_has_no_sources():
    from datetime import datetime, timedelta, timezone
    out = await _projection((datetime.now(tz=timezone.utc).date() - timedelta(days=40)).isoformat(), initial_yield=8000.0)
    assert out["potential_yield_kg_ha"] == 8000.0 and out["source_ids"] == []
