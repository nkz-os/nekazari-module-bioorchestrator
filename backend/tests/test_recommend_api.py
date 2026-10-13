"""HTTP contract of the recommend endpoints."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.core.dependencies import get_neo4j_driver
from app.graph import zone_match
from app.graph.dao import GraphDAO
from app.main import app

URN = "urn:ngsi-ld:AgriParcel:p1"


@pytest.fixture(scope="module")
def client():
    # Do not rely on the shell or conftest setdefault: pin auth-disabled explicitly.
    mp = pytest.MonkeyPatch()
    mp.setenv("AUTH_DISABLED", "true")
    app.dependency_overrides[get_neo4j_driver] = lambda: MagicMock()
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_neo4j_driver, None)
        mp.undo()


@pytest.mark.parametrize("extra", [{"parcel_id": URN}, {"tenant_id": "tenant-a"}])
def test_evidence_route_rejects_tenant_params(client, extra):
    params = {"climate_class": "Cfb", "crop": "TRZAX", **extra}
    assert client.get("/api/graph/agriculture/recommend/evidence", params=params).status_code == 422


def test_evidence_passes_pagination(client):
    page = {"items": [], "total": 0, "page": 2, "page_size": 10}
    with patch.object(
        GraphDAO, "get_similar_sites", AsyncMock(return_value=[{"name": "site-a"}])
    ) as sim, patch.object(GraphDAO, "list_trial_evidence", AsyncMock(return_value=page)) as m:
        r = client.get(
            "/api/graph/agriculture/recommend/evidence",
            params={"climate_class": "Cfb", "crop": "TRZAX", "page": 2, "page_size": 10},
        )
    assert r.status_code == 200 and r.json() == {**page, "attributions": []}
    assert m.call_args.kwargs["similar_sites"] == ["site-a"]
    assert m.call_args.kwargs["page"] == 2 and m.call_args.kwargs["page_size"] == 10
    assert sim.call_args.kwargs["limit"] is None  # every matching field site


@pytest.mark.parametrize("bad", [{"page": 0}, {"page_size": 0}, {"page_size": 51}])
def test_evidence_pagination_bounds(client, bad):
    r = client.get(
        "/api/graph/agriculture/recommend/evidence",
        params={"climate_class": "Cfb", "crop": "TRZAX", **bad},
    )
    assert r.status_code == 422


def test_evidence_crop_and_variety_validated(client):
    base = {"climate_class": "Cfb"}
    assert client.get("/api/graph/agriculture/recommend/evidence",
                      params={**base, "crop": "bad code"}).status_code == 422
    assert client.get("/api/graph/agriculture/recommend/evidence",
                      params={**base, "crop": "TRZAX", "variety": "v" * 65}).status_code == 422


# ── final fix wave ──────────────────────────────────────────────────────────
_EV_PAGE = {"items": [], "total": 0, "page": 1, "page_size": 20}
_VEC = {"annual_rainfall_mm": 500, "annual_et0_mm": 900, "coldest_month_min_c": 1, "annual_temp_c": 14}


def _evidence_call(client, **params):
    sites = AsyncMock(side_effect=lambda **kw: [{"name": "site-v2"}] if kw.get("vector_version") == "v2"
                      else [{"name": "site-a"}])
    with patch.object(GraphDAO, "get_similar_sites", sites), \
         patch.object(GraphDAO, "list_trial_evidence", AsyncMock(return_value=_EV_PAGE)) as m:
        r = client.get("/api/graph/agriculture/recommend/evidence",
                       params={"climate_class": "Cfb", "crop": "TRZAX", **params})
    return r, sites, m


def test_evidence_v2_fallback_uses_v2_sites(client):
    r, sites, m = _evidence_call(client, similarity="vector_v2_fallback", soil_type="Loam", **_VEC)
    assert r.status_code == 200
    kw = sites.await_args.kwargs
    assert kw == {"climate_class": "Cfb", "soil_type": "Loam", "rainfall_min": None, "rainfall_max": None,
                  "limit": 50, "vector_version": "v2",
                  "target_features": {"rainfall": 500, "et0": 900, "coldest_min": 1, "annual_temp": 14}}
    assert m.call_args.kwargs["similar_sites"] == ["site-v2"]


@pytest.mark.parametrize("drop", list(_VEC))
def test_evidence_v2_fallback_needs_complete_vector(client, drop):
    vec = {k: v for k, v in _VEC.items() if k != drop}
    r, sites, _ = _evidence_call(client, similarity="vector_v2_fallback", **vec)
    assert r.status_code == 422
    assert "vector_v2_fallback" in r.json()["detail"]
    assert sites.await_count == 0


def test_evidence_v2_fallback_zero_et0_rejected(client):
    r, _, _ = _evidence_call(client, similarity="vector_v2_fallback", **{**_VEC, "annual_et0_mm": 0})
    assert r.status_code == 422


def test_evidence_default_similarity_is_koppen(client):
    r, sites, m = _evidence_call(client, **_VEC)
    assert r.status_code == 200
    kw = sites.await_args.kwargs
    assert kw.get("vector_version", "v1") == "v1" and kw.get("target_features") is None
    assert m.call_args.kwargs["similar_sites"] == ["site-a"]
    assert _evidence_call(client, similarity="other")[0].status_code == 422


def test_evidence_takes_an_opaque_zone_id_not_a_point(client):
    with patch.object(GraphDAO, "list_trial_evidence", AsyncMock(return_value={"items": []})) as m, \
            patch.object(GraphDAO, "get_similar_sites", AsyncMock(return_value=[])):
        base = {"climate_class": "Cfb", "crop": "HORVX", "tier": "regional", "country": "ES"}
        zone = zone_match.zone_id(zone_match.context_from_values(10.5, 520, None))
        r = client.get("/api/graph/agriculture/recommend/evidence", params={**base, "zone": zone})
        assert r.status_code == 200
        ctx = m.call_args.kwargs["zone_ctx"]
        direct = zone_match.context_from_values(10.5, 520, None)
        assert ctx.ready and ctx.allow == direct.allow and ctx.deny == direct.deny
        for bad in ("41.0,-3.0", "x", "10.5_520", "10.5_520_1"):
            assert client.get("/api/graph/agriculture/recommend/evidence",
                              params={**base, "zone": bad}).status_code == 422
        client.get("/api/graph/agriculture/recommend/evidence",
                   params={**base, "zone": zone + ",no-such-definition:cold:humid"})
        assert m.call_args.kwargs["zone_ctx"] is None  # an id of other zone definitions is not used
        client.get("/api/graph/agriculture/recommend/evidence", params={**base, "country": "IT", "zone": zone})
        assert m.call_args.kwargs["zone_ctx"] is None
        client.get("/api/graph/agriculture/recommend/evidence", params={**base, "lat": 41, "lon": -3})
        assert m.call_args.kwargs["zone_ctx"] is None  # a point is not an input of the route


# ── purpose and tier ────────────────────────────────────────────────────────
def test_unknown_purpose_rejected(client):
    params = {"climate_class": "Cfb", "crop": "TRZAX", "purpose": "grain"}
    assert client.get("/api/graph/agriculture/recommend/evidence", params=params).status_code == 422


def test_evidence_forwards_purpose_and_tier_with_field_sites_by_default(client):
    r, sites, m = _evidence_call(client, purpose="forage")
    assert r.status_code == 200
    assert m.call_args.kwargs["purpose"] == "forage" and m.call_args.kwargs["tier"] == "field"
    assert sites.await_args.kwargs["limit"] is None and "include_aggregate" not in sites.await_args.kwargs


def test_evidence_regional_tier_uses_every_site_of_the_climate(client):
    sites = AsyncMock(return_value=[{"name": "f01", "site_kind": "field"},
                                    {"name": "UK national list", "site_kind": "aggregate"}])
    with patch.object(GraphDAO, "get_similar_sites", sites), \
         patch.object(GraphDAO, "list_trial_evidence", AsyncMock(return_value=_EV_PAGE)) as m:
        r = client.get("/api/graph/agriculture/recommend/evidence",
                       params={"climate_class": "Cfb", "crop": "LYPES", "tier": "regional", "soil_type": "Loam"})
    assert r.status_code == 200
    # aggregate pseudo-sites and field-named ones (aggregate-source rows sit at the latter, policy
    # rule 9): the row policy lists only the rows classed as regional
    assert m.call_args.kwargs["similar_sites"] == ["f01", "UK national list"]
    assert m.call_args.kwargs["tier"] == "regional"
    kw = sites.await_args.kwargs
    assert kw["include_aggregate"] is True and kw["soil_type"] is None  # the soil filter is the field tier's


def test_evidence_regional_tier_needs_koppen_similarity(client):
    r, sites, _ = _evidence_call(client, tier="regional", similarity="vector_v2_fallback", **_VEC)
    assert r.status_code == 422 and sites.await_count == 0
    assert client.get("/api/graph/agriculture/recommend/evidence",
                      params={"climate_class": "Cfb", "crop": "TRZAX", "tier": "national"}).status_code == 422


def _regional_evidence(client, **params):
    sites = AsyncMock(return_value=[{"name": "ES zone", "site_kind": "aggregate"}])
    with patch.object(GraphDAO, "get_similar_sites", sites), \
         patch.object(GraphDAO, "list_trial_evidence", AsyncMock(return_value=_EV_PAGE)):
        r = client.get("/api/graph/agriculture/recommend/evidence",
                       params={"climate_class": "Cfb", "crop": "TRZAX", "tier": "regional", **params})
    return r, sites


def test_evidence_regional_country_is_passed_to_the_site_lookup(client):
    r, sites = _regional_evidence(client, country="ES")
    assert r.status_code == 200 and sites.await_args.kwargs["country"] == "ES"


def test_evidence_without_country_keeps_the_climate_only_lookup(client):
    r, sites = _regional_evidence(client)
    assert r.status_code == 200 and sites.await_args.kwargs["country"] is None


@pytest.mark.parametrize("bad", ["es", "ESP", "E", "1A"])
def test_evidence_country_must_be_iso_alpha2(client, bad):
    r, sites = _regional_evidence(client, country=bad)
    assert r.status_code == 422 and sites.await_count == 0


def test_evidence_field_tier_ignores_country(client):
    r, sites, _ = _evidence_call(client, country="ES")
    assert r.status_code == 200 and "country" not in sites.await_args.kwargs
