"""HTTP contract of the recommend endpoints."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.auth import SKIP_AUTH_PREFIXES
from app.auth_policy import requires_identity
from app.core.dependencies import get_neo4j_driver
from app.graph.dao import GraphDAO
from app.main import app

OK = {"status": "ok", "conditions": {}, "recommendations": [], "data_quality": {}}
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


def test_anonymous_parcel_id_rejected_with_auth_enabled(client, monkeypatch):
    # Either the middleware (401, no identity) or the route (422, parcel_id
    # not accepted) must stop an anonymous caller; both are safe outcomes.
    monkeypatch.setenv("AUTH_DISABLED", "false")
    r = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb", "parcel_id": "x"})
    assert r.status_code in (401, 422)


def test_parcel_route_requires_identity_and_is_not_public():
    path = f"/api/graph/recommend/parcel/{URN}"
    assert requires_identity(path, "GET", {}) is True
    assert not any(path.startswith(p) for p in SKIP_AUTH_PREFIXES)


@pytest.mark.parametrize("extra", [{"parcel_id": URN}, {"tenant_id": "tenant-a"}])
@pytest.mark.parametrize(
    "path", ["/api/graph/agriculture/recommend", "/api/graph/agriculture/recommend/evidence"]
)
def test_conditions_routes_reject_tenant_params(client, path, extra):
    params = {"climate_class": "Cfb", "crop": "TRZAX", **extra}
    assert client.get(path, params=params).status_code == 422


def test_climate_required(client):
    assert client.get("/api/graph/agriculture/recommend").status_code == 422


def test_crops_param_normalized_and_deduped(client):
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        r = client.get(
            "/api/graph/agriculture/recommend",
            params={"climate_class": "Cfb", "crops": "trzax,CIEAR,TRZAX"},
        )
    assert r.status_code == 200
    conds = m.call_args.args[0]
    assert conds["crops"] == ["TRZAX", "CIEAR"]
    assert conds["management"] == "any" and conds["season"] == "all" and conds["top_n"] == 10
    assert "climate_detail" not in conds or conds["climate_detail"] is None


def test_too_many_crops_rejected(client):
    r = client.get(
        "/api/graph/agriculture/recommend",
        params={"climate_class": "Cfb", "crops": "A1,B1,C1,D1,E1"},
    )
    assert r.status_code == 422


def test_numeric_climate_inputs_forwarded(client):
    params = {
        "climate_class": "Cfb", "annual_rainfall_mm": 600, "annual_et0_mm": 1100,
        "coldest_month_min_c": -3.5, "annual_temp_c": 12.5, "frost_margin_c": 3,
    }
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        r = client.get("/api/graph/agriculture/recommend", params=params)
    assert r.status_code == 200
    c = m.call_args.args[0]
    assert c["annual_rainfall_mm"] == 600 and c["annual_et0_mm"] == 1100
    assert c["coldest_month_min_c"] == -3.5 and c["annual_temp_c"] == 12.5
    assert c["frost_margin_c"] == 3


def test_frost_margin_defaults_to_none(client):
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb"})
    assert m.call_args.args[0]["frost_margin_c"] is None


@pytest.mark.parametrize(
    "bad",
    [
        {"annual_rainfall_mm": 5001}, {"annual_et0_mm": -1}, {"coldest_month_min_c": -61},
        {"annual_temp_c": 41}, {"frost_margin_c": 16}, {"frost_margin_c": -1},
        {"soil_ph": 2}, {"top_n": 31}, {"management": "x"}, {"season": "x"},
        {"irrigation_regime": "x"},
    ],
)
def test_out_of_range_rejected(client, bad):
    r = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb", **bad})
    assert r.status_code == 422


def test_parcel_needs_climate(client):
    env = {
        "climate_class": None, "soil": {"data_available": False},
        "irrigation": {"inferred": None}, "climate_detail": None,
        "inputs_used": {"soil": "unavailable", "climate": "unavailable"},
    }
    with patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value=env)):
        r = client.get(f"/api/graph/recommend/parcel/{URN}")
    assert r.status_code == 200
    assert r.json()["status"] == "needs_climate"


def test_parcel_climate_override_recovers_and_is_recorded(client):
    env = {
        "climate_class": None, "soil": {"data_available": True, "ph": 7.9, "texture": "clay"},
        "irrigation": {"inferred": "secano"}, "climate_detail": None,
        "inputs_used": {"soil": "soil_module", "climate": "unavailable"},
    }
    with patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value=env)), \
         patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        r = client.get(f"/api/graph/recommend/parcel/{URN}", params={"climate_class": "Csa"})
    assert r.status_code == 200
    conds = m.call_args.args[0]
    assert conds["climate_class"] == "Csa" and conds["soil_ph"] == 7.9
    assert conds["soil_texture"] == "clay" and conds["irrigation_regime"] == "secano"
    assert r.json()["parcel_environment"]["inputs_used"]["user_override"] == ["climate_class"]


def test_parcel_fills_numeric_climate_from_detail_and_margin(client):
    env = {
        "climate_class": "Csa", "soil": {"data_available": False},
        "irrigation": {"inferred": None},
        "climate_detail": {
            "source": "chelsa_v2.1", "annual_rainfall_mm": 450.0, "annual_et0_mm": 1200.0,
            "coldest_month_min_c": -2.0, "annual_temp_c": 15.0,
        },
        "inputs_used": {"soil": "unavailable", "climate": "chelsa"},
    }
    with patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value=env)), \
         patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        r = client.get(
            f"/api/graph/recommend/parcel/{URN}",
            params={"irrigation_regime": "regadío", "frost_margin_c": 2},
        )
    assert r.status_code == 200
    c = m.call_args.args[0]
    assert c["annual_rainfall_mm"] == 450.0 and c["annual_et0_mm"] == 1200.0
    assert c["coldest_month_min_c"] == -2.0 and c["annual_temp_c"] == 15.0
    assert c["frost_margin_c"] == 2
    assert c["irrigation_regime"] == "regadío"
    assert r.json()["parcel_environment"]["inputs_used"]["user_override"] == ["irrigation_regime"]


def test_parcel_not_found(client):
    with patch.object(
        GraphDAO, "get_parcel_environment", AsyncMock(return_value={"error": "Parcel not found"})
    ):
        assert client.get(f"/api/graph/recommend/parcel/{URN}").status_code == 404


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


def _env(soil, climate_detail=None, climate_class="Csa"):
    return {
        "climate_class": climate_class, "soil": soil, "irrigation": {"inferred": None},
        "climate_detail": climate_detail,
        "inputs_used": {"soil": "soil_module", "climate": "chelsa"},
    }


def _parcel_call(client, env, **params):
    with patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value=env)), \
         patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        r = client.get(f"/api/graph/recommend/parcel/{URN}", params=params)
    return r, m


def test_parcel_soil_type_from_wrb_and_override_wins(client):
    env = _env({"data_available": True, "wrb_type": "Vertisol", "ph": 7.0, "texture": "clay"})
    r, m = _parcel_call(client, env)
    assert r.status_code == 200 and m.call_args.args[0]["soil_type"] == "Vertisol"
    r, m = _parcel_call(client, env, soil_type="Luvisol")
    assert m.call_args.args[0]["soil_type"] == "Luvisol"


def test_parcel_soil_type_ignored_when_soil_unavailable(client):
    env = _env({"data_available": False, "wrb_type": "Vertisol"})
    _, m = _parcel_call(client, env)
    assert m.call_args.args[0]["soil_type"] is None


def test_parcel_marks_mixed_climate_provenance(client):
    detail = {"annual_rainfall_mm": 450.0, "annual_et0_mm": 1200.0,
              "coldest_month_min_c": -2.0, "annual_temp_c": 15.0}
    env = _env({"data_available": False}, climate_detail=detail)
    r, _ = _parcel_call(client, env, climate_class="Cfb")
    iu = r.json()["parcel_environment"]["inputs_used"]
    assert iu["user_override"] == ["climate_class"]
    assert iu["climate_numbers_from_parcel"] is True
    r, _ = _parcel_call(client, env)
    assert "climate_numbers_from_parcel" not in r.json()["parcel_environment"]["inputs_used"]


@pytest.mark.parametrize("crops", ["A1,B1,C1,D1,E1", "trzax,bad code", "X", "TOOLONGCODE"])
def test_bad_crops_rejected(client, crops):
    r = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb", "crops": crops})
    assert r.status_code == 422


def test_long_crops_string_rejected_early(client):
    crops = ",".join(f"C{i:04d}" for i in range(5000))
    r = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb", "crops": crops})
    assert r.status_code == 422


@pytest.mark.parametrize("bad", [
    {"climate_class": "cfb"}, {"climate_class": "Cfbxx"}, {"climate_class": "C" + "a" * 70},
    {"soil_type": "x" * 65}, {"soil_texture": "x" * 65},
])
def test_free_text_validated(client, bad):
    r = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb", **bad})
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


def _real_recommend_patches():
    from app.graph import dao as dao_mod
    variety = {"variety": "V1", "variety_uri": "urn:x", "mean_yield_kg_ha": 5500.0, "min_yield_kg_ha": 4000.0,
               "max_yield_kg_ha": 7000.0, "stddev_yield_kg_ha": 550.0, "numeric_yield_count": 12,
               "trial_count": 12, "trial_sites": ["site-a"], "trial_years": [2020], "disease_scores": {},
               "confidence": "high", "crop_reference_median_kg_ha": 5000.0, "crop_reference_n": 40}
    dao_mod._RECOMMEND_CACHE.clear()
    return (
        patch.object(GraphDAO, "get_available_crops", AsyncMock(return_value=[
            {"eppo_code": "TRZAX", "scientific_name": "Triticum aestivum"},
            {"eppo_code": "CIEAR", "scientific_name": "Cicer arietinum"}])),
        patch.object(GraphDAO, "extrapolate_varieties", AsyncMock(return_value={"ranked_varieties": [variety]})),
        patch.object(GraphDAO, "get_similar_sites", AsyncMock(return_value=[{"name": "site-a"}])),
        patch.object(GraphDAO, "_crops_with_analog_trials",
                     AsyncMock(side_effect=lambda eppos, names, **kw: set(eppos))),
        patch.object(GraphDAO, "get_soil_suitability", AsyncMock(return_value=None)),
        patch.object(GraphDAO, "get_heat_tolerance", AsyncMock(return_value=None)),
    )


def test_parcel_and_conditions_routes_give_identical_recommendations(client):
    from app.graph import dao as dao_mod
    env = _env({"data_available": False}, climate_class="Cfb")
    p = _real_recommend_patches()
    with p[0], p[1], p[2], p[3], p[4], p[5], \
         patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value=env)):
        by_parcel = client.get(f"/api/graph/recommend/parcel/{URN}")
        dao_mod._RECOMMEND_CACHE.clear()
        by_conditions = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb"})
    dao_mod._RECOMMEND_CACHE.clear()
    assert by_parcel.status_code == 200 and by_conditions.status_code == 200
    recs = by_parcel.json()["recommendations"]
    assert recs and recs == by_conditions.json()["recommendations"]


@pytest.mark.parametrize("parcel", [URN, "p1"])
def test_parcel_route_without_identity_is_401_when_auth_enabled(client, monkeypatch, parcel):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    with patch.object(GraphDAO, "get_parcel_environment",
                      AsyncMock(side_effect=AssertionError("must not be reached"))):
        r = client.get(f"/api/graph/recommend/parcel/{parcel}")
    assert r.status_code == 401


def test_country_param_forwarded(client):
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        r = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Csa", "country": "ES"})
    assert r.status_code == 200
    assert m.call_args.args[0]["country"] == "ES"


def test_country_defaults_to_none(client):
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        client.get("/api/graph/agriculture/recommend", params={"climate_class": "Csa"})
    assert m.call_args.args[0]["country"] is None


@pytest.mark.parametrize("bad", ["es", "ESP", "E", "1A", ""])
def test_bad_country_rejected(client, bad):
    r = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Csa", "country": bad})
    assert r.status_code == 422


@pytest.mark.parametrize("country", ["ES", None])
def test_parcel_country_from_environment(client, country):
    env = {
        "climate_class": "Csa", "country": country, "soil": {"data_available": False},
        "irrigation": {"inferred": None}, "climate_detail": None,
        "inputs_used": {"soil": "unavailable", "climate": "chelsa"},
    }
    with patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value=env)), \
         patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        r = client.get(f"/api/graph/recommend/parcel/{URN}")
    assert r.status_code == 200
    assert m.call_args.args[0]["country"] == country
    assert r.json()["parcel_environment"]["country"] == country


def test_parcel_passes_centroid_to_conditions(client):
    env = {**_env({"data_available": False}), "centroid": {"lat": 48.85, "lon": 2.35}}
    _, m = _parcel_call(client, env)
    c = m.call_args.args[0]
    assert c["lat"] == 48.85 and c["lon"] == 2.35


@pytest.mark.parametrize("centroid", [None, {"lat": None, "lon": None}, {"lat": 48.85, "lon": None}])
def test_parcel_without_centroid_sends_no_point(client, centroid):
    _, m = _parcel_call(client, {**_env({"data_available": False}), "centroid": centroid})
    c = m.call_args.args[0]
    assert "lat" not in c and "lon" not in c


def test_conditions_route_forwards_the_point_only_when_both_are_given(client):
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb", "lat": 1, "lon": 2})
        c = m.call_args.args[0]
        assert c["lat"] == 1 and c["lon"] == 2
        client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb"})
        c = m.call_args.args[0]
        assert "lat" not in c and "lon" not in c
        r = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb", "lat": 1})
    assert r.status_code == 422


# ── purpose and tier ────────────────────────────────────────────────────────
def test_purpose_defaults_to_main_and_is_forwarded_on_conditions(client):
    with patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb"})
        assert m.call_args.args[0]["purpose"] == "main"
        r = client.get("/api/graph/agriculture/recommend", params={"climate_class": "Cfb", "purpose": "forage"})
    assert r.status_code == 200 and m.call_args.args[0]["purpose"] == "forage"


@pytest.mark.parametrize("path", ["/api/graph/agriculture/recommend", "/api/graph/agriculture/recommend/evidence"])
def test_unknown_purpose_rejected(client, path):
    params = {"climate_class": "Cfb", "crop": "TRZAX", "purpose": "grain"}
    assert client.get(path, params=params).status_code == 422


def test_parcel_route_forwards_purpose(client):
    env = {"climate_class": "Csa", "soil": {"data_available": False}, "irrigation": {"inferred": None},
           "climate_detail": None, "inputs_used": {}}
    with patch.object(GraphDAO, "get_parcel_environment", AsyncMock(return_value=env)), \
         patch.object(GraphDAO, "recommend_for_conditions", AsyncMock(return_value=OK)) as m:
        client.get(f"/api/graph/recommend/parcel/{URN}")
        assert m.call_args.args[0]["purpose"] == "main"
        client.get(f"/api/graph/recommend/parcel/{URN}", params={"purpose": "forage"})
    assert m.call_args.args[0]["purpose"] == "forage"


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
