from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi.testclient import TestClient

import app.api.v1.planning as planning_api
from app.main import app
from neo4j import AsyncDriver

FIT = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "campaign": 2027,
       "facts": {"climate_class": None, "climate_source": "unavailable", "soil_texture": None, "soil_ph": None,
                 "soil_source": "unavailable", "regime": None, "regime_source": "unknown", "history_available": False},
       "species": [], "graphs": ["global"]}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    monkeypatch.setattr("app.core.knowledge.get_driver", lambda: MagicMock(spec=AsyncDriver))
    return TestClient(app)


def test_fit_route_validates_the_contract(client, monkeypatch):
    monkeypatch.setattr(planning_api, "parcel_fit", AsyncMock(return_value=FIT))
    r = client.get("/api/graph/planning/parcels/urn:ngsi-ld:AgriParcel:t:p1/fit", params={"campaign": 2027})
    assert r.status_code == 200 and r.json()["contract_version"] == "1.0"


def test_fit_route_404_for_unknown_parcel(client, monkeypatch):
    from app.planning.service import ParcelNotFound
    monkeypatch.setattr(planning_api, "parcel_fit", AsyncMock(side_effect=ParcelNotFound("x")))
    r = client.get("/api/graph/planning/parcels/x/fit", params={"campaign": 2027})
    assert r.status_code == 404


def test_fit_route_rejects_bad_regime(client):
    r = client.get("/api/graph/planning/parcels/p1/fit", params={"campaign": 2027, "regime": "drip"})
    assert r.status_code == 422


def test_fit_route_requires_a_verified_tenant(client, monkeypatch):
    # The conftest default disables auth; this test runs the real production branch.
    monkeypatch.setenv("AUTH_DISABLED", "false")
    called = AsyncMock(return_value=FIT)
    monkeypatch.setattr(planning_api, "parcel_fit", called)
    r = client.get("/api/graph/planning/parcels/urn:ngsi-ld:AgriParcel:t:p1/fit", params={"campaign": 2027})
    assert r.status_code == 401
    called.assert_not_called()


def test_varieties_route_rejects_unknown_crop(client):
    r = client.get("/api/graph/planning/parcels/p1/varieties",
                   params={"crop": "zzz", "regime": "secano", "management": "organic"})
    assert r.status_code == 422


VARIETIES = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "crop": "TRZAX", "regime": "secano",
             "management": "organic", "graph_id": "global", "status": "ok", "tier": "field", "n_environments": 3,
             "varieties": [{"variety": "A", "relative_yield_pct": 110.0, "n_environments": 3, "years": [2023, 2025],
                            "mean_kg_ha": 6000, "fw_slope": None, "stability": None, "few_environments": False}]}
VARIETIES_PARAMS = {"crop": "wheat", "regime": "secano", "management": "organic"}
VARIETIES_URL = "/api/graph/planning/parcels/urn:ngsi-ld:AgriParcel:t:p1/varieties"


def test_varieties_route_validates_the_contract(client, monkeypatch):
    monkeypatch.setattr(planning_api, "parcel_varieties", AsyncMock(return_value=VARIETIES))
    r = client.get(VARIETIES_URL, params=VARIETIES_PARAMS)
    body = r.json()
    assert r.status_code == 200 and body["contract_version"] == "1.0"
    assert body["varieties"][0]["variety"] == "A" and body["attributions"] == []


def test_varieties_route_404_for_unknown_parcel(client, monkeypatch):
    from app.planning.service import ParcelNotFound
    monkeypatch.setattr(planning_api, "parcel_varieties", AsyncMock(side_effect=ParcelNotFound("x")))
    assert client.get(VARIETIES_URL, params=VARIETIES_PARAMS).status_code == 404


def test_varieties_route_does_not_turn_other_value_errors_into_422(client, monkeypatch):
    monkeypatch.setattr(planning_api, "parcel_varieties", AsyncMock(side_effect=ValueError("bug")))
    crashing = TestClient(app, raise_server_exceptions=False)
    assert crashing.get(VARIETIES_URL, params=VARIETIES_PARAMS).status_code == 500


ESTIMATES_URL = "/api/graph/planning/parcels/urn:ngsi-ld:AgriParcel:t:p1/estimates"
ESTIMATES_PARAMS = {"crop": "TRZAX", "regime": "secano", "management": "conventional"}


def test_estimates_route_validates_the_contract(client, monkeypatch):
    out = {"parcel_id": "p1", "crop": "TRZAX", "variety": None, "status": "ok",
           "inputs": {"regime": "secano", "management": "conventional", "sowing_date": None, "sowing_source": "none",
                      "soil_source": "unavailable", "climate_source": "unavailable"},
           "estimates": [], "unavailable": [{"provider_id": "aquacrop", "reason": "preseason_not_supported"}],
           "disagreement": False}
    monkeypatch.setattr(planning_api, "parcel_estimates", AsyncMock(return_value=out))
    r = client.get("/api/graph/planning/parcels/p1/estimates",
                   params={"crop": "TRZAX", "regime": "secano", "management": "conventional"})
    assert r.status_code == 200 and r.json()["unavailable"][0]["reason"] == "preseason_not_supported"


def test_estimates_route_404_for_unknown_parcel(client, monkeypatch):
    from app.planning.service import ParcelNotFound
    monkeypatch.setattr(planning_api, "parcel_estimates", AsyncMock(side_effect=ParcelNotFound("x")))
    assert client.get(ESTIMATES_URL, params=ESTIMATES_PARAMS).status_code == 404


def test_estimates_route_rejects_unknown_crop(client):
    r = client.get(ESTIMATES_URL, params={**ESTIMATES_PARAMS, "crop": "zzz"})
    assert r.status_code == 422


def test_estimates_route_does_not_turn_other_value_errors_into_422(client, monkeypatch):
    monkeypatch.setattr(planning_api, "parcel_estimates", AsyncMock(side_effect=ValueError("bug")))
    crashing = TestClient(app, raise_server_exceptions=False)
    assert crashing.get(ESTIMATES_URL, params=ESTIMATES_PARAMS).status_code == 500


FARM_URL = "/api/graph/planning/farm"


def _orion_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://orion.invalid/entities")
    return httpx.HTTPStatusError("orion", request=request, response=httpx.Response(status, request=request))


def test_farm_route_uses_default_campaign(client, monkeypatch):
    monkeypatch.setattr(planning_api, "load_farm", AsyncMock(return_value=([], [], {}, {})))
    monkeypatch.setattr(planning_api, "load_reference", AsyncMock(return_value=MagicMock(constraints=())))
    r = client.get(FARM_URL)
    assert r.status_code == 200 and len(r.json()["campaigns"]) == 4


def test_farm_route_forwards_campaign_and_parcel(client, monkeypatch):
    load = AsyncMock(return_value=([], [], {}, {}))
    monkeypatch.setattr(planning_api, "load_farm", load)
    monkeypatch.setattr(planning_api, "load_reference", AsyncMock(return_value=MagicMock(constraints=())))
    r = client.get(FARM_URL, params={"campaign": 2030, "parcel_id": "urn:ngsi-ld:AgriParcel:t:p1"})
    assert r.status_code == 200 and r.json()["campaign"] == 2030
    assert load.await_args.args[1] == "urn:ngsi-ld:AgriParcel:t:p1"


def test_farm_route_404_when_orion_does_not_know_the_parcel(client, monkeypatch):
    monkeypatch.setattr(planning_api, "load_farm", AsyncMock(side_effect=_orion_error(404)))
    monkeypatch.setattr(planning_api, "load_reference", AsyncMock(return_value=MagicMock(constraints=())))
    assert client.get(FARM_URL, params={"parcel_id": "urn:ngsi-ld:AgriParcel:t:nope"}).status_code == 404


def test_farm_route_does_not_hide_other_orion_errors(client, monkeypatch):
    monkeypatch.setattr(planning_api, "load_farm", AsyncMock(side_effect=_orion_error(500)))
    monkeypatch.setattr(planning_api, "load_reference", AsyncMock(return_value=MagicMock(constraints=())))
    crashing = TestClient(app, raise_server_exceptions=False)
    assert crashing.get(FARM_URL).status_code == 500


def test_farm_route_requires_a_verified_tenant(client, monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    load = AsyncMock(return_value=([], [], {}, {}))
    monkeypatch.setattr(planning_api, "load_farm", load)
    assert client.get(FARM_URL).status_code == 401
    load.assert_not_called()


REAL_PARCEL = "urn:ngsi-ld:AgriParcel:12d68d51-35f5-4957-9222-caa961f9a427"
BAD_PARCEL_IDS = ['x?x="', "p1#frag", "a/../../version", 'p"1', "p 1"]


def _quoted(raw: str) -> str:
    from urllib.parse import quote
    return quote(raw, safe="")


@pytest.mark.parametrize("bad", BAD_PARCEL_IDS)
def test_farm_route_rejects_a_malformed_parcel_id_before_any_io(client, monkeypatch, bad):
    load = AsyncMock(return_value=([], [], {}, {}))
    monkeypatch.setattr(planning_api, "load_farm", load)
    assert client.get(FARM_URL, params={"parcel_id": bad}).status_code == 422
    load.assert_not_called()


def test_farm_route_rejects_an_empty_parcel_id(client, monkeypatch):
    load = AsyncMock(return_value=([], [], {}, {}))
    monkeypatch.setattr(planning_api, "load_farm", load)
    assert client.get(FARM_URL + "?parcel_id=").status_code == 422
    load.assert_not_called()


@pytest.mark.parametrize("good, expected", [
    (REAL_PARCEL, REAL_PARCEL),
    ("urn:ngsi-ld:AgriParcel:t:p1", "urn:ngsi-ld:AgriParcel:t:p1"),
    ("p1", "urn:ngsi-ld:AgriParcel:p1"),
])
def test_farm_route_accepts_and_normalizes_parcel_ids(client, monkeypatch, good, expected):
    load = AsyncMock(return_value=([], [], {}, {}))
    monkeypatch.setattr(planning_api, "load_farm", load)
    monkeypatch.setattr(planning_api, "load_reference", AsyncMock(return_value=MagicMock(constraints=())))
    assert client.get(FARM_URL, params={"parcel_id": good}).status_code == 200
    assert load.await_args.args[1] == expected


@pytest.mark.parametrize("name, params, service_fn", [
    ("fit", {"campaign": 2027}, "parcel_fit"),
    ("varieties", VARIETIES_PARAMS, "parcel_varieties"),
    ("estimates", ESTIMATES_PARAMS, "parcel_estimates"),
])
@pytest.mark.parametrize("bad", BAD_PARCEL_IDS)
def test_parcel_routes_reject_a_malformed_parcel_id_before_any_io(client, monkeypatch, name, params, service_fn, bad):
    called = AsyncMock()
    monkeypatch.setattr(planning_api, service_fn, called)
    r = client.get(f"/api/graph/planning/parcels/{_quoted(bad)}/{name}", params=params)
    assert r.status_code == 422
    called.assert_not_called()


@pytest.mark.parametrize("name, params, service_fn, ok", [
    ("fit", {"campaign": 2027}, "parcel_fit", FIT),
    ("varieties", VARIETIES_PARAMS, "parcel_varieties", VARIETIES),
])
@pytest.mark.parametrize("good, expected", [
    (REAL_PARCEL, REAL_PARCEL),
    ("urn:ngsi-ld:AgriParcel:t:p1", "urn:ngsi-ld:AgriParcel:t:p1"),
    ("p1", "urn:ngsi-ld:AgriParcel:p1"),
])
def test_parcel_routes_hand_the_normalized_urn_to_the_service(client, monkeypatch, name, params, service_fn, ok,
                                                              good, expected):
    called = AsyncMock(return_value=ok)
    monkeypatch.setattr(planning_api, service_fn, called)
    assert client.get(f"/api/graph/planning/parcels/{good}/{name}", params=params).status_code == 200
    assert called.await_args.kwargs["parcel_id"] == expected


def test_farm_route_maps_an_unreachable_orion_to_502(client, monkeypatch):
    monkeypatch.setattr(planning_api, "load_farm", AsyncMock(side_effect=httpx.ConnectError("down")))
    monkeypatch.setattr(planning_api, "load_reference", AsyncMock(return_value=MagicMock(constraints=())))
    assert client.get(FARM_URL).status_code == 502
    assert client.get(FARM_URL, params={"parcel_id": "p1"}).status_code == 502


def test_farm_route_does_not_blame_a_parcel_for_a_listing_404(client, monkeypatch):
    # Without parcel_id nothing is looked up by id: an Orion 404 is an upstream failure, not "parcel not found".
    monkeypatch.setattr(planning_api, "load_farm", AsyncMock(side_effect=_orion_error(404)))
    monkeypatch.setattr(planning_api, "load_reference", AsyncMock(return_value=MagicMock(constraints=())))
    crashing = TestClient(app, raise_server_exceptions=False)
    assert crashing.get(FARM_URL).status_code == 500


def test_parcel_route_rejects_an_empty_parcel_id(client, monkeypatch):
    called = AsyncMock()
    monkeypatch.setattr(planning_api, "parcel_fit", called)
    assert client.get("/api/graph/planning/parcels//fit", params={"campaign": 2027}).status_code == 422
    called.assert_not_called()


PLAN_URL = "/api/graph/planning/parcels/urn:ngsi-ld:AgriParcel:t:p1/plan"
PLAN_BODY = {"campaign": 2027, "regime": "secano", "management": "conventional", "cycles": [
    {"role": "main_crop", "crop": "maize", "sowing_start": "2027-04-20", "expected_termination": "2027-10-01"}]}
PLAN_WRITTEN = {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p1", "campaign": 2027,
                "written": ["urn:ngsi-ld:AgriCrop:t:p1:2027:0"], "removed": []}


def test_put_plan_route_validates_the_contract_and_hands_over_the_normalized_urn(client, monkeypatch):
    called = AsyncMock(return_value=PLAN_WRITTEN)
    monkeypatch.setattr(planning_api, "write_plan", called)
    r = client.put("/api/graph/planning/parcels/p1/plan", json=PLAN_BODY)
    assert r.status_code == 200 and r.json()["contract_version"] == "1.0"
    assert called.await_args.kwargs["parcel_urn"] == "urn:ngsi-ld:AgriParcel:p1"
    assert called.await_args.kwargs["plan"].campaign == 2027


def test_put_plan_route_rejects_a_malformed_parcel_id_before_any_io(client, monkeypatch):
    called = AsyncMock(return_value=PLAN_WRITTEN)
    monkeypatch.setattr(planning_api, "write_plan", called)
    r = client.put("/api/graph/planning/parcels/a%3Fx=1/plan", json=PLAN_BODY)
    assert r.status_code == 422
    called.assert_not_called()


@pytest.mark.parametrize("code, status", [("campaign_past", 422), ("campaign_started", 409), ("perennial_occupies", 409),
                                          ("parcel_not_found", 404)])
def test_put_plan_route_maps_plan_errors_to_their_status(client, monkeypatch, code, status):
    from app.planning.plan_write import PlanError
    monkeypatch.setattr(planning_api, "write_plan", AsyncMock(side_effect=PlanError(code, status)))
    r = client.put(PLAN_URL, json=PLAN_BODY)
    assert r.status_code == status and r.json()["detail"] == {"code": code}


def test_put_plan_route_rejects_an_invalid_body(client, monkeypatch):
    called = AsyncMock(return_value=PLAN_WRITTEN)
    monkeypatch.setattr(planning_api, "write_plan", called)
    assert client.put(PLAN_URL, json={**PLAN_BODY, "cycles": []}).status_code == 422
    called.assert_not_called()


def test_put_plan_route_requires_a_verified_tenant(client, monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    called = AsyncMock(return_value=PLAN_WRITTEN)
    monkeypatch.setattr(planning_api, "write_plan", called)
    assert client.put(PLAN_URL, json=PLAN_BODY).status_code == 401
    called.assert_not_called()


def test_put_plan_route_maps_an_unreachable_orion_to_502(client, monkeypatch):
    monkeypatch.setattr(planning_api, "write_plan", AsyncMock(side_effect=httpx.ConnectError("down")))
    assert client.put(PLAN_URL, json=PLAN_BODY).status_code == 502


def test_farm_route_survives_a_failing_reference_load(client, monkeypatch, caplog):
    # A failing reference read degrades to "rotation unknown"; the page still renders.
    parcel = {"id": "urn:ngsi-ld:AgriParcel:t:p1", "name": "Soto", "area": 2, "irrigationRegime": "secano"}
    monkeypatch.setattr(planning_api, "load_farm",
                        AsyncMock(return_value=([parcel], [], {}, {parcel["id"]: {"cycles": []}})))
    monkeypatch.setattr(planning_api, "load_reference", AsyncMock(side_effect=RuntimeError("graph down")))
    with caplog.at_level("ERROR", logger=planning_api.logger.name):
        r = client.get(FARM_URL, params={"campaign": 2027})
    assert r.status_code == 200
    assert [w["code"] for w in r.json()["parcels"][0]["warnings"]] == ["rotation_unknown"]
    assert any("graph down" in rec.getMessage() or rec.exc_info for rec in caplog.records)
