from unittest.mock import AsyncMock, MagicMock

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
