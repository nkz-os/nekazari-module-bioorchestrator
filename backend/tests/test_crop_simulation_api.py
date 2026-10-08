"""POST /api/graph/agriculture/crop-simulation: status codes and error body shape."""
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import app.api.v1.graph as graph_mod
from app.main import app
from app.services.sim_errors import SimulationError
from neo4j import AsyncDriver

URN = "urn:ngsi-ld:AgriParcel:montiko:p-1"
URL = "/api/graph/agriculture/crop-simulation"
H = {"X-Tenant-ID": "montiko"}


@pytest.fixture
def client(monkeypatch):
    calls = []
    outcome = {}

    class _DAO:
        def __init__(self, *a, **k): pass
        async def run_crop_simulation(self, **kw):
            calls.append(kw)
            if "raise" in outcome:
                raise outcome["raise"]
            return {"status": "complete", "parcel_id": kw["parcel_id"]}

    monkeypatch.setattr(graph_mod, "GraphDAO", _DAO)
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    c = TestClient(app)
    c.calls, c.outcome = calls, outcome
    return c


def test_ok_passes_query_params(client):
    r = client.post(URL, params={"parcel_id": URN, "crop_slug": "maize", "sowing_date": "2026-04-20",
                                 "irrigation": "full"}, headers=H)
    assert r.status_code == 200 and r.json()["status"] == "complete"
    kw = {k: v for k, v in client.calls[0].items() if k != "tenant_id"}  # tenant: auth is disabled in tests
    assert client.calls[0]["tenant_id"]
    assert kw == {
        "parcel_id": URN, "crop_slug": "maize",
        "sowing_date_str": "2026-04-20", "irrigation": "full", "engine": "aquacrop"}


def test_defaults(client):
    client.post(URL, params={"parcel_id": URN}, headers=H)
    kw = client.calls[0]
    assert (kw["irrigation"], kw["engine"], kw["crop_slug"], kw["sowing_date_str"]) == (
        "rainfed", "aquacrop", None, None)


def test_input_error_is_422_with_code_and_message(client):
    client.outcome["raise"] = SimulationError("unsupported_crop", "Crop 'olive' cannot be simulated yet.")
    r = client.post(URL, params={"parcel_id": URN}, headers=H)
    assert r.status_code == 422
    assert r.json() == {"detail": {"code": "unsupported_crop",
                                   "message": "Crop 'olive' cannot be simulated yet."}}


def test_upstream_error_is_503(client):
    client.outcome["raise"] = SimulationError("archive_not_configured", "no archive", 503)
    r = client.post(URL, params={"parcel_id": URN}, headers=H)
    assert r.status_code == 503 and r.json()["detail"]["code"] == "archive_not_configured"


def test_parcel_not_found_is_404(client):
    client.outcome["raise"] = SimulationError("parcel_not_found", "nope", 404)
    assert client.post(URL, params={"parcel_id": URN}, headers=H).status_code == 404


def test_old_route_is_gone(client):
    assert client.post("/api/graph/agriculture/wofost-simulation",
                       params={"parcel_id": URN}, headers=H).status_code == 404
