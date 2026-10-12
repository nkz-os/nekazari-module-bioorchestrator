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


# ------------------------------------------------------------------ jobs
JOBS = "/api/graph/agriculture/crop-simulation/jobs"


@pytest.fixture
def jobs_client(monkeypatch):
    class _DAO:
        def __init__(self, *a, **k): pass
        async def run_crop_simulation(self, **kw):
            kw["progress"](0, 3)
            for i in range(3):
                kw["progress"](i + 1, 3)
            return {"status": "preseason", "parcel_id": kw["parcel_id"], "tenant": kw["tenant_id"]}

    monkeypatch.setattr(graph_mod, "GraphDAO", _DAO)
    monkeypatch.setattr(graph_mod, "_SIM_JOBS", None)
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    with TestClient(app) as c:
        yield c


def _poll(c, job_id, headers=H):
    import time
    for _ in range(100):
        r = c.get(f"{JOBS}/{job_id}", headers=headers)
        if r.status_code != 200 or r.json()["state"] in ("done", "failed"):
            return r
        time.sleep(0.02)
    raise AssertionError("job did not finish")


def test_job_start_then_poll_until_done(jobs_client):
    r = jobs_client.post(JOBS, params={"parcel_id": URN, "sowing_date": "2026-11-01"}, headers=H)
    assert r.status_code == 202
    job_id = r.json()["job_id"]
    body = _poll(jobs_client, job_id).json()
    assert body["state"] == "done" and (body["done"], body["total"]) == (3, 3)
    assert body["result"]["status"] == "preseason" and body["error"] is None
    # identical request: same job, already done (the cache)
    again = jobs_client.post(JOBS, params={"parcel_id": URN, "sowing_date": "2026-11-01"}, headers=H).json()
    assert again["job_id"] == job_id and again["state"] == "done"


def test_unknown_job_is_404(jobs_client):
    r = jobs_client.get(f"{JOBS}/does-not-exist", headers=H)
    assert r.status_code == 404 and r.json()["detail"]["code"] == "job_not_found"
