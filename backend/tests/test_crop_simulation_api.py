"""POST /api/graph/agriculture/crop-simulation: status codes and error body shape."""
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import app.api.v1.graph as graph_mod
from app.main import app
from app.services.sim_errors import SimulationError
from neo4j import AsyncDriver

URN = "urn:ngsi-ld:AgriParcel:acmefarm:p-1"
URL = "/api/graph/agriculture/crop-simulation"
H = {"X-Tenant-ID": "acmefarm"}


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
                                 "irrigation": "full", "sowing_method": "direct"}, headers=H)
    assert r.status_code == 200 and r.json()["status"] == "complete"
    kw = {k: v for k, v in client.calls[0].items() if k != "tenant_id"}  # tenant: auth is disabled in tests
    assert client.calls[0]["tenant_id"]
    assert kw == {
        "parcel_id": URN, "crop_slug": "maize",
        "sowing_date_str": "2026-04-20", "irrigation": "full", "engine": "aquacrop", "sowing_method": "direct"}


def test_defaults(client):
    client.post(URL, params={"parcel_id": URN}, headers=H)
    kw = client.calls[0]
    assert (kw["irrigation"], kw["engine"], kw["crop_slug"], kw["sowing_date_str"]) == (
        "rainfed", "aquacrop", None, None)
    assert kw["sowing_method"] is None


def test_input_error_is_422_with_code_and_message(client):
    client.outcome["raise"] = SimulationError("unsupported_crop", "Crop 'olive' cannot be simulated yet.")
    r = client.post(URL, params={"parcel_id": URN}, headers=H)
    assert r.status_code == 422
    assert r.json() == {"detail": {"code": "unsupported_crop",
                                   "message": "Crop 'olive' cannot be simulated yet."}}


def test_invalid_sowing_method_is_422(client):
    client.outcome["raise"] = SimulationError(
        "invalid_sowing_method", "sowing_method must be one of: direct, conventional.")
    r = client.post(URL, params={"parcel_id": URN, "sowing_method": "no-till"}, headers=H)
    assert r.status_code == 422 and r.json()["detail"]["code"] == "invalid_sowing_method"


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
    monkeypatch.setattr("app.services.sim_jobs._SHARED", None)
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


def test_a_tenant_past_its_cap_gets_429(jobs_client, monkeypatch):
    class _Full:
        async def start(self, *a, **k):
            raise SimulationError("too_many_jobs", "m", 429)

    monkeypatch.setattr(graph_mod, "shared_sim_jobs", lambda: _Full())
    r = jobs_client.post(JOBS, params={"parcel_id": URN, "sowing_date": "2026-11-01"}, headers=H)
    assert r.status_code == 429 and r.json()["detail"]["code"] == "too_many_jobs"


def test_job_rejects_an_invalid_method_before_starting(jobs_client):
    r = jobs_client.post(JOBS, params={"parcel_id": URN, "sowing_date": "2026-11-01",
                                       "sowing_method": "no-till"}, headers=H)
    assert r.status_code == 422 and r.json()["detail"]["code"] == "invalid_sowing_method"


def test_direct_sowing_is_its_own_job_and_absent_keeps_old_identity(jobs_client):
    q = {"parcel_id": URN, "sowing_date": "2026-11-01"}
    bare = jobs_client.post(JOBS, params=q, headers=H).json()["job_id"]
    direct = jobs_client.post(JOBS, params={**q, "sowing_method": "direct"}, headers=H).json()["job_id"]
    assert direct != bare
    _poll(jobs_client, bare)
    _poll(jobs_client, direct)
    assert jobs_client.post(JOBS, params=q, headers=H).json()["job_id"] == bare


def test_an_eppo_code_joins_the_job_of_its_species_slug(jobs_client):
    # Crop-health links send the EPPO code; the in-season forecast keys its job by the species slug.
    q = {"parcel_id": URN, "sowing_date": "2026-11-01"}
    by_slug = jobs_client.post(JOBS, params={**q, "crop_slug": "wheat"}, headers=H).json()["job_id"]
    by_eppo = jobs_client.post(JOBS, params={**q, "crop_slug": "TRZAX"}, headers=H).json()["job_id"]
    assert by_eppo == by_slug
    _poll(jobs_client, by_slug)


def test_the_simulation_receives_the_species_slug_for_an_eppo_code(client):
    client.post(URL, params={"parcel_id": URN, "crop_slug": "TRZAX"}, headers=H)
    assert client.calls[0]["crop_slug"] == "wheat"


def test_an_unknown_crop_is_passed_through_for_the_engine_to_refuse(client):
    client.post(URL, params={"parcel_id": URN, "crop_slug": "not-a-crop"}, headers=H)
    assert client.calls[0]["crop_slug"] == "not-a-crop"
