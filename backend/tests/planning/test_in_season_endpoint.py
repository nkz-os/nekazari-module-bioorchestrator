"""/planning/.../estimates/in-season with fake data loading and a fake job store: no engine runs."""
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock

import httpx
import pytest

from app.planning import in_season
from app.planning.service import ParcelNotFound
from app.services.sim_errors import SimulationError
from app.services.sim_jobs import Job, SimJobs

TODAY = date(2026, 3, 15)
PARCEL_ID = "urn:ngsi-ld:AgriParcel:t:p1"
PARCEL = {"id": PARCEL_ID, "irrigationRegime": "rainfed",
          "location": {"type": "Polygon", "coordinates": [[[-1.6, 42.8], [-1.5, 42.8], [-1.5, 42.9],
                                                          [-1.6, 42.9], [-1.6, 42.8]]]}}
CROP_ID = "urn:ngsi-ld:AgriCrop:t:c1"
CROPS = {CROP_ID: {"id": CROP_ID, "species": "TRZAX", "irrigationRegime": "rainfed"}}
SOWN = {"current": {"crop_id": CROP_ID, "species": "TRZAX", "start": {"date": "2025-10-20", "provenance": "actual"}}}
RESULT = {"status": "in_season", "yield_basis": {"moisture": "commercial", "grain_moisture_pct": 13.0},
          "yield_t_ha": {"p10": 3.0, "p50": 4.0, "p90": 5.0}, "ensemble": {"n_years": 14},
          "harvest_date": "2026-06-28",
          "expected_yield": {"status": "ok", "range_t_ha": {"low": 2.4, "high": 3.9}, "coverage": 0.8},
          "inputs": {"weather": {"source": "parcel_daily+archive", "end": "2026-03-14"},
                     "field_management": {"sowing_method": "conventional", "source": "default"}}}


class FakeJobs:
    version = "v-test"
    job_id = staticmethod(SimJobs.job_id)

    def __init__(self, full=False):
        self.jobs, self.started, self.full, self.runs = {}, [], full, []

    def get(self, jid, tenant):
        j = self.jobs.get(jid)
        return j if j and j.tenant_id == tenant else None

    async def start(self, tenant, params, run, today=None):
        self.runs.append(run)
        jid = self.job_id(tenant, params, today, self.version)
        job = self.jobs.get(jid)
        if (job is None or job.state == "failed") and self.full:
            raise SimulationError("too_many_jobs", "m", 429)
        if job is None or job.state == "failed":
            self.started.append(params)
            job = self.jobs[jid] = Job(jid, tenant)
        return job


def loader(cycles=SOWN, crops=CROPS, parcel=PARCEL):
    async def load(tenant, parcel_id):
        return parcel, crops, cycles
    return load


async def call(jobs, load=None, **over):
    kw = {"dao": AsyncMock(), "jobs": jobs, "tenant_id": "t", "parcel_id": PARCEL_ID, "today": TODAY,
          "load": load or loader()}
    return await in_season.parcel_in_season(**{**kw, **over})


async def test_unsown_parcel_never_starts_a_job():
    jobs = FakeJobs()
    planned = {"current": {**SOWN["current"], "start": {"date": "2025-10-20", "provenance": "planned"}}}
    out = await call(jobs, load=loader(cycles=planned))
    assert out["state"] == "unavailable" and out["season"] is None
    assert out["unavailable"] == [{"provider_id": "aquacrop", "reason": "not_sown"}]
    assert jobs.started == []


async def test_a_sown_parcel_starts_the_shared_simulation_job():
    jobs = FakeJobs()
    out = await call(jobs)
    assert out["state"] == "queued" and out["job_id"]
    assert jobs.started == [{"parcel_id": PARCEL_ID, "crop_slug": "wheat", "sowing_date": "2025-10-20",
                             "irrigation": "rainfed", "engine": "aquacrop"}]
    again = await call(jobs)
    assert again["job_id"] == out["job_id"] and len(jobs.started) == 1


async def test_done_job_answers_experimental_estimates_and_the_season():
    jobs = FakeJobs()
    out = await call(jobs)
    job = jobs.jobs[out["job_id"]]
    finished = datetime(2026, 3, 15, 6, 0, tzinfo=UTC)
    job.state, job.result, job.finished_at = "done", RESULT, finished
    out = await call(jobs)
    assert out["state"] == "done"
    gap = next(e for e in out["estimates"] if e["basis"] == "gap_corrected")
    assert gap["validation"]["status"] == "experimental"
    assert out["season"] == {
        "status": "in_season", "as_of": "2026-03-14", "crop": "TRZAX", "regime": "rainfed",
        "sowing": {"date": "2025-10-20", "source": "crop_cycle", "provenance": "actual"},
        "sowing_method": "conventional", "sowing_method_source": "default",
        "harvest_date": "2026-06-28", "n_years": 14, "weather_source": "parcel_daily+archive",
        "computed_at": finished.isoformat(), "provider_version": "v-test"}


async def test_a_failed_job_is_reported_and_only_retry_relaunches_it():
    jobs = FakeJobs()
    out = await call(jobs)
    job = jobs.jobs[out["job_id"]]
    job.state, job.error = "failed", {"code": "soil_unavailable"}
    out = await call(jobs)
    assert out["state"] == "failed"
    assert out["unavailable"] == [{"provider_id": "aquacrop", "reason": "engine_error", "code": "soil_unavailable"}]
    assert len(jobs.started) == 1
    await call(jobs, retry=True)
    assert len(jobs.started) == 2


async def test_a_busy_tenant_starts_nothing():
    out = await call(FakeJobs(full=True))
    assert out["state"] == "unavailable"
    assert out["unavailable"] == [{"provider_id": "aquacrop", "reason": "busy"}]


async def test_a_missing_parcel_is_parcel_not_found():
    async def load(tenant, parcel_id):
        raise httpx.HTTPStatusError("nf", request=httpx.Request("GET", "http://x"),
                                    response=httpx.Response(404))
    with pytest.raises(ParcelNotFound):
        await call(FakeJobs(), load=load)


URL = f"/api/graph/planning/parcels/{PARCEL_ID}/estimates/in-season"


@pytest.fixture
def client(monkeypatch):
    from unittest.mock import MagicMock

    from fastapi.testclient import TestClient

    from app.main import app
    from neo4j import AsyncDriver
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    monkeypatch.setattr("app.core.knowledge.get_driver", lambda: MagicMock(spec=AsyncDriver))
    return TestClient(app)


def test_route_requires_a_verified_tenant(client, monkeypatch):
    import app.api.v1.planning as planning_api
    monkeypatch.setenv("AUTH_DISABLED", "false")  # the conftest default disables auth
    called = AsyncMock()
    monkeypatch.setattr(planning_api, "parcel_in_season", called)
    assert client.get(URL).status_code == 401 and not called.called


def test_route_answers_the_2_1_contract(client, monkeypatch):
    import app.api.v1.planning as planning_api
    monkeypatch.setattr(planning_api, "parcel_in_season", AsyncMock(return_value={
        "state": "unavailable", "done": 0, "total": None, "estimates": [],
        "unavailable": [{"provider_id": "aquacrop", "reason": "not_sown"}], "job_id": None, "season": None}))
    r = client.get(URL)
    assert r.status_code == 200 and r.json()["contract_version"] == "2.1"
    assert r.json()["unavailable"] == [{"provider_id": "aquacrop", "reason": "not_sown", "code": None}]


def test_route_404_for_unknown_parcel(client, monkeypatch):
    import app.api.v1.planning as planning_api
    monkeypatch.setattr(planning_api, "parcel_in_season", AsyncMock(side_effect=ParcelNotFound("x")))
    assert client.get(URL).status_code == 404


# ---- review fixes (2026-10-13)
async def test_an_unreachable_broker_fails_with_a_code_not_a_500():
    async def down(tenant, parcel_id):
        raise httpx.ConnectError("down")

    async def busy(tenant, parcel_id):
        raise httpx.HTTPStatusError("x", request=httpx.Request("GET", "http://x"), response=httpx.Response(503))

    for load, code in ((down, "ConnectError"), (busy, "http_503")):
        out = await call(FakeJobs(), load=load)
        assert out["state"] == "failed" and out["job_id"] is None and out["season"] is None
        assert out["unavailable"] == [{"provider_id": "aquacrop", "reason": "parcel_unavailable", "code": code}]


async def test_the_job_runs_the_engine_with_the_sown_date_only():
    from unittest.mock import ANY

    from app.planning import service
    jobs = FakeJobs()
    dao = AsyncMock()
    dao.run_crop_simulation.return_value = RESULT
    await call(jobs, dao=dao)
    # the very identity of the preseason and crop-simulation jobs: identical inputs share one
    assert jobs.started == [service._job_params(PARCEL_ID, "wheat", date(2025, 10, 20))]
    assert await jobs.runs[-1](lambda done, total: None) == RESULT
    dao.run_crop_simulation.assert_awaited_once_with(
        parcel_id=PARCEL_ID, tenant_id="t", crop_slug="wheat", sowing_date_str="2025-10-20",
        irrigation="rainfed", engine="aquacrop", progress=ANY)
