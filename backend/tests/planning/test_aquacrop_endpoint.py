"""/planning/.../estimates/aquacrop with a fake job store: no engine runs."""
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.planning import service
from app.services.sim_jobs import Job, SimJobs
from neo4j import AsyncDriver

TODAY = datetime.now(UTC).date()
SOW = date.fromordinal(TODAY.toordinal() + 30)
PARCEL = "urn:ngsi-ld:AgriParcel:t:p1"
ENV = {"parcel_id": PARCEL, "country": "ES"}
RESULT = {"status": "preseason", "yield_basis": {"moisture": "commercial", "grain_moisture_pct": 13.0},
          "yield_t_ha": {"p10": 2.0, "p50": 3.0, "p90": 4.0}, "ensemble": {"n_years": 12},
          "expected_yield": {"status": "ok", "range_t_ha": {"low": 2.5, "high": 4.5}, "coverage": 0.78}}


class FakeJobs:
    """Same surface as SimJobs for the provider: job_id/get/start, _version."""
    _version = "v-test"
    job_id = staticmethod(SimJobs.job_id)

    def __init__(self):
        self.jobs: dict[str, Job] = {}
        self.started: list[dict] = []

    def get(self, jid, tenant):
        j = self.jobs.get(jid)
        return j if j and j.tenant_id == tenant else None

    async def start(self, tenant, params, run, today=None):
        jid = self.job_id(tenant, params, today, self._version)
        job = self.jobs.get(jid)
        if job is None or job.state == "failed":
            self.started.append(params)  # a launch, as the real store: joining a live job is not one
            job = self.jobs[jid] = Job(jid, tenant)  # queued
        return job

    def only(self):
        (job,) = self.jobs.values()
        return job


def _dao():
    dao = AsyncMock()
    dao.get_parcel_environment.return_value = ENV
    return dao


async def _call(jobs, dao=None, **over):
    kw = {"dao": dao or _dao(), "jobs": jobs, "tenant_id": "t", "parcel_id": PARCEL, "crop": "wheat",
          "regime": "secano", "sowing_date": SOW, "today": TODAY}
    return await service.parcel_aquacrop(**{**kw, **over})


async def test_gated_inputs_never_start_a_job():
    jobs = FakeJobs()
    out = await _call(jobs, crop="barley")
    assert out["state"] == "unavailable" and out["job_id"] is None
    assert out["unavailable"] == [{"provider_id": "aquacrop", "reason": "not_validated"}]
    out = await _call(jobs, sowing_date=None)
    assert out["unavailable"][0]["reason"] == "needs_sowing_date"
    assert jobs.started == []


async def test_queued_then_running_then_done_with_the_same_job():
    jobs = FakeJobs()
    first = await _call(jobs)
    assert first["state"] == "queued" and first["estimates"] == [] and first["job_id"]
    job = jobs.only()
    job.state, job.done, job.total = "running", 3, 12
    mid = await _call(jobs)
    assert (mid["state"], mid["done"], mid["total"], mid["job_id"]) == ("running", 3, 12, first["job_id"])
    job.state, job.result = "done", RESULT
    done = await _call(jobs)
    assert done["state"] == "done" and [e["basis"] for e in done["estimates"]] == ["gap_corrected", "water_limited"]
    assert done["estimates"][0]["provider_version"] == "v-test"


async def test_job_params_are_those_of_the_crop_simulation_jobs_route():
    jobs = FakeJobs()
    await _call(jobs)
    assert jobs.started == [{"parcel_id": PARCEL, "crop_slug": "wheat", "sowing_date": SOW.isoformat(),
                             "irrigation": "rainfed", "engine": "aquacrop"}]


async def test_failed_job_is_reported_not_relaunched_or_emptied():
    jobs = FakeJobs()
    await _call(jobs)
    job = jobs.only()
    job.state, job.error = "failed", {"code": "soil_unavailable", "message": "m", "status_code": 503}
    for _ in range(2):
        out = await _call(jobs)
        assert out["state"] == "failed" and out["estimates"] == []
        assert out["unavailable"] == [{"provider_id": "aquacrop", "reason": "engine_error", "code": "soil_unavailable"}]
    assert len(jobs.started) == 1


async def test_failed_job_without_retry_never_calls_start_again():
    jobs = FakeJobs()
    await _call(jobs)
    jobs.only().state = "failed"
    jobs.only().error = {"code": "c"}
    await _call(jobs)
    assert len(jobs.started) == 1


async def test_failed_job_with_retry_relaunches_once():
    jobs = FakeJobs()
    await _call(jobs)
    jobs.only().state = "failed"
    jobs.only().error = {"code": "c"}
    out = await _call(jobs, retry=True)
    assert out["state"] == "queued" and out["unavailable"] == [] and len(jobs.started) == 2


async def test_retry_on_a_done_or_running_job_changes_nothing():
    jobs = FakeJobs()
    await _call(jobs)
    job = jobs.only()
    job.state, job.result = "done", RESULT
    out = await _call(jobs, retry=True)
    assert out["state"] == "done" and len(jobs.started) == 1
    job.state = "running"
    assert (await _call(jobs, retry=True))["state"] == "running" and len(jobs.started) == 1


async def test_poll_by_job_id_skips_gate_and_environment():
    jobs = FakeJobs()
    first = await _call(jobs)
    jobs.only().state, jobs.only().done, jobs.only().total = "running", 2, 12
    dao = _dao()
    out = await _call(jobs, dao=dao, job_id=first["job_id"])
    assert out["state"] == "running" and out["done"] == 2 and out["job_id"] == first["job_id"]
    dao.get_parcel_environment.assert_not_called()


@pytest.mark.parametrize("other", [{"crop": "barley"}, {"sowing_date": date.fromordinal(SOW.toordinal() + 1)},
                                   {"parcel_id": "urn:ngsi-ld:AgriParcel:t:p2"}])
async def test_a_job_id_of_other_inputs_never_answers_for_these(other):
    # The id is a hash of the inputs: a stale or foreign id must not show another parcel's, crop's or date's result.
    jobs = FakeJobs()
    first = await _call(jobs)
    jobs.only().state, jobs.only().result = "done", RESULT
    dao = _dao()
    if "parcel_id" in other:
        dao.get_parcel_environment.return_value = {"parcel_id": other["parcel_id"], "country": "ES"}
    out = await _call(jobs, dao=dao, job_id=first["job_id"], **other)
    dao.get_parcel_environment.assert_awaited_once()
    assert out["job_id"] != first["job_id"]


async def test_poll_by_job_id_reports_done_and_failed_without_relaunch():
    jobs = FakeJobs()
    first = await _call(jobs)
    job = jobs.only()
    job.state, job.result = "done", RESULT
    out = await _call(jobs, dao=_dao(), job_id=first["job_id"])
    assert [e["basis"] for e in out["estimates"]] == ["gap_corrected", "water_limited"]
    job.state, job.error = "failed", {"code": "c"}
    out = await _call(jobs, job_id=first["job_id"])
    assert out["unavailable"][0]["reason"] == "engine_error" and len(jobs.started) == 1


async def test_unknown_job_id_runs_the_full_path():
    jobs = FakeJobs()
    dao = _dao()
    out = await _call(jobs, dao=dao, job_id="gone")
    assert out["state"] == "queued" and len(jobs.started) == 1
    dao.get_parcel_environment.assert_awaited_once()


async def test_failed_job_id_with_retry_takes_the_full_path():
    jobs = FakeJobs()
    first = await _call(jobs)
    jobs.only().state, jobs.only().error = "failed", {"code": "c"}
    out = await _call(jobs, job_id=first["job_id"], retry=True)
    assert out["state"] == "queued" and len(jobs.started) == 2


async def test_a_lost_job_restarts_transparently():
    jobs = FakeJobs()
    first = await _call(jobs)
    jobs.jobs.clear()  # pod restart
    again = await _call(jobs)
    assert again["state"] == "queued" and again["job_id"] == first["job_id"] and len(jobs.started) == 2


async def test_unknown_parcel_and_crop_propagate():
    dao = _dao()
    dao.get_parcel_environment.return_value = {"error": "nope"}
    with pytest.raises(service.ParcelNotFound):
        await _call(FakeJobs(), dao=dao)
    with pytest.raises(service.UnknownCrop):
        await _call(FakeJobs(), crop="zzz")


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    monkeypatch.setattr("app.core.knowledge.get_driver", lambda: MagicMock(spec=AsyncDriver))
    return TestClient(app)


def test_route_validates_the_contract(client, monkeypatch):
    import app.api.v1.planning as planning_api
    out = {"state": "running", "done": 2, "total": 12, "estimates": [], "unavailable": [], "job_id": "j"}
    called = AsyncMock(return_value=out)
    monkeypatch.setattr(planning_api, "parcel_aquacrop", called)
    r = client.get(f"/api/graph/planning/parcels/{PARCEL}/estimates/aquacrop",
                   params={"crop": "TRZAX", "regime": "secano", "management": "conventional",
                           "sowing_date": SOW.isoformat()})
    assert r.status_code == 200
    body = r.json()
    assert body["provider_id"] == "aquacrop" and body["contract_version"] == "1.1" and body["state"] == "running"
    assert called.await_args.kwargs["sowing_date"] == SOW


def test_route_forwards_retry(client, monkeypatch):
    import app.api.v1.planning as planning_api
    called = AsyncMock(return_value={"state": "queued", "estimates": [], "unavailable": [], "job_id": "j"})
    monkeypatch.setattr(planning_api, "parcel_aquacrop", called)
    url = f"/api/graph/planning/parcels/{PARCEL}/estimates/aquacrop"
    params = {"crop": "TRZAX", "regime": "secano", "management": "conventional"}
    client.get(url, params=params)
    assert called.await_args.kwargs["retry"] is False
    client.get(url, params={**params, "retry": "true", "job_id": "abc"})
    assert called.await_args.kwargs["retry"] is True and called.await_args.kwargs["job_id"] == "abc"


def test_route_maps_unknown_parcel_and_crop(client, monkeypatch):
    import app.api.v1.planning as planning_api
    params = {"crop": "TRZAX", "regime": "secano", "management": "conventional"}
    url = f"/api/graph/planning/parcels/{PARCEL}/estimates/aquacrop"
    monkeypatch.setattr(planning_api, "parcel_aquacrop", AsyncMock(side_effect=service.ParcelNotFound("x")))
    assert client.get(url, params=params).status_code == 404
    monkeypatch.setattr(planning_api, "parcel_aquacrop", AsyncMock(side_effect=service.UnknownCrop("x")))
    assert client.get(url, params=params).status_code == 422
