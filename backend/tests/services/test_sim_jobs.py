"""Asynchronous crop simulations: one job per distinct request per day, with progress."""
import asyncio
from datetime import date

import pytest

from app.services import sim_jobs as sj
from app.services.sim_errors import SimulationError

DAY = date(2026, 10, 11)
PARAMS = {"parcel_id": "urn:p", "crop_slug": "wheat", "sowing_date": "2026-11-01",
          "irrigation": "rainfed", "engine": "aquacrop"}


async def _finish(jobs, job_id, tenant="t1"):
    for _ in range(200):
        job = jobs.get(job_id, tenant)
        if job.state in ("done", "failed"):
            return job
        await asyncio.sleep(0.01)
    raise AssertionError("job did not finish")


def _ok(calls, total=2):
    async def run(progress):
        calls.append(1)
        progress(0, total)
        for i in range(total):
            progress(i + 1, total)
        return {"ok": True}
    return run


@pytest.mark.asyncio
async def test_identical_inputs_on_the_same_day_share_one_job_and_one_run():
    jobs, calls = sj.SimJobs(version="v1"), []
    a = await jobs.start("t1", PARAMS, _ok(calls), today=DAY)
    b = await jobs.start("t1", dict(PARAMS), _ok(calls), today=DAY)
    assert a.id == b.id
    job = await _finish(jobs, a.id)
    assert (job.state, job.done, job.total, job.result) == ("done", 2, 2, {"ok": True})
    again = await jobs.start("t1", PARAMS, _ok(calls), today=DAY)
    assert again.id == a.id and again.state == "done" and calls == [1]  # the cache answers


@pytest.mark.asyncio
async def test_a_new_day_or_another_version_is_a_new_job():
    jobs = sj.SimJobs(version="v1")
    a = await jobs.start("t1", PARAMS, _ok([]), today=DAY)
    b = await jobs.start("t1", PARAMS, _ok([]), today=date(2026, 10, 12))
    assert a.id != b.id
    assert sj.SimJobs.job_id("t1", PARAMS, DAY, "v1") != sj.SimJobs.job_id("t1", PARAMS, DAY, "v2")


@pytest.mark.asyncio
async def test_another_tenant_never_sees_the_job():
    jobs = sj.SimJobs(version="v1")
    a = await jobs.start("t1", PARAMS, _ok([]), today=DAY)
    await _finish(jobs, a.id)
    assert jobs.get(a.id, "t2") is None
    assert sj.SimJobs.job_id("t2", PARAMS, DAY, "v1") != a.id


@pytest.mark.asyncio
async def test_a_failed_job_carries_the_error_and_is_retried():
    jobs, calls = sj.SimJobs(version="v1"), []

    async def fails(progress):
        calls.append(1)
        raise SimulationError("weather_gaps", "Weather is missing for 2 day(s).")

    a = await jobs.start("t1", PARAMS, fails, today=DAY)
    job = await _finish(jobs, a.id)
    assert job.state == "failed" and job.result is None
    assert job.error == {"code": "weather_gaps", "message": "Weather is missing for 2 day(s).",
                         "status_code": 422}
    b = await jobs.start("t1", PARAMS, _ok(calls), today=DAY)
    assert b.id == a.id
    assert (await _finish(jobs, b.id)).state == "done" and calls == [1, 1]


@pytest.mark.asyncio
async def test_an_unexpected_error_is_reported_without_internals():
    jobs = sj.SimJobs(version="v1")

    async def boom(progress):
        raise RuntimeError("secret detail")

    job = await _finish(jobs, (await jobs.start("t1", PARAMS, boom, today=DAY)).id)
    assert job.error["code"] == "internal_error" and job.error["status_code"] == 500
    assert "secret" not in job.error["message"]


@pytest.mark.asyncio
async def test_one_simulation_runs_at_a_time():
    jobs, gate = sj.SimJobs(version="v1"), asyncio.Event()

    async def slow(progress):
        await gate.wait()
        return {"ok": 1}

    a = await jobs.start("t1", PARAMS, slow, today=DAY)
    b = await jobs.start("t1", {**PARAMS, "crop_slug": "barley"}, _ok([]), today=DAY)
    await asyncio.sleep(0.05)
    assert (jobs.get(a.id, "t1").state, jobs.get(b.id, "t1").state) == ("running", "queued")
    gate.set()
    assert (await _finish(jobs, a.id)).state == "done" and (await _finish(jobs, b.id)).state == "done"


@pytest.mark.asyncio
async def test_the_store_evicts_the_oldest_finished_jobs():
    jobs = sj.SimJobs(version="v1", max_jobs=2)
    ids = []
    for crop in ("wheat", "barley", "maize"):
        job = await jobs.start("t1", {**PARAMS, "crop_slug": crop}, _ok([]), today=DAY)
        await _finish(jobs, job.id)
        ids.append(job.id)
    assert jobs.get(ids[0], "t1") is None and jobs.get(ids[2], "t1") is not None


@pytest.mark.asyncio
async def test_view_is_the_polling_shape():
    jobs = sj.SimJobs(version="v1")
    job = await _finish(jobs, (await jobs.start("t1", PARAMS, _ok([]), today=DAY)).id)
    assert sj.SimJobs.view(job) == {"job_id": job.id, "state": "done", "done": 2, "total": 2,
                                    "result": {"ok": True}, "error": None}
