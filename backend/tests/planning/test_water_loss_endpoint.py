"""/planning/.../estimates/water-loss-by-stage with fake data loading and a fake job store: no engine runs."""
import asyncio
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from app.planning import in_season, water_loss
from app.planning import models as planning_models
from app.planning.service import ParcelNotFound
from app.services.sim_errors import SimulationError
from app.services.sim_jobs import Job, SimJobs

TODAY = date(2026, 3, 15)
PARCEL_ID = "urn:ngsi-ld:AgriParcel:acme:p1"
PARCEL = {"id": PARCEL_ID, "irrigationRegime": "rainfed",
          "location": {"type": "Polygon", "coordinates": [[[-1.6, 42.8], [-1.5, 42.8], [-1.5, 42.9],
                                                          [-1.6, 42.9], [-1.6, 42.8]]]}}
CROP_ID = "urn:ngsi-ld:AgriCrop:acme:c1"
CROPS = {CROP_ID: {"id": CROP_ID, "species": "TRZAX", "irrigationRegime": "rainfed"}}
SOWN = {"current": {"crop_id": CROP_ID, "species": "TRZAX", "start": {"date": "2025-10-20", "provenance": "actual"}}}
BAND = {"p10": 0.1, "p50": 0.3, "p90": 0.5}
BASIS = {"moisture": "commercial", "grain_moisture_pct": 13.0, "source": "x"}
MILESTONES = {"status": "ok", "items": {
    "emergence": {"gdd": 100.0, "status": "reached", "date": "2025-11-05"},
    "flowering_start": {"gdd": 900.0, "status": "reached", "date": "2026-03-10"},
    "senescence_start": {"gdd": 1500.0, "status": "unavailable", "reason": "no_analog_members"},
    "maturity": {"gdd": 2000.0, "status": "unavailable", "reason": "no_analog_members"}}}
WATER_LOSS = {
    "season": "in_season", "as_of": "2026-03-14", "ensemble": {"n_years": 14, "years": list(range(2008, 2022))},
    "total": {"water_gap_pct": 21.5, "water_gap_band": BAND, "water_limited_t_ha": BAND, "potential_t_ha": BAND},
    "stages": [
        {"index": 1, "key": "establishment", "start": "2025-10-20", "end": "2025-11-20", "state": "complete",
         "gain_t_ha_model": BAND, "gain_pct_of_potential": BAND, "end_in_relief_run": None},
        {"index": 2, "key": "canopy_growth", "start": "2025-11-21", "end": "2026-02-20", "state": "complete",
         "gain_t_ha_model": 0.2, "gain_pct_of_potential": 3.0, "end_in_relief_run": "2026-02-22"},
        {"index": 3, "key": "max_canopy", "start": "2026-02-21", "end": None, "state": "in_progress",
         "gain_t_ha_model": None, "gain_pct_of_potential": None, "end_in_relief_run": None},
        {"index": 4, "key": "senescence", "start": None, "end": None, "state": "not_reached",
         "gain_t_ha_model": None, "gain_pct_of_potential": None, "end_in_relief_run": None}],
    "engine_version": "3.1.0", "warnings": ["stage warning"]}
RESULT = {"water_loss": WATER_LOSS, "milestones": MILESTONES, "inputs": {}, "yield_basis": BASIS,
          "warnings": ["input warning"]}


@pytest.fixture(autouse=True)
def in_season_switch_on(monkeypatch):
    """The route answers only finished campaigns unless this switch is on; these tests cover both."""
    monkeypatch.setenv("WATER_LOSS_IN_SEASON", "true")


class FakeJobs:
    version = "v-test|wl1"
    job_id = staticmethod(SimJobs.job_id)

    def version_for(self, crop_slug=None):
        return self.version

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
    kw = {"dao": AsyncMock(), "jobs": jobs, "tenant_id": "acme", "parcel_id": PARCEL_ID, "today": TODAY,
          "load": load or loader()}
    return await water_loss.parcel_water_loss(**{**kw, **over})


def species(code, regime="rainfed", sowing="2025-10-20"):
    crops = {CROP_ID: {"id": CROP_ID, "species": code, "irrigationRegime": regime}}
    cycles = {"current": {"crop_id": CROP_ID, "species": code, "start": {"date": sowing, "provenance": "actual"}}}
    return loader(cycles=cycles, crops=crops)


def finish(jobs, out, result=RESULT, finished=datetime(2026, 3, 15, 6, 0, tzinfo=UTC)):
    job = jobs.jobs[out["job_id"]]
    job.state, job.result, job.finished_at = "done", result, finished


UNAVAILABLE = [
    ("irrigated_regime", species("TRZAX", "irrigated")),
    ("not_sown", loader(cycles={"current": {**SOWN["current"], "start": {"date": "2025-10-20", "provenance": "planned"}}})),
    ("sowing_in_future", species("TRZAX", sowing="2026-04-01")),
    ("season_too_old", species("TRZAX", sowing="2024-01-01")),
    ("unsupported_crop", species("SECCE")),
]


@pytest.mark.parametrize("reason, load", UNAVAILABLE)
async def test_gaps_answer_unavailable_without_a_job(reason, load):
    jobs = FakeJobs()
    out = await call(jobs, load=load)
    assert out["state"] == "unavailable" and out["job_id"] is None and out["result"] is None
    assert out["unavailable"] == [{"provider_id": "aquacrop_water_loss", "reason": reason}]
    assert jobs.started == []


async def test_irrigated_regime_unavailable():
    out = await call(FakeJobs(), load=species("TRZAX", "irrigated"))
    assert out["unavailable"][0]["reason"] == "irrigated_regime"


async def test_unknown_regime_unavailable():
    crops = {CROP_ID: {"id": CROP_ID, "species": "TRZAX"}}
    parcel = {k: v for k, v in PARCEL.items() if k != "irrigationRegime"}
    jobs = FakeJobs()
    out = await call(jobs, load=loader(crops=crops, parcel=parcel))
    assert out["unavailable"][0]["reason"] == "no_regime" and jobs.started == []


async def test_not_sown_unavailable():
    out = await call(FakeJobs(), load=loader(cycles={"current": None}))
    assert out["state"] == "unavailable" and out["unavailable"][0]["reason"] == "no_committed_crop"


async def test_unsupported_crop_unavailable():
    jobs = FakeJobs()
    out = await call(jobs, load=species("SECCE"))
    assert out["unavailable"][0]["reason"] == "unsupported_crop" and jobs.started == []


async def test_a_sown_rainfed_parcel_starts_its_own_job_in_water_loss_mode():
    jobs = FakeJobs()
    out = await call(jobs)
    assert out["state"] == "queued" and out["job_id"] and out["result"] is None and out["season"] is None
    assert jobs.started == [{"parcel_id": PARCEL_ID, "crop_slug": "wheat", "sowing_date": "2025-10-20",
                             "irrigation": "rainfed", "engine": "aquacrop", "kind": "water_loss_by_stage",
                             "cycle": "current"}]
    assert (await call(jobs))["job_id"] == out["job_id"] and len(jobs.started) == 1
    dao = AsyncMock()
    dao.run_crop_simulation.return_value = RESULT
    await call(jobs, dao=dao)
    assert await jobs.runs[-1](lambda done, total: None) == RESULT
    kw = dao.run_crop_simulation.call_args.kwargs
    assert kw["mode"] == "water_loss_by_stage" and kw["irrigation"] == "rainfed" and kw["engine"] == "aquacrop"
    assert kw["crop_slug"] == "wheat" and kw["sowing_date_str"] == "2025-10-20"


async def test_done_response_shape_for_a_validated_crop():
    jobs = FakeJobs()
    out = await call(jobs)
    finish(jobs, out)
    got = await call(jobs)
    assert got["state"] == "done"
    assert got["season"] == {"status": "in_season", "as_of": "2026-03-14", "crop": "TRZAX", "regime": "rainfed",
                             "sowing": {"date": "2025-10-20", "source": "crop_cycle", "provenance": "actual"},
                             "n_years": 14,
                             "ensemble": {"n_years": 14, "years": list(range(2008, 2022))},
                             "cycle": "current", "harvest_recorded": None,
                             "computed_at": "2026-03-15T06:00:00+00:00",
                             "provider_version": "v-test|wl1"}
    r = got["result"]
    assert r["validation"]["status"] == "validated_forecast_crop" and r["validation"]["note"]
    assert r["label"] == "model estimate; no observed data arbitrates it"
    assert r["notes"] == [
        "stage gains do not add up: keeping the soil wet in one stage leaves it full for the next",
        ("in season: observed weather up to as_of, rest of season from N reference years; a finished "
         "stage's figure still depends on the weather after it and changes until harvest"),
        "stages follow canopy development; flowering and grain fill fall across max_canopy and senescence"]
    assert r["sources"] and all({"short", "detail"} <= set(s) for s in r["sources"])
    assert [x["short"] for x in r["sources"]] == ["AquaCrop-OSPy", "AquaCrop-OS"]
    assert "doi:10.1016/j.agwat.2021.106976" in r["sources"][0]["detail"]
    assert "doi:10.1016/j.agwat.2016.11.015" in r["sources"][1]["detail"]
    assert r["engine_version"] == "3.1.0"
    assert r["stage_bounds_basis"] == "rainfed_simulation"
    assert r["milestones_basis"] == "thermal_time_no_stress_delay"
    assert r["yield_basis"] == BASIS and r["warnings"] == ["input warning"]
    assert r["total"] == WATER_LOSS["total"]
    assert [s["state"] for s in r["stages"]] == ["complete", "complete", "in_progress", "not_reached"]
    assert r["stages"][1]["end_in_relief_run"] == "2026-02-22"


async def test_other_cereal_is_unvalidated():
    jobs = FakeJobs()
    out = await call(jobs, load=species("HORVX"))
    finish(jobs, out)
    r = (await call(jobs, load=species("HORVX")))["result"]
    assert r["validation"] == {"status": "unvalidated", "note": (
        "crop, regime or region outside the validated forecast; figures are model output only")}
    assert r["milestones_basis"] == "thermal_time_no_stress_delay"


async def test_milestones_only_reached_and_placed_in_their_stage():
    jobs = FakeJobs()
    out = await call(jobs)
    finish(jobs, out)
    stages = (await call(jobs))["result"]["stages"]
    by = {s["index"]: s["milestones"] for s in stages}
    emergence = {"key": "emergence", "date": "2025-11-05", "basis": "thermal_time_no_stress_delay"}
    flowering = {"key": "flowering_start", "date": "2026-03-10", "basis": "thermal_time_no_stress_delay"}
    # unreached milestones (senescence, maturity) never appear; an in-progress stage runs to as_of
    assert by == {1: [emergence], 2: [], 3: [flowering], 4: []}
    assert all(s["milestones_reason"] is None for s in stages)


async def test_non_cereal_milestones_null():
    jobs = FakeJobs()
    load = species("ZEAMX")
    out = await call(jobs, load=load)
    result = {**RESULT, "milestones": {"status": "unavailable", "reason": "no_operative_calendar", "items": {}}}
    finish(jobs, out, result)
    stages = (await call(jobs, load=load))["result"]["stages"]
    assert all(s["milestones"] is None and s["milestones_reason"] == "no_operative_calendar" for s in stages)


async def test_failed_job_reports_its_code_and_only_retry_relaunches():
    jobs = FakeJobs()
    out = await call(jobs)
    job = jobs.jobs[out["job_id"]]
    job.state, job.error = "failed", {"code": "stage_bounds_mismatch"}
    out = await call(jobs)
    assert out["state"] == "failed" and out["result"] is None
    assert out["unavailable"] == [{"provider_id": "aquacrop_water_loss", "reason": "engine_error",
                                   "code": "stage_bounds_mismatch"}]
    assert len(jobs.started) == 1
    await call(jobs, retry=True)
    assert len(jobs.started) == 2


async def test_a_busy_queue_is_unavailable_busy():
    out = await call(FakeJobs(full=True))
    assert out["state"] == "unavailable"
    assert out["unavailable"] == [{"provider_id": "aquacrop_water_loss", "reason": "busy"}]


async def test_a_missing_parcel_is_parcel_not_found():
    async def load(tenant, parcel_id):
        raise httpx.HTTPStatusError("nf", request=httpx.Request("GET", "http://x"), response=httpx.Response(404))
    with pytest.raises(ParcelNotFound):
        await call(FakeJobs(), load=load)


async def test_an_unreachable_broker_fails_with_a_code():
    async def down(tenant, parcel_id):
        raise httpx.ConnectError("down")
    out = await call(FakeJobs(), load=down)
    assert out["state"] == "failed"
    assert out["unavailable"] == [{"provider_id": "aquacrop_water_loss", "reason": "parcel_unavailable",
                                   "code": "ConnectError"}]


async def test_in_season_not_busy_while_water_loss_pending():
    """The water-loss queue is its own: a pending job there never makes the in-season route answer busy."""
    gate = asyncio.Event()

    async def slow(progress):
        await gate.wait()
        return RESULT

    wl_jobs = water_loss.water_loss_jobs()
    assert wl_jobs is water_loss.water_loss_jobs()
    wl = SimJobs(version="v|wl1", max_concurrent=1, max_pending_per_tenant=1)
    other = SimJobs(version="v", max_concurrent=1, max_pending_per_tenant=1)
    dao = AsyncMock()
    dao.run_crop_simulation.return_value = {}
    pending = await wl.start("acme", {"parcel_id": PARCEL_ID, "kind": "water_loss_by_stage"}, slow, TODAY)
    assert pending.state in ("queued", "running")
    out = await in_season.parcel_in_season(dao=dao, jobs=other, tenant_id="acme", parcel_id=PARCEL_ID,
                                           today=TODAY, load=loader())
    assert out["state"] in ("queued", "running") and out["unavailable"] == []
    # and the water-loss queue itself is full for a second parcel of the same tenant
    second = await water_loss.parcel_water_loss(dao=dao, jobs=wl, tenant_id="acme",
                                                parcel_id="urn:ngsi-ld:AgriParcel:acme:p2",
                                                today=TODAY, load=loader())
    assert second["unavailable"] == [{"provider_id": "aquacrop_water_loss", "reason": "busy"}]
    gate.set()
    await pending.task


PREVIOUS = {"crop_id": CROP_ID, "species": "TRZAX",
            "start": {"date": "2024-10-20", "provenance": "actual"},
            "end": {"date": "2025-07-01", "provenance": "actual"}}
COMPLETE = {**RESULT, "water_loss": {**WATER_LOSS, "season": "complete", "as_of": "2025-12-31", "ensemble": None}}


def cycles_of(current=SOWN["current"], previous=PREVIOUS):
    return loader(cycles={"current": current, "previous": previous})


def off(monkeypatch):
    monkeypatch.setenv("WATER_LOSS_IN_SEASON", "false")


async def test_switch_off_answers_the_finished_previous_campaign(monkeypatch):
    off(monkeypatch)
    jobs = FakeJobs()
    out = await call(jobs, load=cycles_of())
    assert out["state"] == "queued"
    assert jobs.started == [{"parcel_id": PARCEL_ID, "crop_slug": "wheat", "sowing_date": "2024-10-20",
                             "irrigation": "rainfed", "engine": "aquacrop", "kind": "water_loss_by_stage",
                             "cycle": "previous"}]
    finish(jobs, out, COMPLETE)
    got = await call(jobs, load=cycles_of())
    assert got["season"]["cycle"] == "previous" and got["season"]["harvest_recorded"] == "2025-07-01"
    assert got["season"]["status"] == "complete" and got["season"]["sowing"]["date"] == "2024-10-20"
    dao = AsyncMock()
    dao.run_crop_simulation.return_value = COMPLETE
    await call(jobs, dao=dao, load=cycles_of())
    await jobs.runs[-1](lambda done, total: None)
    kw = dao.run_crop_simulation.call_args.kwargs
    assert kw["allow_ensemble"] is False and kw["sowing_date_str"] == "2024-10-20"


@pytest.mark.parametrize("previous", [
    None,
    {**PREVIOUS, "end": {"date": "2025-03-14", "provenance": "actual"}},  # 366 days ago
    {**PREVIOUS, "end": {"date": "2025-07-01", "provenance": "planned"}},
    {**PREVIOUS, "end": {"date": "2026-03-16", "provenance": "actual"}},  # in the future
    {**PREVIOUS, "end": None},
    {**PREVIOUS, "start": {"date": "2024-10-20", "provenance": "planned"}},
])
async def test_switch_off_without_a_finished_campaign(monkeypatch, previous):
    off(monkeypatch)
    jobs = FakeJobs()
    out = await call(jobs, load=cycles_of(previous=previous))
    assert out["state"] == "unavailable" and jobs.started == []
    assert out["unavailable"] == [{"provider_id": "aquacrop_water_loss", "reason": "no_finished_season"}]


async def test_switch_off_sowing_after_the_recorded_harvest_is_not_a_finished_season(monkeypatch):
    off(monkeypatch)
    inverted = {**PREVIOUS, "start": {"date": "2025-08-01", "provenance": "actual"}}  # harvest 2025-07-01
    jobs = FakeJobs()
    out = await call(jobs, load=cycles_of(previous=inverted))
    assert jobs.started == []
    assert out["unavailable"] == [{"provider_id": "aquacrop_water_loss", "reason": "no_finished_season"}]


async def test_switch_off_boundaries_and_old_sowing(monkeypatch):
    off(monkeypatch)
    edge = {**PREVIOUS, "end": {"date": "2025-03-15", "provenance": "actual"}}  # exactly 365 days ago
    jobs = FakeJobs()
    assert (await call(jobs, load=cycles_of(previous=edge)))["state"] == "queued"
    old = {**PREVIOUS, "start": {"date": "2023-01-01", "provenance": "declared"}}  # sown > 366 d ago
    assert (await call(FakeJobs(), load=cycles_of(previous=old)))["state"] == "queued"


async def test_switch_off_cycles_and_regime_codes(monkeypatch):
    off(monkeypatch)
    assert (await call(FakeJobs(), load=loader(cycles=None)))["unavailable"][0]["reason"] == "cycles_unavailable"
    assert (await call(FakeJobs(), load=loader(cycles={"current": None})))["unavailable"][0]["reason"] == "no_finished_season"
    no_species = {**PREVIOUS, "species": "NOPE1"}
    assert (await call(FakeJobs(), load=cycles_of(previous=no_species)))["unavailable"][0]["reason"] == "unknown_crop"
    no_date = {**PREVIOUS, "start": {"date": None, "provenance": "actual"}}
    assert (await call(FakeJobs(), load=cycles_of(previous=no_date)))["unavailable"][0]["reason"] == "no_sowing_date"
    irrigated = loader(cycles={"current": None, "previous": PREVIOUS},
                       crops={CROP_ID: {"id": CROP_ID, "species": "TRZAX", "irrigationRegime": "irrigated"}})
    assert (await call(FakeJobs(), load=irrigated))["unavailable"][0]["reason"] == "irrigated_regime"
    parcel = {k: v for k, v in PARCEL.items() if k != "irrigationRegime"}
    unknown = loader(cycles={"previous": PREVIOUS}, crops={CROP_ID: {"id": CROP_ID, "species": "TRZAX"}},
                     parcel=parcel)
    assert (await call(FakeJobs(), load=unknown))["unavailable"][0]["reason"] == "no_regime"


async def test_switch_off_ignores_the_current_in_season_crop(monkeypatch):
    off(monkeypatch)
    jobs = FakeJobs()
    await call(jobs, load=cycles_of())
    assert [p["cycle"] for p in jobs.started] == ["previous"]


async def test_switch_on_current_sown_is_the_current_behaviour():
    jobs = FakeJobs()
    out = await call(jobs, load=cycles_of())
    assert [p["cycle"] for p in jobs.started] == ["current"] and jobs.started[0]["sowing_date"] == "2025-10-20"
    finish(jobs, out)
    got = await call(jobs, load=cycles_of())
    assert got["season"]["cycle"] == "current" and got["season"]["harvest_recorded"] is None
    dao = AsyncMock()
    dao.run_crop_simulation.return_value = RESULT
    await call(jobs, dao=dao, load=cycles_of())
    await jobs.runs[-1](lambda done, total: None)
    assert dao.run_crop_simulation.call_args.kwargs["allow_ensemble"] is True


async def test_switch_on_without_current_uses_the_finished_previous():
    jobs = FakeJobs()
    out = await call(jobs, load=cycles_of(current=None))
    assert out["state"] == "queued" and jobs.started[0]["cycle"] == "previous"
    # nothing finished either: the current cycle's own reason stays
    gap = await call(FakeJobs(), load=loader(cycles={"current": None}))
    assert gap["unavailable"][0]["reason"] == "no_committed_crop"


async def test_current_and_previous_jobs_never_share_an_id(monkeypatch):
    jobs = FakeJobs()
    cur = await call(jobs, load=cycles_of())
    off(monkeypatch)
    prev = await call(jobs, load=cycles_of())
    assert cur["job_id"] != prev["job_id"]


async def test_a_season_not_finished_job_is_unavailable_not_failed(monkeypatch):
    off(monkeypatch)
    jobs = FakeJobs()
    out = await call(jobs, load=cycles_of())
    job = jobs.jobs[out["job_id"]]
    job.state, job.error = "failed", {"code": "season_not_finished"}
    got = await call(jobs, load=cycles_of())
    assert got["state"] == "unavailable" and got["result"] is None
    assert got["unavailable"] == [{"provider_id": "aquacrop_water_loss", "reason": "season_not_finished"}]


@pytest.mark.parametrize("raw, expected", [
    (None, False), ("", False), ("false", False), ("true", True), ("TRUE", True), ("1", False), ("yes", False)])
def test_the_switch_reads_the_environment_at_call_time(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("WATER_LOSS_IN_SEASON", raising=False)
    else:
        monkeypatch.setenv("WATER_LOSS_IN_SEASON", raw)
    assert water_loss.in_season_enabled() is expected


def test_an_invalid_switch_value_warns_once(monkeypatch, caplog):
    monkeypatch.setattr(water_loss, "_WARNED", False)
    monkeypatch.setenv("WATER_LOSS_IN_SEASON", "maybe")
    with caplog.at_level("WARNING"):
        assert water_loss.in_season_enabled() is False and water_loss.in_season_enabled() is False
    assert len([r for r in caplog.records if "WATER_LOSS_IN_SEASON" in r.getMessage()]) == 1


def test_the_singleton_is_its_own_queue_with_tight_caps():
    jobs = water_loss.water_loss_jobs()
    assert jobs._max_pending == 1 and jobs._slots._value == 1
    assert jobs.version_for("wheat").endswith("|wl1")
    from app.services.sim_jobs import shared_sim_jobs
    assert jobs is not shared_sim_jobs()


URL = f"/api/graph/planning/parcels/{PARCEL_ID}/estimates/water-loss-by-stage"


@pytest.fixture
def client(monkeypatch):
    from fastapi.testclient import TestClient

    from app.main import app
    from neo4j import AsyncDriver
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    monkeypatch.setattr("app.core.knowledge.get_driver", lambda: MagicMock(spec=AsyncDriver))
    return TestClient(app)


def test_route_requires_a_verified_tenant(client, monkeypatch):
    import app.api.v1.planning as planning_api
    monkeypatch.setenv("AUTH_DISABLED", "false")
    called = AsyncMock()
    monkeypatch.setattr(planning_api, "parcel_water_loss", called)
    assert client.get(URL).status_code == 401 and not called.called


def test_route_answers_the_2_11_contract_for_a_done_job(client, monkeypatch):
    import app.api.v1.planning as planning_api

    async def done(**kw):
        jobs = FakeJobs()
        out = await call(jobs)
        finish(jobs, out)
        return await call(jobs)
    monkeypatch.setattr(planning_api, "parcel_water_loss", done)
    r = client.get(URL)
    assert r.status_code == 200
    body = r.json()
    assert body["contract_version"] == "2.12" and body["provider_id"] == "aquacrop_water_loss"
    assert body["result"]["stages"][0]["gain_t_ha_model"] == BAND
    assert body["result"]["stages"][1]["gain_t_ha_model"] == 0.2


def test_route_unavailable_and_404(client, monkeypatch):
    import app.api.v1.planning as planning_api
    monkeypatch.setattr(planning_api, "parcel_water_loss", AsyncMock(return_value={
        "state": "unavailable", "done": 0, "total": None, "job_id": None, "season": None, "result": None,
        "unavailable": [{"provider_id": "aquacrop_water_loss", "reason": "irrigated_regime"}]}))
    r = client.get(URL)
    assert r.status_code == 200 and r.json()["unavailable"][0] == {
        "provider_id": "aquacrop_water_loss", "reason": "irrigated_regime", "code": None}
    monkeypatch.setattr(planning_api, "parcel_water_loss", AsyncMock(side_effect=ParcelNotFound("x")))
    assert client.get(URL).status_code == 404


def test_contract_version_2_11(client, monkeypatch):
    import app.api.v1.planning as planning_api
    assert planning_models.CONTRACT_VERSION == "2.12"
    ts = Path("../src/planning/types.ts").read_text(encoding="utf-8")
    assert "CONTRACT_VERSION = '2.12'" in ts
    monkeypatch.setattr(planning_api, "parcel_in_season", AsyncMock(return_value={
        "state": "unavailable", "done": 0, "total": None, "estimates": [], "job_id": None, "season": None,
        "unavailable": [{"provider_id": "aquacrop", "reason": "not_sown"}]}))
    r = client.get(f"/api/graph/planning/parcels/{PARCEL_ID}/estimates/in-season")
    assert r.json()["contract_version"] == "2.12"
