"""Water loss by growth stage of a sown rainfed parcel (contract 2.11): the job and its answer.

By default it answers only for the last finished campaign (the ``previous`` cycle); the current,
in-season cycle is served only while ``WATER_LOSS_IN_SEASON`` is true. Crop, sowing and regime resolve
as for the in-season forecast (``resolve_sown_crop``), but with a queue of its own (never the shared one, so it cannot make the in-season route answer
``busy``), rainfed only, no validation gate: the answer says whether the crop, regime and region
are inside the validated forecast, and always that no observed data arbitrates the figures.
"""
from __future__ import annotations

import logging
import os
from datetime import UTC, date, datetime

import httpx

from app.planning.aquacrop_provider import VALIDATED
from app.planning.in_season import (
    IRRIGATION,
    SownCrop,
    load_parcel,
    parcel_point,
    resolve_finished_crop,
    resolve_sown_crop,
    season_window_reason,
)
from app.planning.service import ParcelNotFound
from app.services.country_lookup import country_at
from app.services.sim_crops import resolve_aquacrop_crop
from app.services.sim_errors import SimulationError
from app.services.sim_jobs import SimJobs, crop_simulation_version
from app.services.water_loss_by_stage import BASIS_STAGE_BOUNDS

logger = logging.getLogger(__name__)

PROVIDER_ID = "aquacrop_water_loss"
JOB_KIND = "water_loss_by_stage"
VERSION_SUFFIX = "|wl1"
MILESTONES_BASIS = "thermal_time_no_stress_delay"
LABEL = "model estimate; no observed data arbitrates it"
NOTES = [
    "stage gains do not add up: keeping the soil wet in one stage leaves it full for the next",
    ("in season: observed weather up to as_of, rest of season from N reference years; a finished "
     "stage's figure still depends on the weather after it and changes until harvest"),
    "stages follow canopy development; flowering and grain fill fall across max_canopy and senescence",
]
SOURCES = [
    {"short": "AquaCrop-OSPy",
     "detail": ("Kelly T.D., Foster T. (2021). AquaCrop-OSPy: Bridging the gap between research and practice "
                "in crop-water modelling. Agricultural Water Management 254:106976. "
                "doi:10.1016/j.agwat.2021.106976. Per-stage irrigation by soil-moisture threshold: "
                "IrrigationManagement(irrigation_method=1, SMT).")},
    {"short": "AquaCrop-OS",
     "detail": ("Foster T. et al. (2017). AquaCrop-OS: An open source version of FAO's crop water model. "
                "Agricultural Water Management 181:18-22. doi:10.1016/j.agwat.2016.11.015.")},
]
VALIDATED_NOTE = ("crop, regime and region of the validated forecast; the stage figures themselves "
                  "are not validated")
UNVALIDATED_NOTE = "crop, regime or region outside the validated forecast; figures are model output only"

_JOBS: SimJobs | None = None
_WARNED = False


def in_season_enabled() -> bool:
    """``WATER_LOSS_IN_SEASON`` ("true"/"false", default off), read at call time.

    Off: the route answers only for the last finished campaign. Any other value counts as off
    and is logged once.
    """
    global _WARNED
    raw = os.environ.get("WATER_LOSS_IN_SEASON", "").strip().lower()
    if raw == "true":
        return True
    if raw not in ("", "false") and not _WARNED:
        _WARNED = True
        logger.warning("WATER_LOSS_IN_SEASON=%r is not 'true' or 'false': treated as false", raw)
    return False


def water_loss_jobs() -> SimJobs:
    """The queue of this route (single replica): one job at a time, one pending job per tenant."""
    global _JOBS
    if _JOBS is None:
        _JOBS = SimJobs(version=crop_simulation_version() + VERSION_SUFFIX,
                        version_for=lambda crop: crop_simulation_version(crop) + VERSION_SUFFIX,
                        max_concurrent=1, max_pending_per_tenant=1)
    return _JOBS


def _unavailable(reason: str, code: str | None = None) -> dict:
    item = {"provider_id": PROVIDER_ID, "reason": reason}
    if code is not None:
        item["code"] = code
    return {"state": "unavailable", "done": 0, "total": None, "unavailable": [item], "job_id": None,
            "season": None, "result": None}


def _failed(reason: str, code: str) -> dict:
    return {**_unavailable(reason, code), "state": "failed"}


def _country(parcel: dict) -> str | None:
    point = parcel_point(parcel)
    return country_at(*point) if point else None


def _iso(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _place_milestones(stage: dict, as_of: date | None, milestones: dict) -> tuple[list | None, str | None]:
    """The reached milestones inside the stage (to ``as_of`` while it is in progress), by date."""
    if milestones.get("status") != "ok":
        return None, milestones.get("reason")
    if stage["state"] == "not_reached" or stage["start"] is None:
        return [], None
    start = _iso(stage["start"])
    end = _iso(stage["end"]) if stage["state"] == "complete" else as_of
    found = []
    for key, item in (milestones.get("items") or {}).items():
        when = _iso(item.get("date")) if item.get("status") == "reached" else None
        if when is not None and start <= when and (end is None or when <= end):
            found.append({"key": key, "date": when.isoformat(), "basis": MILESTONES_BASIS})
    return sorted(found, key=lambda m: m["date"]), None


def _result(job_result: dict, sown: SownCrop, country: str | None) -> dict:
    wl = job_result["water_loss"]
    milestones = job_result.get("milestones") or {"status": "unavailable", "reason": "calendar_failed"}
    as_of = _iso(wl["as_of"])
    stages = []
    for stage in wl["stages"]:
        placed, reason = _place_milestones(stage, as_of, milestones)
        stages.append({**stage, "milestones": placed, "milestones_reason": reason})
    validated = (sown.eppo, sown.regime, country) in VALIDATED
    return {
        "total": wl["total"], "stages": stages,
        "stage_bounds_basis": BASIS_STAGE_BOUNDS, "milestones_basis": MILESTONES_BASIS,
        "yield_basis": job_result["yield_basis"],
        "validation": {"status": "validated_forecast_crop" if validated else "unvalidated",
                       "note": VALIDATED_NOTE if validated else UNVALIDATED_NOTE},
        "engine_version": wl["engine_version"],
        "label": LABEL, "notes": list(NOTES), "sources": [dict(s) for s in SOURCES],
        "warnings": list(job_result.get("warnings") or []),
    }


def _season(job_result: dict, sown: SownCrop, finished_at: datetime | None, version: str) -> dict:
    wl = job_result["water_loss"]
    return {"status": wl["season"], "as_of": wl["as_of"], "crop": sown.eppo, "regime": sown.regime,
            "sowing": {"date": sown.sowing_date.isoformat(), "source": "crop_cycle",
                       "provenance": sown.sowing_provenance},
            "n_years": (wl.get("ensemble") or {}).get("n_years") or 0,
            "ensemble": wl.get("ensemble"),
            "cycle": "previous" if sown.harvest_date else "current",
            "harvest_recorded": sown.harvest_date.isoformat() if sown.harvest_date else None,
            "computed_at": finished_at.isoformat() if finished_at else None, "provider_version": version}


def _view(job, sown: SownCrop, country: str | None, version: str) -> dict:
    out = {"state": job.state, "done": job.done, "total": job.total, "unavailable": [], "job_id": job.id,
           "season": None, "result": None}
    if job.state == "failed" and (job.error or {}).get("code") == "season_not_finished":
        return _unavailable("season_not_finished")
    if job.state == "failed":
        out["unavailable"] = [{"provider_id": PROVIDER_ID, "reason": "engine_error",
                               "code": (job.error or {}).get("code")}]
    elif job.state == "done":
        out["result"] = _result(job.result or {}, sown, country)
        out["season"] = _season(job.result or {}, sown, job.finished_at, version)
    return out


async def parcel_water_loss(*, dao, jobs, tenant_id: str, parcel_id: str, today: date | None = None,
                            retry: bool = False, load=load_parcel) -> dict:
    """State of the water-loss job of the parcel's current cycle, starting or joining it.

    Idempotent (the caller polls); inputs are resolved from facts on every call. Gaps answer
    ``unavailable`` without a job. Only rainfed: irrigation is not simulated here.
    """
    today = today or datetime.now(UTC).date()
    try:
        parcel, crops, cycles = await load(tenant_id, parcel_id)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise ParcelNotFound(parcel_id) from e
        code = f"http_{e.response.status_code}"
        logger.warning("water_loss_parcel_unavailable tenant=%s parcel=%s code=%s", tenant_id, parcel_id, code)
        return _failed("parcel_unavailable", code)
    except httpx.HTTPError as e:
        code = type(e).__name__
        logger.warning("water_loss_parcel_unavailable tenant=%s parcel=%s code=%s", tenant_id, parcel_id, code)
        return _failed("parcel_unavailable", code)
    sown = resolve_sown_crop(parcel, crops, cycles, today) if in_season_enabled() else None
    if isinstance(sown, SownCrop):
        reason = season_window_reason(sown.sowing_date, today)
        if reason is not None:
            return _unavailable(reason)
    else:
        # The last finished campaign; its harvest window replaces the in-season sowing window.
        current_reason = sown
        sown = resolve_finished_crop(parcel, crops, cycles, today)
        if isinstance(sown, str):
            return _unavailable(current_reason if current_reason and sown == "no_finished_season" else sown)
    if sown.regime is None:
        return _unavailable("no_regime")
    if sown.regime != "rainfed":
        return _unavailable("irrigated_regime")
    try:
        resolve_aquacrop_crop(sown.crop_slug)
    except SimulationError:
        return _unavailable("unsupported_crop")
    params = {"parcel_id": parcel_id, "crop_slug": sown.crop_slug,
              "sowing_date": sown.sowing_date.isoformat(), "irrigation": IRRIGATION[sown.regime],
              "engine": "aquacrop", "kind": JOB_KIND,
              "cycle": "previous" if sown.harvest_date else "current"}

    async def run(progress):
        return await dao.run_crop_simulation(
            parcel_id=parcel_id, tenant_id=tenant_id, crop_slug=params["crop_slug"],
            sowing_date_str=params["sowing_date"], irrigation=params["irrigation"], engine="aquacrop",
            progress=progress, mode=JOB_KIND,
            allow_ensemble=params["cycle"] == "current")

    version = jobs.version_for(params["crop_slug"])
    country = _country(parcel)
    known = jobs.get(jobs.job_id(tenant_id, params, today, version), tenant_id)
    if known is not None and known.state == "failed" and not retry:
        return _view(known, sown, country, version)
    try:
        job = await jobs.start(tenant_id, params, run, today)
    except SimulationError as e:
        if e.code != "too_many_jobs":
            raise
        return _unavailable("busy")
    return _view(job, sown, country, version)
