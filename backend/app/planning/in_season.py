"""In-season forecast inputs (F5): the crop of the parcel's current cycle, a sowing that is a
fact (``actual`` or ``declared``, never a plan), its regime and country; and the gate.

Pure: the caller loads the parcel, its crops and the core's crop-cycle timeline. The core
already folds completed Field-Operations sowings into the cycle start, so they are not read
again here; when the core does not answer the reason is ``cycles_unavailable``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import httpx
from nkz_platform_sdk.orion import OrionClient

from app.graph.dao import _geometry_centroid
from app.planning.aquacrop_provider import PROVIDER_ID, VALIDATED, map_in_season
from app.planning.farm import parcel_regime
from app.planning.service import ParcelNotFound
from app.services.country_lookup import country_at
from app.services.crop_cycles_client import fetch_crop_cycles
from app.services.sim_errors import SimulationError
from app.species_registry import catalog_code, get_species_info, resolve_species

logger = logging.getLogger(__name__)
MAX_SEASON_DAYS = 366
FACT = ("actual", "declared")


@dataclass(frozen=True)
class InSeasonInputs:
    crop_slug: str
    eppo: str
    regime: str
    regime_source: str
    country: str | None
    sowing_date: date
    sowing_provenance: str


def _country(parcel: dict) -> str | None:
    location = parcel.get("location")
    geometry = location.get("value", location) if isinstance(location, dict) else None
    point = _geometry_centroid(geometry.get("coordinates")) if isinstance(geometry, dict) else None
    if point is None:
        return None
    lon, lat = point
    return country_at(lat, lon)


def _iso(value) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def _ended_recently(previous, today: date) -> bool:
    """The previous cycle was sown and its end (harvest) is recorded within the season window."""
    if not isinstance(previous, dict):
        return False
    start, end = previous.get("start"), previous.get("end")
    if not (isinstance(start, dict) and isinstance(end, dict)):
        return False
    ended = _iso(end.get("date"))
    return (start.get("provenance") in FACT and end.get("provenance") == "actual" and ended is not None
            and today - timedelta(days=MAX_SEASON_DAYS) <= ended <= today)


def resolve_in_season(parcel: dict, crops: dict[str, dict], cycles: dict | None,
                      today: date) -> InSeasonInputs | str:
    """The inputs of an in-season forecast, or the reason code why there is none.

    No current cycle: a crop due but not sown (``pending``) is ``not_sown``; else a season whose
    harvest was just recorded is ``season_ended``; else ``no_committed_crop``. A malformed core
    answer is treated as no answer (``cycles_unavailable``), never guessed through.
    """
    if not isinstance(cycles, dict):
        return "cycles_unavailable"
    current = cycles.get("current")
    if current is not None and not isinstance(current, dict):
        return "cycles_unavailable"
    if not current:
        if cycles.get("pending"):
            return "not_sown"
        return "season_ended" if _ended_recently(cycles.get("previous"), today) else "no_committed_crop"
    start = current.get("start") or {}
    if not isinstance(start, dict):
        return "cycles_unavailable"
    if start.get("provenance") not in FACT:
        return "not_sown"
    sowing = _iso(start.get("date"))
    if sowing is None:
        return "no_sowing_date"
    crop_id = current.get("crop_id")
    crop = (crops.get(crop_id) if isinstance(crop_id, str) else None) or {}
    slug = resolve_species(str(current.get("species") or crop.get("species") or ""))
    eppo = (get_species_info(slug) or {}).get("eppo_code") if slug else None
    if not eppo:
        return "unknown_crop"
    eppo = catalog_code(eppo)
    regime, regime_source = parcel_regime(parcel, [crop] if crop else [])
    if regime is None:
        return "no_regime"
    country = _country(parcel)
    if (eppo, regime, country) not in VALIDATED:
        return "not_validated"
    if sowing > today:
        return "sowing_in_future"
    if sowing < today - timedelta(days=MAX_SEASON_DAYS):
        return "season_too_old"
    return InSeasonInputs(crop_slug=slug, eppo=eppo, regime=regime, regime_source=regime_source,
                          country=country, sowing_date=sowing, sowing_provenance=start["provenance"])


IRRIGATION = {"rainfed": "rainfed", "irrigated": "full"}


async def load_parcel(tenant_id: str, parcel_id: str) -> tuple[dict, dict[str, dict], dict | None]:
    """The parcel, its crops by id and the core's crop-cycle timeline (None: core unreachable)."""
    client = OrionClient(tenant_id=tenant_id)
    try:
        parcel = await client.get_entity(parcel_id, options="keyValues")
        crops = await client.query_entities(type="AgriCrop", q=f'hasAgriParcel=="{parcel_id}"',
                                            limit=1000, options="keyValues")
    finally:
        await client.close()
    return parcel, {c["id"]: c for c in crops}, await fetch_crop_cycles(parcel_id, tenant_id)


def season_info(result: dict, inputs: InSeasonInputs, finished_at: datetime | None,
                provider_version: str) -> dict:
    used = result.get("inputs") or {}
    fm, weather = used.get("field_management") or {}, used.get("weather") or {}
    complete = result.get("status") == "complete"
    return {"status": "complete" if complete else "in_season", "as_of": weather.get("end"),
            "crop": inputs.eppo, "regime": inputs.regime,
            "sowing": {"date": inputs.sowing_date.isoformat(), "source": "crop_cycle",
                       "provenance": inputs.sowing_provenance},
            "sowing_method": fm.get("sowing_method"), "sowing_method_source": fm.get("source"),
            "harvest_date": result.get("harvest_date"),
            "n_years": 0 if complete else ((result.get("ensemble") or {}).get("n_years") or 0),
            "weather_source": weather.get("source"),
            "computed_at": finished_at.isoformat() if finished_at else None,
            "provider_version": provider_version}


def _unavailable(reason: str, code: str | None = None) -> dict:
    item = {"provider_id": PROVIDER_ID, "reason": reason}
    if code is not None:
        item["code"] = code
    return {"state": "unavailable", "done": 0, "total": None, "estimates": [], "unavailable": [item],
            "job_id": None, "season": None}


def _failed(reason: str, code: str) -> dict:
    return {**_unavailable(reason, code), "state": "failed"}


def _view(job, inputs: InSeasonInputs, version: str) -> dict:
    out = {"state": job.state, "done": job.done, "total": job.total, "estimates": [], "unavailable": [],
           "job_id": job.id, "season": None}
    if job.state == "failed":
        out["unavailable"] = [{"provider_id": PROVIDER_ID, "reason": "engine_error",
                               "code": (job.error or {}).get("code")}]
    elif job.state == "done":
        out["estimates"], out["unavailable"] = map_in_season(job.result or {}, version)
        out["season"] = season_info(job.result or {}, inputs, job.finished_at, version)
    return out


async def parcel_in_season(*, dao, jobs, tenant_id: str, parcel_id: str, today: date | None = None,
                           retry: bool = False, load=load_parcel) -> dict:
    """State of the in-season forecast, starting or joining its day-keyed simulation job.

    Idempotent (the caller polls). Inputs are resolved from facts on every call (no light poll):
    a stale job id never answers for other inputs. Gaps answer ``unavailable`` without a job.
    """
    today = today or datetime.now(UTC).date()
    try:
        parcel, crops, cycles = await load(tenant_id, parcel_id)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise ParcelNotFound(parcel_id) from e
        code = f"http_{e.response.status_code}"
        logger.warning("in_season_parcel_unavailable tenant=%s parcel=%s code=%s", tenant_id, parcel_id, code)
        return _failed("parcel_unavailable", code)
    except httpx.HTTPError as e:
        code = type(e).__name__
        logger.warning("in_season_parcel_unavailable tenant=%s parcel=%s code=%s", tenant_id, parcel_id, code)
        return _failed("parcel_unavailable", code)
    inputs = resolve_in_season(parcel, crops, cycles, today)
    if isinstance(inputs, str):
        return _unavailable(inputs)
    # The parameters of POST /agriculture/crop-simulation/jobs: identical inputs share the job.
    params = {"parcel_id": parcel_id, "crop_slug": inputs.crop_slug,
              "sowing_date": inputs.sowing_date.isoformat(), "irrigation": IRRIGATION[inputs.regime],
              "engine": "aquacrop"}

    async def run(progress):
        return await dao.run_crop_simulation(
            parcel_id=parcel_id, tenant_id=tenant_id, crop_slug=params["crop_slug"],
            sowing_date_str=params["sowing_date"], irrigation=params["irrigation"], engine="aquacrop",
            progress=progress)

    known = jobs.get(jobs.job_id(tenant_id, params, today, jobs.version), tenant_id)
    if known is not None and known.state == "failed" and not retry:
        return _view(known, inputs, jobs.version)
    try:
        job = await jobs.start(tenant_id, params, run, today)
    except SimulationError as e:
        if e.code != "too_many_jobs":
            raise
        return _unavailable("busy")
    return _view(job, inputs, jobs.version)
