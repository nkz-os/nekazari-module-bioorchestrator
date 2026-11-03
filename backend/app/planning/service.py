"""Planning use cases: read the parcel, apply the pure rules, return contract dicts."""
from __future__ import annotations

import asyncio
import calendar
import functools
import logging
import os
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import yaml

from app.common.attribution import attach_attributions, rows_source_ids
from app.core.knowledge import KnowledgeSource
from app.graph.balance_dao import BalanceDao
from app.graph.dao import _to_parcel_urn
from app.graph.recommend import sowing_info
from app.planning.aquacrop_provider import PROVIDER_ID as AQUACROP_ID
from app.planning.aquacrop_provider import aquacrop_gate, map_result, poll_in_scope
from app.planning.calendar import (
    PreviousCrop,
    build_calendar,
    campaign_fortnights,
    fortnight_of,
)
from app.planning.climatology import (
    cell_key,
    climatology_status,
    get_climatology,
    schedule_climatology,
)
from app.planning.estimates import (
    aquacrop_status,
    disagreement,
    estimate_providers,
    trial_estimate,
    usable_rows,
    visible,
)
from app.planning.evidence import load_crop_evidence
from app.planning.evidence_depth import evidence_depth
from app.planning.fit import (
    CLIMATE_UNKNOWN,
    MULTI_YEAR,
    REGIME_MISSING,
    ROTATION_UNKNOWN,
    VERDICT_ORDER,
    ParcelClimate,
    allotment_reasons,
    frost_reasons,
    soil_reasons,
    verdict_of,
    water_reasons,
)
from app.planning.history import (
    campaign_of_start,
    cycles_of,
    declared_only,
    first_campaign,
    history_complete,
    iso_date,
    missing_history_campaigns,
    past_crops,
    species_slug,
)
from app.planning.irrigation import (
    CropStages,
    irrigation_need,
    kc_curve,
    load_pe_method,
    load_stage_table,
    read_allotment,
)
from app.planning.reference import EMPTY_REFERENCE, load_reference
from app.planning.regional_yield import regional_yield
from app.planning.rotation import rotation_reasons
from app.planning.varieties import variety_stats
from app.services import ggcmi_calendar
from app.services.crop_cycles_client import fetch_crop_cycles
from app.services.crop_reference import CROP_REFERENCE
from app.services.nuts_reference import names_or_none, plausible_range, province_table
from app.services.region_lookup import es_province_at
from app.services.sim_errors import SimulationError
from app.services.soil_client import assess_soil_suitability
from app.services.sowing_windows import load_rows
from app.species_registry import (
    catalog_code,
    get_lifecycle,
    get_species_info,
    is_cover_only,
    is_woody,
    list_species,
    resolve_species,
)

logger = logging.getLogger(__name__)

REGIMES = ("rainfed", "irrigated")
NO_SOWING = {"sowing_type": None, "sowing_window": None, "source": None, "typical_sowing_doy": None}


class ParcelNotFound(LookupError):
    pass


class UnknownCrop(ValueError):
    pass


class InvalidParcelId(ValueError):
    pass


# A parcel id is interpolated into an Orion path and an NGSI-LD ``q`` filter: anything outside this
# alphabet (``/ ? # " ;`` ...) could rewrite the request, so it is rejected before any I/O.
PARCEL_URN_RE = re.compile(r"^urn:ngsi-ld:AgriParcel:[A-Za-z0-9._~:-]+$")


def parcel_urn(raw: str) -> str:
    """Normalize a short id to the AgriParcel URN and validate it; raise InvalidParcelId otherwise."""
    urn = _to_parcel_urn(raw)
    # fullmatch: ``$`` alone would accept a trailing newline.
    if not isinstance(urn, str) or not PARCEL_URN_RE.fullmatch(urn):
        raise InvalidParcelId("invalid parcel id")
    return urn


def resolve_regime(requested: str | None, env: dict) -> tuple[str | None, str]:
    """The declared regime wins; else the parcel's own attribute; never a guess."""
    if requested in REGIMES:
        return requested, "declared"
    irrigation = env.get("irrigation") or {}
    inferred = irrigation.get("inferred")
    if inferred in REGIMES:
        return inferred, f"parcel:{irrigation.get('source') or 'unknown'}"
    return None, "unknown"


def has_trials(eppo: str | None, trial_eppo: frozenset[str]) -> bool:
    return bool(eppo) and (eppo in trial_eppo or catalog_code(eppo) in trial_eppo)


def _sowing(sowing: dict) -> dict:
    window = sowing.get("sowing_window") or {}
    return {"sowing_type": sowing.get("sowing_type"), "start_month": window.get("start_month"),
            "end_month": window.get("end_month"), "typical_sowing_doy": sowing.get("typical_sowing_doy"),
            "source": sowing.get("source")}


async def load_environment(dao, parcel_id: str, tenant_id: str, *, fresh_soil: bool = False) -> dict:
    env = await dao.get_parcel_environment(parcel_id, tenant_id, fresh_soil=fresh_soil)
    if "error" in env:
        raise ParcelNotFound(env["error"])
    return env


REGION_LOOKUP_UNAVAILABLE = "region_lookup_unavailable"
CROP_NOT_IN_STATISTICS = "crop_not_in_statistics"
REGION_STATS_UNAVAILABLE = "region_stats_unavailable"
EUROSTAT_SOURCE_ID = "eurostat_apro_cpshr"


@functools.lru_cache(maxsize=1)
def _statistics_crops() -> frozenset[str]:
    """Catalog codes of the crops the Eurostat series cover (the codes the graph was seeded with)."""
    path = Path(os.getenv("DATA_DIR", "./data")) / "eurostat" / "crop_codes.yaml"
    return frozenset(catalog_code(c) for c in yaml.safe_load(path.read_text())["crops"].values())


def parcel_nuts2(lat: float | None, lon: float | None) -> str | None:
    """NUTS 2 code of the parcel's centroid (Spain only, through its province), else None."""
    province = es_province_at(lat, lon)
    return province_table().get(province["code"]) if province else None


REGIONAL_READ_TIMEOUT_S = 5.0


async def regional_yields_for(source: KnowledgeSource, nuts: str | None, *, tenant_id: str,
                              parcel_id: str) -> tuple[list[dict], bool]:
    """Rows of the region and its country for the statistics crops, in ONE graph query; (rows, failed).

    Any failure (crop list unreadable, graph error, timeout) is `failed`: the fit is still returned.
    """
    if nuts is None:
        return [], False
    try:
        codes = sorted(_statistics_crops())
        rows = await asyncio.wait_for(
            BalanceDao(source.driver).get_regional_yields_batch(codes, [nuts, nuts[:2]]),
            timeout=REGIONAL_READ_TIMEOUT_S)
        return rows, False
    except TimeoutError:
        logger.warning("planning fit: regional yields read timed out after %ss graph=%s tenant=%s parcel=%s nuts=%s",
                       REGIONAL_READ_TIMEOUT_S, source.graph_id, tenant_id, parcel_id, nuts)
    except Exception:
        logger.exception("planning fit: regional yields read failed graph=%s tenant=%s parcel=%s nuts=%s",
                         source.graph_id, tenant_id, parcel_id, nuts)
    return [], True


def crop_regional_yield(code: str | None, nuts: str | None, rows: list[dict], failed: bool):
    """(regional yield dict | None, reason | None) of one crop. Never a default figure."""
    if nuts is None:
        return None, REGION_LOOKUP_UNAVAILABLE
    if failed:
        return None, REGION_STATS_UNAVAILABLE
    if code is None or catalog_code(code) not in _statistics_crops():
        return None, CROP_NOT_IN_STATISTICS
    code = catalog_code(code)
    region = [r for r in rows if r.get("eppo") == code and r.get("nuts") == nuts]
    country = [r for r in rows if r.get("eppo") == code and r.get("nuts") == nuts[:2]]
    name = (names_or_none() or {}).get(nuts)  # Eurostat labels are English
    found, reason = regional_yield(region, country, plausible=plausible_range(code),
                                   region_name={"en": name} if name else None)
    if found is None:
        return None, reason
    found = found.model_copy(update={
        "attributions": attach_attributions({}, [EUROSTAT_SOURCE_ID])["attributions"]})
    return found.model_dump(), reason


def _season_start_doy(sowing: dict, eppo: str | None, lat, lon, regime: str | None) -> int | None:
    """Typical sowing day of the crop at the point (GGCMI): from the sowing info, else straight from the calendar."""
    doy = sowing.get("typical_sowing_doy")
    if doy is None and eppo and lat is not None and lon is not None:
        cal = ggcmi_calendar.lookup(catalog_code(eppo), lat, lon, irrigation=regime)
        doy = cal["planting_doy"] if cal else None
    return doy


IRRIGATION_SOURCE_IDS = ("open_meteo_historical", "copernicus_era5", "fao56")


class IrrigationContext:
    """What the irrigation need of every species of ONE fit shares: regime, weather cell, stages, method."""

    def __init__(self, regime: str | None, lat, lon, stages: dict[str, CropStages] | None):
        self.regime = regime
        self.lat, self.lon = lat, lon
        self.stages = stages  # FAO-56 table by species slug; None: the table could not be loaded
        self.attributions = attach_attributions({}, IRRIGATION_SOURCE_IDS)["attributions"]
        self.clim = None
        self.status = "absent"
        self.cell = cell_key(lat, lon) if regime == "irrigated" and lat is not None and lon is not None else None
        if self.cell is not None:
            schedule_climatology(self.cell)  # the first request never waits for the archive
            self.clim = get_climatology(self.cell)
            self.status = climatology_status(self.cell)


def species_irrigation_need(ctx: IrrigationContext, slug: str, eppo: str | None, sowing: dict) -> tuple[dict | None, str | None]:
    """(irrigation_need dict | None, reason | None). Never a default figure."""
    if ctx.regime is None:
        return None, None  # regime_missing already says so
    if ctx.regime != "irrigated":
        return None, "regime_not_irrigated"
    if ctx.stages is None:
        return None, "kc_unavailable"
    crop = ctx.stages.get(slug)
    if crop is None:
        return None, "no_stage_lengths"  # only the crops of the cited FAO-56 table have a Kc curve
    kc = kc_curve(crop)
    if len(kc) > 365:
        return None, "no_stage_lengths"  # a season longer than a year is not an annual cycle
    if ctx.cell is None:
        return None, "weather_unavailable"  # the parcel has no location
    doy = _season_start_doy(sowing, eppo, ctx.lat, ctx.lon, ctx.regime)
    if doy is None:
        return None, "no_sowing_date"
    if ctx.clim is None:
        return None, "weather_unavailable" if ctx.status == "failed" else "pending"
    pe = load_pe_method()
    result = irrigation_need(kc, int(doy), ctx.clim, pe)
    if result is None:
        return None, "weather_unavailable"
    return {
        "percentiles": {"p10": round(result.p10, 1), "p50": round(result.p50, 1), "p90": round(result.p90, 1)},
        "unit": "mm", "n_years": result.n_years, "first_year": result.first_year, "last_year": result.last_year,
        "sowing_doy": result.sowing_doy, "method": {"kc_source": crop.kc_source, "pe_method": pe.id},
        "source": "openmeteo_era5", "attributions": list(ctx.attributions),
    }, None


def _rank_key(s: dict) -> tuple:
    ry = s.get("regional_yield")
    index = ry.get("relative_index") if ry else None
    return (VERDICT_ORDER[s["verdict"]], index is None, -(index or 0.0), s["species"])


async def parcel_fit(*, dao, source: KnowledgeSource, parcel_id: str, tenant_id: str,
                     campaign: int, regime: str | None, refresh_soil: bool = False) -> dict:
    # Only the read right after the farmer declared a soil texture skips the 24 h soil cache.
    env = await load_environment(dao, parcel_id, tenant_id, fresh_soil=refresh_soil)
    regime_used, regime_source = resolve_regime(regime, env)
    cycles = await fetch_crop_cycles(env["parcel_id"], tenant_id)
    if cycles is not None and not isinstance(cycles, dict):
        # A malformed answer is "history unavailable", the same as no answer: it must never read as "no history".
        logger.warning("planning fit: crop-cycles answer is %s, not an object tenant=%s parcel=%s",
                       type(cycles).__name__, tenant_id, env["parcel_id"])
        cycles = None
    history = past_crops(cycles, campaign) if cycles is not None else None
    try:
        ref = await load_reference(source)
    except Exception:
        # A failing step must not take the page down: without the reference every gate that needs it is "unknown".
        logger.exception("planning fit: reference load failed graph=%s tenant=%s parcel=%s",
                         source.graph_id, tenant_id, env["parcel_id"])
        ref = EMPTY_REFERENCE
    detail = env.get("climate_detail") or {}
    climate_known = bool(env.get("climate_class")) and bool(detail)
    climate = ParcelClimate(detail.get("coldest_month_min_c"), detail.get("annual_rainfall_mm"),
                            detail.get("annual_et0_mm"))
    soil = env.get("soil") or {}
    centroid = env.get("centroid") or {}
    rows = load_rows()
    nuts = parcel_nuts2(centroid.get("lat"), centroid.get("lon"))
    yield_rows, yield_failed = await regional_yields_for(
        source, nuts, tenant_id=tenant_id, parcel_id=env["parcel_id"])
    allotment = read_allotment(env.get("irrigation_allotment_raw"), parcel_id=env["parcel_id"], tenant_id=tenant_id)
    stages = None
    if regime_used == "irrigated":
        try:
            stages = load_stage_table()
        except Exception:
            # A broken packaged table must not take the page down: every crop reports kc_unavailable.
            logger.exception("planning fit: FAO-56 stage table load failed tenant=%s parcel=%s",
                             tenant_id, env["parcel_id"])
    irrigation = IrrigationContext(regime_used, centroid.get("lat"), centroid.get("lon"), stages)
    species = []
    for slug in list_species():
        if is_woody(slug):
            # Herbaceous perennials (alfalfa) are campaign choices; trees and vines are not:
            # they are a plantation decision, not a rotation one.
            continue
        info = get_species_info(slug) or {}
        lifecycle = get_lifecycle(slug)
        eppo = info.get("eppo_code") or None
        tol = ref.tolerances.get(slug)
        sowing = (sowing_info(eppo, env.get("climate_class"), rows, env.get("country"),
                              lat=centroid.get("lat"), lon=centroid.get("lon"), irrigation=regime_used)
                  if eppo else NO_SOWING)
        gate = assess_soil_suitability(tol.soil if tol else None, soil)
        reasons = soil_reasons(gate, parcel_soil_available=bool(soil.get("data_available")),
                               tolerance_available=bool(tol and tol.soil))
        # A herbaceous perennial stands through winter whatever its sowing calendar says.
        reasons += frost_reasons("perennial" if lifecycle == "perennial" else sowing.get("sowing_type"),
                                 climate.coldest_month_min_c, tol.frost_damage_c if tol else None)
        season_days = (CROP_REFERENCE.get(catalog_code(eppo)) or {}).get("growing_season_days") if eppo else None
        reasons += water_reasons(regime_used, climate, season_days)
        # No history, a partial one (a cycle that cannot be placed) or no rotation rules in the graph
        # at all is "unknown", never "no restriction".
        if history is None or not ref.constraints or not history_complete(cycles):
            reasons.append(ROTATION_UNKNOWN)
        else:
            reasons += rotation_reasons(history, slug, campaign, ref.constraints)
        if regime_used is None:
            reasons.append(REGIME_MISSING)
        if not climate_known:
            # Without the parcel's climate the frost, water and sowing gates silently pass: say so.
            reasons.append(CLIMATE_UNKNOWN)
        if lifecycle == "perennial":
            reasons.append(MULTI_YEAR)
        if irrigation.regime == "irrigated":
            # numpy over ~15 seasons per species: off the event loop (the answers are memoized per cell).
            need, need_reason = await asyncio.to_thread(species_irrigation_need, irrigation, slug, eppo, sowing)
        else:
            need, need_reason = species_irrigation_need(irrigation, slug, eppo, sowing)
        if need is not None:
            reasons += allotment_reasons(need["percentiles"]["p50"], need["percentiles"]["p90"], allotment)
        trials = has_trials(eppo, ref.trial_eppo)
        regional, regional_reason = crop_regional_yield(eppo, nuts, yield_rows, yield_failed)
        species.append({
            "species": slug, "eppo": eppo, "names": dict(info.get("common_names") or {}), "lifecycle": lifecycle,
            "verdict": verdict_of(reasons), "reasons": reasons, "sowing": _sowing(sowing),
            "has_variety_trials": trials, "estimate_providers": estimate_providers(
                catalog_code(eppo) if eppo else None, has_trials=trials, regime=regime_used, country=env.get("country")),
            "cover_only": is_cover_only(slug),
            "regional_yield": regional, "regional_yield_unavailable": regional_reason,
            "irrigation_need": need, "irrigation_need_unavailable": need_reason,
        })
    species.sort(key=_rank_key)
    return {
        "parcel_id": env["parcel_id"], "campaign": campaign, "graphs": [source.graph_id], "species": species,
        "facts": {"climate_class": env.get("climate_class"), "climate_source": detail.get("source") or "unavailable",
                  "soil_texture": soil.get("texture"), "soil_ph": soil.get("ph"),
                  "soil_source": soil.get("source") or "unavailable", "regime": regime_used,
                  "regime_source": regime_source, "history_available": cycles is not None,
                  "history_missing_campaigns": missing_history_campaigns(cycles, campaign),
                  "irrigation_allotment_mm": allotment},
    }


def crop_code(identifier: str) -> str:
    """Slug, EPPO code or name -> the code trials are listed under. UnknownCrop when unknown."""
    slug = resolve_species(identifier)
    eppo = (get_species_info(slug) or {}).get("eppo_code") if slug else None
    if not eppo:
        raise UnknownCrop(f"unknown crop: {identifier[:32]}")
    return catalog_code(eppo)


async def parcel_varieties(*, dao, source: KnowledgeSource, parcel_id: str, tenant_id: str,
                           crop: str, regime: str, management: str) -> dict:
    eppo = crop_code(crop)
    env = await load_environment(dao, parcel_id, tenant_id)
    base = {"parcel_id": env["parcel_id"], "crop": eppo, "regime": regime, "management": management,
            "graph_id": source.graph_id}
    ev = await load_crop_evidence(dao, env, crop_eppo=eppo, regime=regime, management=management)
    if ev["status"] != "ok":
        return {**base, "status": ev["status"]}
    stats, n_env = variety_stats(ev["rows"])
    data_gaps = list(ev["data_gaps"])
    if not stats:
        # Trials exist, but no trial table has enough varieties to compare them against each other.
        data_gaps.append("insufficient_environments")
    payload = {**base, "status": "ok", "tier": ev["tier"], "zone_match": ev["zone_match"], "country": ev["country"],
               "n_environments": n_env, "varieties": stats, "data_gaps": data_gaps,
               "evidence_depth": evidence_depth(ev["rows"], total=ev.get("total"), summary=ev.get("summary"))}
    return attach_attributions(payload, rows_source_ids(ev["rows"]))


async def parcel_estimates(*, dao, source: KnowledgeSource, parcel_id: str, tenant_id: str, crop: str,
                           variety: str | None, regime: str, management: str, sowing_date: date | None,
                           today: date | None = None) -> dict:
    eppo = crop_code(crop)
    env = await load_environment(dao, parcel_id, tenant_id)
    gate = aquacrop_gate(eppo, regime, env.get("country"), sowing_date, today or datetime.now(UTC).date())
    inputs = {"regime": regime, "management": management, "sowing_date": sowing_date,
              "sowing_source": "planned" if sowing_date else "none",
              "soil_source": (env.get("soil") or {}).get("source") or "unavailable",
              "climate_source": (env.get("climate_detail") or {}).get("source") or "unavailable"}
    base = {"parcel_id": env["parcel_id"], "crop": eppo, "variety": variety, "inputs": inputs,
            "unavailable": [aquacrop_status(gate)]}
    ev = await load_crop_evidence(dao, env, crop_eppo=eppo, regime=regime, management=management)
    if ev["status"] != "ok":
        # needs_climate is already the status; for no_trials the trial provider also states its reason.
        reasons = [_no_trials_reason(variety, [])] if ev["status"] == "no_trials" else []
        return {**base, "status": ev["status"], "unavailable": base["unavailable"] + reasons,
                "estimates": [], "disagreement": False}
    gaps = list(ev["data_gaps"])
    # Credit only the sources of the rows that feed the number (same filter as trial_estimate).
    attributions = attach_attributions({}, rows_source_ids(usable_rows(ev["rows"], variety)))["attributions"]
    # The evidence gaps (e.g. evidence_truncated) travel with the estimate they qualify.
    found = [{**e, "warnings": gaps}
             for e in (trial_estimate(ev["rows"], variety, attributions, source.graph_id, ev["policy_version"]),)
             if e is not None]
    unavailable = base["unavailable"] + ([] if found else [_no_trials_reason(variety, gaps)])
    shown = visible(found)
    return {**base, "status": "ok", "unavailable": unavailable, "estimates": shown,
            "disagreement": disagreement(shown)}


async def parcel_aquacrop(*, dao, jobs, tenant_id: str, parcel_id: str, crop: str, regime: str,
                          sowing_date: date | None, today: date | None = None, retry: bool = False,
                          job_id: str | None = None, sowing_method: str | None = None) -> dict:
    """State of the AquaCrop provider for this input, starting or joining its simulation job.

    Idempotent: the UI polls this. Gated inputs answer ``unavailable`` without touching the
    engine; a failed job is reported as failed (relaunched only with ``retry``, never by polling) and a lost
    one (restart) is started again by the same call.
    """
    today = today or datetime.now(UTC).date()
    eppo = crop_code(crop)
    if job_id and sowing_date is not None and poll_in_scope(eppo, regime, sowing_date, today):
        # Light poll: read the job by id (tenant-scoped), skipping the parcel lookup. Only the id these very
        # inputs hash to is accepted, so a stale or foreign id never answers for another parcel, crop or
        # date. A lost job, or a failed one being retried, takes the full path below.
        expected = jobs.job_id(tenant_id, _job_params(parcel_id, crop, sowing_date, sowing_method), today,
                               jobs.version)
        live = jobs.get(job_id, tenant_id) if job_id == expected else None
        if live is not None and not (live.state == "failed" and retry):
            return _job_view(live, jobs.version)
    env = await load_environment(dao, parcel_id, tenant_id)
    reason = aquacrop_gate(eppo, regime, env.get("country"), sowing_date, today)
    if reason is not None:
        return {"state": "unavailable", "done": 0, "total": None, "estimates": [],
                "unavailable": [{"provider_id": AQUACROP_ID, "reason": reason}], "job_id": None}
    # The very parameters of POST /agriculture/crop-simulation/jobs, so identical inputs share the job.
    params = _job_params(env["parcel_id"], crop, sowing_date, sowing_method)

    async def run(progress):
        return await dao.run_crop_simulation(
            parcel_id=params["parcel_id"], tenant_id=tenant_id, crop_slug=params["crop_slug"],
            sowing_date_str=params["sowing_date"], irrigation="rainfed", engine="aquacrop", progress=progress,
            sowing_method=params.get("sowing_method"))

    # ``start`` relaunches a failed job, which would loop on every poll: look first.
    known = jobs.get(jobs.job_id(tenant_id, params, today, jobs.version), tenant_id)
    if known is not None and known.state == "failed" and not retry:
        return _job_view(known, jobs.version)
    try:
        job = await jobs.start(tenant_id, params, run, today)
    except SimulationError as e:
        if e.code != "too_many_jobs":
            raise
        # The tenant's other simulations are still waiting or running: say so, start nothing.
        return {"state": "unavailable", "done": 0, "total": None, "estimates": [],
                "unavailable": [{"provider_id": AQUACROP_ID, "reason": "busy"}], "job_id": None}
    return _job_view(job, jobs.version)


def _job_params(parcel_id: str, crop: str, sowing_date: date, sowing_method: str | None = None) -> dict:
    params = {"parcel_id": parcel_id, "crop_slug": resolve_species(crop), "sowing_date": sowing_date.isoformat(),
              "irrigation": "rainfed", "engine": "aquacrop"}
    if sowing_method is not None:  # as POST /agriculture/crop-simulation/jobs: absent keeps the old identity
        params["sowing_method"] = sowing_method
    return params


def _job_view(job, version: str) -> dict:
    out = {"state": job.state, "done": job.done, "total": job.total, "estimates": [], "unavailable": [],
           "job_id": job.id}
    if job.state == "failed":
        out["unavailable"] = [{"provider_id": AQUACROP_ID, "reason": "engine_error",
                               "code": (job.error or {}).get("code")}]
    elif job.state == "done":
        out["estimates"], out["unavailable"] = map_result(job.result or {}, version)
        fm = ((job.result or {}).get("inputs") or {}).get("field_management") or {}
        out["sowing_method"], out["sowing_method_source"] = fm.get("sowing_method"), fm.get("source")
    return out


def _no_trials_reason(variety: str | None, data_gaps: list[str]) -> dict:
    """Why the trial provider gave no estimate, as a reason code of the ``unavailable`` list."""
    if variety is None:
        reason = "no_trials"
    elif "evidence_truncated" in data_gaps:
        # The page cap cut the trial list, and the asked variety may sit in the part that was cut.
        reason = "variety_not_in_evidence_sample"
    else:
        reason = "no_trials_for_variety"
    return {"provider_id": "trial_evidence", "reason": reason}


class CalendarInputError(ValueError):
    """A calendar request the rules refuse (code in the message)."""


def typical_maturity(slug: str | None, start: date, *, lat: float | None, lon: float | None,
                     regime: str | None) -> date | None:
    """When a crop sown on ``start`` typically matures at the point (GGCMI calendar); None when unknown.

    The calendars list crops under the catalog code (maize is ZEAMA in the registry, ZEAMX there).
    """
    eppo = (get_species_info(slug) or {}).get("eppo_code") if slug else None
    if not eppo:
        return None
    cal = ggcmi_calendar.lookup(catalog_code(eppo), lat, lon, irrigation=regime)
    if not cal or not cal.get("maturity_doy"):
        return None
    doy = int(cal["maturity_doy"])
    for year in (start.year, start.year + 1):
        day = date(year, 1, 1) + timedelta(days=min(doy, 366 if calendar.isleap(year) else 365) - 1)
        if day > start:
            return day
    return None


def previous_crop(cycles: dict | None, campaign: int, *, maturity_for) -> PreviousCrop | None:
    """The main crop that precedes the campaign and when it ends (spec §4, first found).

    ``maturity_for(slug, start)`` gives the typical end of a crop with no end recorded or planned.
    """
    def campaign_of(c: dict) -> int | None:
        # Bioorch's own rule: the sowing date decides the campaign (Aug-Jul). It is kept on purpose,
        # independent of the SDK's campaign label, so the wizard and this lookup always agree.
        start = iso_date((c.get("start") or {}).get("date"))
        return campaign_of_start(start) if start else first_campaign(c)

    mains = [c for c in cycles_of(cycles) if c.get("role") == "main_crop"
             and (campaign_of(c) or campaign) < campaign]
    if not mains:
        return None
    dated = [c for c in mains if not declared_only(c)]
    declared = [c for c in mains if declared_only(c)]
    last = max(dated, key=lambda c: iso_date((c.get("start") or {}).get("date")) or date.min) if dated else None
    if declared:
        latest_declared = max(declared, key=lambda c: campaign_of(c))
        # Facts beat declarations: a declared crop wins only from a strictly later campaign.
        if last is None or campaign_of(latest_declared) > campaign_of(last):
            last = latest_declared
    slug = species_slug(last.get("species"))
    end = last.get("end") or {}
    end_date = iso_date(end.get("date"))
    if end_date and end.get("provenance") in ("actual", "declared"):
        return PreviousCrop(slug, end_date, "harvest_recorded")
    if end_date:
        return PreviousCrop(slug, end_date, "planned_end")
    start = iso_date((last.get("start") or {}).get("date"))
    typical = maturity_for(slug, start) if start else None
    if typical:
        return PreviousCrop(slug, typical, "typical_maturity")
    return PreviousCrop(slug, None, "campaign_start")


async def parcel_calendar(*, dao, parcel_id: str, tenant_id: str, campaign: int, crop: str,
                          cover: str | None, main_sowing: date | None, regime: str | None = None) -> dict:
    """Fortnights for a plan: the main sowing window and a cover coherent with it (spec W6)."""
    eppo = crop_code(crop)
    slug = resolve_species(crop)
    env = await load_environment(dao, parcel_id, tenant_id)
    fns = campaign_fortnights(campaign)
    if main_sowing is not None and not (fns[0].start <= main_sowing <= fns[-1].end):
        raise CalendarInputError("main_sowing_outside_campaign")
    # The plan's regime (the wizard header) first, else the parcel's; never a guess.
    regime, _ = resolve_regime(regime, env)
    rows = load_rows()
    centroid = env.get("centroid") or {}

    def sowing_for(code: str | None) -> dict:
        return sowing_info(code, env.get("climate_class"), rows, env.get("country"),
                           lat=centroid.get("lat"), lon=centroid.get("lon"), irrigation=regime) if code else {}

    cycles = await fetch_crop_cycles(env["parcel_id"], tenant_id)

    def maturity_for(prev_slug: str | None, start: date) -> date | None:
        return typical_maturity(prev_slug, start, lat=centroid.get("lat"), lon=centroid.get("lon"), regime=regime)

    out = build_calendar(campaign=campaign, sowing=sowing_for(eppo),
                         perennial=get_lifecycle(slug) == "perennial" if slug else False,
                         main_sowing=fortnight_of(main_sowing).start if main_sowing else None,
                         previous=previous_crop(cycles if isinstance(cycles, dict) else None, campaign,
                                                maturity_for=maturity_for),
                         with_cover=cover is not None)
    return {"parcel_id": env["parcel_id"], "campaign": campaign, "crop": slug, "cover_crop": cover, **out}
