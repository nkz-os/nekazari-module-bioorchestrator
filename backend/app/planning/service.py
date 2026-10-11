"""Planning use cases: read the parcel, apply the pure rules, return contract dicts."""
from __future__ import annotations

from datetime import date

from app.common.attribution import attach_attributions, rows_source_ids
from app.core.knowledge import KnowledgeSource
from app.graph.recommend import sowing_info
from app.planning.estimates import (
    aquacrop_status,
    disagreement,
    trial_estimate,
    usable_rows,
    visible,
)
from app.planning.evidence import load_crop_evidence
from app.planning.fit import (
    MULTI_YEAR,
    REGIME_MISSING,
    ROTATION_UNKNOWN,
    VERDICT_ORDER,
    ParcelClimate,
    frost_reasons,
    soil_reasons,
    verdict_of,
    water_reasons,
)
from app.planning.history import history_complete, past_crops
from app.planning.reference import load_reference
from app.planning.rotation import rotation_reasons
from app.planning.varieties import variety_stats
from app.services.crop_cycles_client import fetch_crop_cycles
from app.services.crop_reference import CROP_REFERENCE
from app.services.soil_client import assess_soil_suitability
from app.services.sowing_windows import load_rows
from app.species_registry import (
    catalog_code,
    get_lifecycle,
    get_species_info,
    is_woody,
    list_species,
    resolve_species,
)

REGIMES = ("secano", "regadío")
NO_SOWING = {"sowing_type": None, "sowing_window": None, "source": None, "typical_sowing_doy": None}


class ParcelNotFound(LookupError):
    pass


class UnknownCrop(ValueError):
    pass


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


async def load_environment(dao, parcel_id: str, tenant_id: str) -> dict:
    env = await dao.get_parcel_environment(parcel_id, tenant_id)
    if "error" in env:
        raise ParcelNotFound(env["error"])
    return env


async def parcel_fit(*, dao, source: KnowledgeSource, parcel_id: str, tenant_id: str,
                     campaign: int, regime: str | None) -> dict:
    env = await load_environment(dao, parcel_id, tenant_id)
    regime_used, regime_source = resolve_regime(regime, env)
    cycles = await fetch_crop_cycles(env["parcel_id"], tenant_id)
    history = past_crops(cycles, campaign) if cycles is not None else None
    ref = await load_reference(source)
    detail = env.get("climate_detail") or {}
    climate = ParcelClimate(detail.get("coldest_month_min_c"), detail.get("annual_rainfall_mm"),
                            detail.get("annual_et0_mm"))
    soil = env.get("soil") or {}
    centroid = env.get("centroid") or {}
    rows = load_rows()
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
        reasons += frost_reasons(sowing.get("sowing_type"), climate.coldest_month_min_c,
                                 tol.frost_damage_c if tol else None)
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
        if lifecycle == "perennial":
            reasons.append(MULTI_YEAR)
        trials = has_trials(eppo, ref.trial_eppo)
        species.append({
            "species": slug, "eppo": eppo, "names": dict(info.get("common_names") or {}), "lifecycle": lifecycle,
            "verdict": verdict_of(reasons), "reasons": reasons, "sowing": _sowing(sowing),
            "has_variety_trials": trials, "estimate_providers": ["trial_evidence"] if trials else [],
        })
    species.sort(key=lambda s: (VERDICT_ORDER[s["verdict"]], s["species"]))
    return {
        "parcel_id": env["parcel_id"], "campaign": campaign, "graphs": [source.graph_id], "species": species,
        "facts": {"climate_class": env.get("climate_class"), "climate_source": detail.get("source") or "unavailable",
                  "soil_texture": soil.get("texture"), "soil_ph": soil.get("ph"),
                  "soil_source": soil.get("source") or "unavailable", "regime": regime_used,
                  "regime_source": regime_source, "history_available": cycles is not None},
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
               "n_environments": n_env, "varieties": stats, "data_gaps": data_gaps}
    return attach_attributions(payload, rows_source_ids(ev["rows"]))


async def parcel_estimates(*, dao, source: KnowledgeSource, parcel_id: str, tenant_id: str, crop: str,
                           variety: str | None, regime: str, management: str, sowing_date: date | None) -> dict:
    eppo = crop_code(crop)
    env = await load_environment(dao, parcel_id, tenant_id)
    inputs = {"regime": regime, "management": management, "sowing_date": sowing_date,
              "sowing_source": "planned" if sowing_date else "none",
              "soil_source": (env.get("soil") or {}).get("source") or "unavailable",
              "climate_source": (env.get("climate_detail") or {}).get("source") or "unavailable"}
    base = {"parcel_id": env["parcel_id"], "crop": eppo, "variety": variety, "inputs": inputs,
            "unavailable": [aquacrop_status()]}
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
