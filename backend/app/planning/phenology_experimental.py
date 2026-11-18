"""Experimental vernalization-photoperiod phenology: parameter selection and validation evidence.

Pure functions apart from loading the two versioned data files (cached). A missing or malformed
file raises; the caller decides how to report it.
"""
from __future__ import annotations

import asyncio
import functools
import json
import logging
from dataclasses import dataclass
from typing import Any

from app.planning.cereal_type import CerealType, cereal_group, resolve_cereal_type
from app.services import crop_calendar as cc
from app.services import yield_calibration as yc
from app.services.country_lookup import country_at
from app.services.engines import aquacrop_engine as aq
from app.services.phenology_vp import MODEL_VERSION, VPParams, vp_block_milestones

logger = logging.getLogger(__name__)

PARAMS_PATH = yc.DATA_DIR / "phenology_vp_params.json"
VALIDATION_PATH = yc.DATA_DIR / "phenology_vp_validation.json"

# The 27 EU member states, ISO 3166-1 alpha-2.
# Source: European Union, "EU countries" (official list of member states).
EU_MEMBERS = frozenset({
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE",
    "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
})

_DATASET = {"ES": "GENVCE variety trials (Spain)", "DE": "DWD phenology (Germany)"}
_MATURITY_MIN_N = 10


@dataclass(frozen=True)
class Selection:
    set: str  # "ES" | "DE" | "EU_joint": where the parameters come from
    params: VPParams
    at_grid_bound: list[str]
    zone: str | None  # "ES" | "DE" when the set is validated at the parcel, else None


@dataclass(frozen=True)
class Unavailable:
    reason: str  # "outside_eu" | "country_unknown"


@functools.cache
def _params_file() -> dict:
    return json.loads(PARAMS_PATH.read_text(encoding="utf-8"))


@functools.cache
def _validation_file() -> dict:
    return json.loads(VALIDATION_PATH.read_text(encoding="utf-8"))


def _set_for(group: str, country: str) -> tuple[str, str | None]:
    """(parameter set, validation zone) for a group in an EU country."""
    sets = _params_file()["sets"]
    if country == "ES":
        return "ES", "ES"
    if country == "DE":
        if group in sets["DE"]:
            return "DE", "DE"
        return "ES", None  # no German data for this group: Spanish parameters, unvalidated
    if group in sets["EU_joint"]:
        return "EU_joint", None
    return "ES", None


def select_parameters(group: str, lat: float, lon: float) -> Selection | Unavailable:
    country = country_at(lat, lon)
    if country is None:
        return Unavailable("country_unknown")
    if country not in EU_MEMBERS:
        return Unavailable("outside_eu")
    set_name, zone = _set_for(group, country)
    row = _params_file()["sets"][set_name][group]
    params = VPParams(rp=row["rp"], rv=row["rv"], t2=row["t2"], mat_p=row["mat_p"],
                      t1_adj=float(_params_file()["t1_adj"]))
    return Selection(set=set_name, params=params, at_grid_bound=list(row["at_grid_bound"]), zone=zone)


def parameters_block(sel: Selection) -> dict[str, Any]:
    p = sel.params
    return {
        "set": sel.set, "rp": p.rp, "rv": p.rv, "t2": p.t2, "t1_adj": 401,
        "maturity_gdd": p.mat_p, "at_grid_bound": list(sel.at_grid_bound),
        "fitted_on": {
            "ES": "Spanish cereal variety trials of the group",
            "DE": "German DWD phenology pairs of the group",
            "EU_joint": "Spanish trials and German DWD pairs of the group, jointly",
        }[sel.set],
    }


_VERDICT = {"gana": "wins", "no_se_pudo_comprobar": "not_checkable"}


def _verdict(raw: Any) -> dict[str, str] | None:
    """Served verdict: the comparison is against M1 only, in neutral English codes."""
    if raw is None:
        return None
    result = _VERDICT.get(raw) if isinstance(raw, str) else None
    if result is None:
        logger.warning("phenology_vp_unknown_verdict value=%r", raw)
        result = "unknown"
    return {"vs": "m1", "result": result}


def _milestone(entry: dict, zone: str, data: dict) -> dict:
    out = dict(entry)
    if "verdict" in out:
        out["verdict"] = _verdict(out["verdict"])
    out["dataset"] = _DATASET[zone]
    out["partition"] = data["partitions"][zone]
    out["report"] = data["report"]
    return out


def validation_block(group: str, zone: str | None) -> dict[str, Any]:
    """Per-milestone evidence for a group in a validated zone; no figures when ``zone`` is None."""
    emergence = {"status": "same_as_operative"}
    if zone is None:
        return {"flowering_start": {"status": "unvalidated_zone"},
                "maturity": {"status": "unvalidated_zone"}, "emergence": emergence}
    data = _validation_file()
    rows = data["zones"].get(zone, {}).get(group)
    block: dict[str, Any] = {"emergence": emergence}
    for m in ("flowering_start", "maturity"):
        entry = rows.get(m) if rows else None
        if entry is None:
            block[m] = {"status": "not_tested"}
            continue
        entry = dict(entry)
        if m == "maturity" and (entry.get("n") or 0) < _MATURITY_MIN_N and entry["status"] == "measured":
            entry["status"] = "not_tested"
            entry["descriptive"] = True
        block[m] = _milestone(entry, zone, data)
    return block


def experimental_sources() -> list[dict[str, str]]:
    return [
        {"short": "APSIM-Wheat 7.5",
         "detail": "Zheng, Chenu, Doherty and Chapman (2015), APSIM-Wheat phenology documentation"},
        {"short": "APSIM day length", "detail": "APSIM710 science.f90 day_length"},
        {"short": "DWD",
         "detail": "Phenology and climate data, CC BY 4.0. Source: Deutscher Wetterdienst"},
        {"short": "GENVCE", "detail": "Spanish cereal variety trials"},
        {"short": "Vernalization experiment",
         "detail": "Out-of-sample comparison against the operative calendar and a fixed-date "
                   "baseline, November 2026"},
        {"short": "Grid-bound parameters",
         "detail": "Parameters at a fitting-grid bound are fitting results, not physiological traits"},
    ]


MODEL_ID = "vernalization_photoperiod"
# The parameters were fitted with grid-cell Tmin; the endpoint's archive series is altitude-corrected.
NOTE_TMIN = "tmin_differs_from_fit"


def builtin_thresholds(slug: str) -> tuple[float, float, float]:
    """(tbase, tupp, emergence GDD) of the cultivar without provincial calibration, as in the fit."""
    c = cc.effective_calendar(slug)  # no point: no regional calibration
    return c.tbase, c.tupp, c.emergence


def _cereal_type_block(ct: CerealType | None, variety: str | None) -> dict[str, Any]:
    if ct is None:
        return {"value": None, "source": None, "variety": variety}
    return {"value": ct.value, "source": ct.source, "variety": ct.variety}


def _model_version() -> str:
    """Code version plus params-file version; the code version alone when the file cannot be read."""
    try:
        return f"{MODEL_VERSION}+{_params_file()['version']}"
    except Exception:  # noqa: BLE001 - the fallback block must not depend on any data file
        return MODEL_VERSION


def _block(state: str, reason: str | None, cereal_type: dict, *, parameters=None, milestones=None,
           validation=None, notes=None) -> dict[str, Any]:
    return {
        "model": {"id": MODEL_ID, "version": _model_version(),
                  "role": "experimental", "operative": False},
        "state": state, "reason": reason, "cereal_type": cereal_type,
        "parameters": parameters, "milestones": milestones, "validation": validation,
        "sources": experimental_sources(), "notes": list(notes or []),
    }


async def _no_lookup(eppo: str):
    raise RuntimeError("no cereal-type source configured")


def _compute(sowing_date, observed_days, members, lat, tbase, tupp, emergence, params, as_of) -> dict:
    out = vp_block_milestones(sowing_date, observed_days, members, lat, tbase, tupp, emergence, params, as_of)
    out["emergence"] = {**out["emergence"], "shared_with": "aquacrop_calendar"}
    return out


async def experimental_block(*, slug: str, variety: str | None, lat: float, lon: float, sowing_date,
                             observed, analogs, fetch_classes=None, ctx: str = "") -> dict[str, Any]:
    """The ``experimental`` block of the parcel phenology answer. Never raises.

    ``observed`` is the endpoint's observed weather (or None when it could not be assembled) and
    ``analogs`` its analog years: the same series the operative calendar uses. Nothing here feeds an
    operative consumer. Any error becomes ``unavailable``/``internal_error`` with a log line.
    """
    ct: CerealType | None = None
    try:
        ct = await resolve_cereal_type(slug, variety, fetch_classes=fetch_classes or _no_lookup)
        ctype = _cereal_type_block(ct, variety)
        group = cereal_group(slug, ct.value)
        if group is None:
            return _block("unavailable", ct.reason or "no_cereal_type", ctype)
        sel = select_parameters(group, lat, lon)
        if isinstance(sel, Unavailable):
            return _block("unavailable", sel.reason, ctype)
        parameters = parameters_block(sel)
        if observed is None:
            return _block("unavailable", "weather_unavailable", ctype, parameters=parameters)
        tbase, tupp, emergence = builtin_thresholds(slug)
        members = [aq.project_weather(observed.weather, analogs[y], sowing_date, y) for y in sorted(analogs)]
        as_of = observed.weather[-1].day
        milestones = await asyncio.to_thread(_compute, sowing_date, observed.weather, members, lat, tbase,
                                             tupp, emergence, sel.params, as_of)
        return _block("done", None, ctype, parameters=parameters, milestones=milestones,
                      validation=validation_block(group, sel.zone), notes=[NOTE_TMIN])
    except Exception:
        logger.exception("parcel_phenology_experimental_failed %s", ctx)
        return _block("unavailable", "internal_error", _cereal_type_block(ct, variety))


async def safe_experimental_block(**kwargs) -> dict[str, Any]:
    """Outermost guard: nothing from the experimental path escapes into the operative answer."""
    try:
        return await experimental_block(**kwargs)
    except Exception:
        logger.exception("parcel_phenology_experimental_failed %s", kwargs.get("ctx", ""))
        return {"model": {"id": MODEL_ID, "version": MODEL_VERSION, "role": "experimental",
                          "operative": False},
                "state": "unavailable", "reason": "internal_error",
                "cereal_type": {"value": None, "source": None, "variety": kwargs.get("variety")},
                "parameters": None, "milestones": None, "validation": None,
                "sources": experimental_sources(), "notes": []}
