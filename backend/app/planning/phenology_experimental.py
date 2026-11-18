"""Experimental vernalization-photoperiod phenology: parameter selection and validation evidence.

Pure functions apart from loading the two versioned data files (cached). A missing or malformed
file raises; the caller decides how to report it.
"""
from __future__ import annotations

import functools
import json
from dataclasses import dataclass
from typing import Any

from app.services import yield_calibration as yc
from app.services.country_lookup import country_at
from app.services.phenology_vp import VPParams

PARAMS_PATH = yc.DATA_DIR / "phenology_vp_params.json"
VALIDATION_PATH = yc.DATA_DIR / "phenology_vp_validation.json"

# The 27 EU member states, ISO 3166-1 alpha-2.
# Source: European Union, "EU countries", https://european-union.europa.eu/principles-countries-history/eu-countries_en
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


def _milestone(entry: dict, zone: str, data: dict) -> dict:
    out = dict(entry)
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
