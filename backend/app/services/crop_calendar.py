"""Cereal phenology calendar of the yield engine: one calendar per crop, the engine's own.

The table and the milestone dates come from the parameters AquaCrop simulates with (library
crop, sourced cultivar, provincial calibration where it applies), obtained through the engine's
own crop builder, and from the engine's own daily GDD method. Nothing is copied into a second
table: a second calendar for the same crop is the thing this module exists to prevent.

All thresholds are growing degree days counted from sowing (the sowing day included).

The dates are stress-free thermal-time dates: cumulative GDD of the weather against the
thresholds. The engine itself advances stages on cumulative GDD minus delayed GDD (germination
delay in dry soil, early stress), so the simulated crop can be later than these dates. The
``phenology`` block says so: ``basis: thermal_time``, ``notes: [no_stress_delay]``.
"""
from __future__ import annotations

import functools
import json
import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from app.services import yield_calibration as yc
from app.services.engines import aquacrop_engine as aq
from app.services.region_lookup import es_province_at
from app.services.sim_crops import AQUACROP_CROPS

logger = logging.getLogger(__name__)

CEREAL_SLUGS = ("wheat", "durum_wheat", "barley")
GDD_METHOD = "aquacrop_m3"
MODEL_ID = "aquacrop_calendar"
MILESTONES = ("emergence", "flowering_start", "senescence_start", "maturity")
# Percentiles of the dates need at least as many members as the engine's ensemble.
MIN_MEMBERS = aq.MIN_ENSEMBLE_MEMBERS

_SOURCE_HISTART = {"short": "Engine assumption",
                   "detail": "The stage starting at the engine's start of yield formation (HIstart) covers heading "
                             "and flowering; it was compared against observed heading dates (see validation)"}
_SOURCE_BBCH = {"short": "BBCH",
                "detail": "Meier U. (ed.), Growth stages of mono- and dicotyledonous plants, BBCH Monograph, "
                          "Julius Kuhn-Institut (2018), cereals; CC BY 4.0"}
_SOURCE_LIBRARY = {"short": "AquaCrop",
                   "detail": "AquaCrop-OSPy library crop parameters (FAO AquaCrop), version {version}"}


# Same data directory as the calibration file (backend/data locally, /app/data in the image).
VALIDATION_PATH = yc.DATA_DIR / "phenology_validation.json"
_NOT_VALIDATED = {"status": "not_validated"}


@functools.cache
def _validation_file() -> dict | None:
    try:
        return json.loads(VALIDATION_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        logger.warning("phenology_validation_unavailable path=%s", VALIDATION_PATH, exc_info=True)
        return None


def validation_for(crop_slug: str) -> dict:
    """The measured error (engine date minus observed date) of this crop, else ``not_validated``."""
    data = _validation_file()
    try:
        groups = data["crops"].get(crop_slug) if data else None
        if not groups:
            return dict(_NOT_VALIDATED)
        return {"status": "measured", "version": data["version"], "sign": data["sign"],
                "dataset": data["dataset"], "comparisons": data["comparisons"], "groups": groups}
    except (KeyError, TypeError, AttributeError):
        logger.warning("phenology_validation_malformed path=%s", VALIDATION_PATH, exc_info=True)
        return dict(_NOT_VALIDATED)


@dataclass(frozen=True)
class CropCalendar:
    crop_slug: str
    aquacrop_crop: str
    cultivar: str
    tbase: float
    tupp: float
    emergence: float
    hi_start: float
    flowering: float
    senescence: float
    maturity: float
    model_version: str
    calibration: dict
    sources: tuple[dict, ...]


def daily_gdd_m3(tmin: float, tmax: float, tbase: float, tupp: float) -> float:
    """AquaCrop's GDD method 3: Tmax clipped to [Tbase, Tupp], Tmin clipped from above at Tupp,
    their mean floored at Tbase, minus Tbase."""
    hi = min(max(tmax, tbase), tupp)
    lo = min(tmin, tupp)
    return max((hi + lo) / 2.0, tbase) - tbase


@functools.lru_cache(maxsize=64)
def _calendar(crop_slug: str, region: tuple | None, irrigation: str) -> CropCalendar:
    aquacrop_crop = AQUACROP_CROPS[crop_slug]
    region_dict = {"scheme": region[0], "code": region[1], "name": region[2]} if region else None
    overrides = yc.calibrated_phenology(crop_slug, irrigation, region_dict)
    crop = aq._aquacrop_crop(aquacrop_crop, date(2000, 1, 1), overrides or {})
    version = aq._version()
    sources: list[dict] = []
    cultivar = aq.CULTIVARS.get(aquacrop_crop)
    if cultivar:
        sources.append({"short": "Cultivar", "detail": cultivar["source"]})
    sources.append({**_SOURCE_LIBRARY, "detail": _SOURCE_LIBRARY["detail"].format(version=version)})
    if overrides:
        cal = yc._calibration(crop_slug)
        sources.append({"short": "Calibration",
                        "detail": f"Provincial phenology calibration {cal['version']}; ranges: "
                                  f"{cal['sources']['phenology_ranges']}"})
        calibration = {"status": "calibrated", "scheme": region[0],
                       "region": {"code": region[1], "name": region[2]}, "version": cal["version"]}
    else:
        calibration = {"status": "builtin", "scheme": None, "region": None, "version": None}
    sources += [_SOURCE_HISTART, _SOURCE_BBCH]
    return CropCalendar(
        crop_slug=crop_slug, aquacrop_crop=aquacrop_crop, cultivar=aquacrop_crop,
        tbase=float(crop.Tbase), tupp=float(crop.Tupp), emergence=float(crop.Emergence),
        hi_start=float(crop.HIstart), flowering=float(crop.Flowering),
        senescence=float(crop.Senescence), maturity=float(crop.Maturity),
        model_version=version, calibration=calibration, sources=tuple(sources))


def effective_calendar(crop_slug: str, *, lat: float | None = None, lon: float | None = None,
                       irrigation: str | None = None) -> CropCalendar:
    """The parameters the engine simulates ``crop_slug`` with at this place and regime.

    ``irrigation`` uses the engine's vocabulary (``rainfed`` | ``full``); the regional
    calibration applies only to the calibrated regime. Without a point, no calibration.
    """
    if crop_slug not in CEREAL_SLUGS:
        raise ValueError(f"no cereal calendar for crop {crop_slug!r}")
    region = es_province_at(lat, lon)
    key = (region["scheme"], region["code"], region["name"]) if region else None
    return _calendar(crop_slug, key, irrigation or yc.CALIBRATED_IRRIGATION)


def stage_table(cal: CropCalendar) -> list[dict]:
    """The five stages with GDD from sowing and BBCH ranges (stages the engine lacks stay inside
    ``vegetative``)."""
    rows = (
        ("emergence", 0.0, cal.emergence, 0, 9),
        ("vegetative", cal.emergence, cal.hi_start, 10, 49),
        ("flowering", cal.hi_start, cal.hi_start + cal.flowering, 51, 69),
        ("grain_filling", cal.hi_start + cal.flowering, cal.maturity, 71, 87),
        ("maturity", cal.maturity, cal.maturity, 89, 89),
    )
    return [{"stage": s, "gddMin": lo, "gddMax": hi, "baseTemp": cal.tbase, "bbchFrom": b0, "bbchTo": b1}
            for s, lo, hi, b0, b1 in rows]


def _thresholds(cal: CropCalendar) -> dict[str, float]:
    return {"emergence": cal.emergence, "flowering_start": cal.hi_start,
            "senescence_start": cal.senescence, "maturity": cal.maturity}


def _check_finite(w: Any) -> None:
    """A non-finite temperature would poison the running total: refuse, naming the day."""
    for name in ("tmin_c", "tmax_c"):
        v = getattr(w, name)
        if not isinstance(v, (int, float)) or not math.isfinite(v):
            raise ValueError(f"non-finite {name} on {w.day.isoformat()}: {v!r}")


def _crossing(days: Sequence[Any], cum: float, gdd: float, tbase: float, tupp: float) -> tuple[date | None, float]:
    """First day the running total reaches ``gdd``, and the total at the end of ``days``."""
    for w in days:
        _check_finite(w)
        cum += daily_gdd_m3(w.tmin_c, w.tmax_c, tbase, tupp)
        if cum >= gdd:
            return w.day, cum
    return None, cum


def milestone_dates(sowing_date: date, observed_days: Sequence[Any], analog_members: Sequence[Sequence[Any]],
                    cal: CropCalendar) -> dict[str, dict]:
    """Date of each milestone: reached in observed weather, projected over the analog members
    (each a full series whose days after the observed ones are that member's weather), or unavailable.

    P10/P50/P90 of the member dates: the engine's percentile definition (linear interpolation
    between order statistics) on the day ordinals, rounded to a day.
    """
    observed = [w for w in observed_days if w.day >= sowing_date]
    last_observed = observed_days[-1].day if observed_days else None
    out: dict[str, dict] = {}
    for name, gdd in _thresholds(cal).items():
        reached, cum_end = _crossing(observed, 0.0, gdd, cal.tbase, cal.tupp)
        if reached is not None:
            out[name] = {"gdd": gdd, "status": "reached", "date": reached.isoformat()}
            continue
        if not analog_members:
            out[name] = {"gdd": gdd, "status": "unavailable", "reason": "no_analog_members"}
            continue
        if len(analog_members) < MIN_MEMBERS:
            out[name] = {"gdd": gdd, "status": "unavailable", "reason": "too_few_members"}
            continue
        dates: list[date] = []
        for member in analog_members:
            tail = [w for w in member
                    if w.day >= sowing_date and (last_observed is None or w.day > last_observed)]
            found, _ = _crossing(tail, cum_end, gdd, cal.tbase, cal.tupp)
            if found is not None:
                dates.append(found)
        if not dates:
            out[name] = {"gdd": gdd, "status": "unavailable", "reason": "not_reached"}
        elif len(dates) < len(analog_members):
            out[name] = {"gdd": gdd, "status": "unavailable", "reason": "not_reached_by_all_members"}
        else:
            pct = aq._pct([float(d.toordinal()) for d in dates])
            out[name] = {"gdd": gdd, "status": "projected", "n_members": len(dates),
                         **{f"date_{k}": date.fromordinal(round(v)).isoformat() for k, v in pct.items()}}
    return out


def model_info(cal: CropCalendar) -> dict:
    return {"id": MODEL_ID, "cultivar": cal.cultivar, "version": cal.model_version}


def phenology_block(sowing_date: date, observed_days: Sequence[Any], analog_members: Sequence[Sequence[Any]],
                    cal: CropCalendar) -> dict:
    """The ``phenology`` block of the simulation and in-season answers."""
    return {"gdd_method": GDD_METHOD, "base_temp": cal.tbase, "upper_cutoff": cal.tupp, "origin": "sowing",
            "basis": "thermal_time", "notes": ["no_stress_delay"],
            "validation": validation_for(cal.crop_slug), "model": model_info(cal), "calibration": dict(cal.calibration),
            "milestones": milestone_dates(sowing_date, observed_days, analog_members, cal)}


def cereal_slugs() -> tuple[str, ...]:
    return CEREAL_SLUGS


def cereal_stage_response(crop_slug: str, lat: float | None, lon: float | None, irrigation: str) -> dict:
    """The ``phenology-stages`` answer for a cereal: the engine's table and where it comes from."""
    cal = effective_calendar(crop_slug, lat=lat, lon=lon, irrigation=_ENGINE_REGIME[irrigation])
    logger.info("phenology_stages_calendar crop=%s calibration=%s irrigation=%s",
                crop_slug, cal.calibration["status"], irrigation)
    return {"species": crop_slug, "stages": stage_table(cal), "gddMethod": GDD_METHOD,
            "upperCutoff": cal.tupp, "origin": "sowing", "model": model_info(cal),
            "calibration": dict(cal.calibration), "validation": validation_for(crop_slug),
            "sources": [dict(s) for s in cal.sources]}


# The public vocabulary is rainfed | irrigated; the engine calls the irrigated scenario "full".
_ENGINE_REGIME = {"rainfed": "rainfed", "irrigated": "full"}
