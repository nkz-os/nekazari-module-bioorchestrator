"""Late-frost frequency of a parcel from its daily history (pure core, no I/O).

For each past year the crop's sensitive stage window is found with the engine's own thermal-time
calendar (``crop_calendar``: AquaCrop GDD method 3, counted from sowing, sowing day included) over
that year's daily Tmin/Tmax; the year is "hit" when any day of the window (both ends included) has
Tmin <= the stage threshold. Frequency = hit years / complete years (a year is hit when ANY evaluated stage is; complete when ALL are).

Thresholds, transcribed from the primary sources:

* Wheat: Shroyer, J.P., Mikesell, M.E. and Paulsen, G.M., "Spring Freeze Injury to Kansas Wheat",
  Kansas State University Cooperative Extension Service, publication C-646, March 1995, table 1,
  2 h of exposure: jointing -4 C, boot -2 C, heading -1 C, flowering -1 C, milk -2 C, dough -2 C.
  Used here: flowering -1 C, grain filling -2 C.
* Other field crops: Snyder, R.L. and de Melo-Abreu, J.P. (2005), "Frost protection: fundamentals,
  practice and economics", FAO, vol. 1, table 4.5 (after Ventskevich, 1958), flowering column
  (a range; the warmer end is used), e.g. barley and oat -1 C, pea and lentil -2 C,
  sunflower -2 C, maize -1 C.

On the validated risk-margin path the frequency uses weather-api's declared frost-risk margin
(validated with stations); the minimum / warm-bias note below applies to the raw-Tmin path.

The frequency is a MINIMUM: the source thresholds refer to 2 h of exposure while the daily Tmin is
only a proxy of that, and the reanalysis Tmin has a warm bias (cold hollows are not resolved).
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Literal

from app.services.crop_calendar import CropCalendar, daily_gdd_m3

SEASON_DAYS = 365  # one season, as sim_weather
RISK_SHARE = 0.2  # 1 year in 5 (owner, 2026-11-17)
TMIN_SOURCE = "ERA5"


@dataclass(frozen=True)
class StageThreshold:
    stage: str
    threshold_c: float
    source: str


@dataclass(frozen=True)
class FrostFrequency:
    years_hit: int
    years_total: int
    hit_years: tuple[int, ...]


@dataclass(frozen=True)
class FrostHistory:
    days: Mapping[date, tuple[float | None, float | None]]  # day -> (tmin_c, tmax_c)
    tmin_caveat: Literal["warm_bias", "unvalidated_region"]
    # Validated frost-risk Tmin per day: (0 C field, -2 C field). Empty on histories cached before
    # these fields existed: those are read as raw Tmin.
    risk: Mapping[date, tuple[float | None, float | None]] = field(default_factory=dict)
    risk_validated: tuple[bool, bool] = (False, False)  # per field: the region passed its validation
    risk_labels: tuple[str | None, str | None] = (None, None)  # provenance label per field


STAGE_THRESHOLDS: dict[str, tuple[StageThreshold, ...]] = {
    "wheat": (StageThreshold("flowering", -1.0, "kstate_c646"),
              StageThreshold("grain_filling", -2.0, "kstate_c646")),
    "barley": (StageThreshold("flowering", -1.0, "fao_2005_t45"),),
}

# Crops with a sourced threshold but no stage model: only used to choose ``no_stage_model``.
FAO_ONLY_THRESHOLDS: dict[str, float] = {
    "oat": -1.0, "pea": -2.0, "lentil": -2.0, "sunflower": -2.0, "maize": -1.0,
}

_PENDING = {"loading", "absent"}

# The two calibration thresholds of the weather API's frost-risk fields are 0 C and -2 C; a stage
# threshold at or below their midpoint uses the hard-frost field. No new agronomic constant.
HARD_FROST_SPLIT_C = -1.5


def sowing_date(year: int, doy: int) -> date:
    """Day-of-year in ``year``; 366 in a common year clamps to 31 December."""
    # ASSUMPTION: doy is a 1-based day of year; values past the year end clamp to its last day.
    return date(year, 1, 1) + timedelta(days=min(doy, 366 if _leap(year) else 365) - 1)


def window_mid_doy(window: Mapping | None) -> int | None:
    """Sowing day backing up a crop with a curated sowing window but no typical sowing day.

    The middle day of the window (first day of its first month to last day of its last month,
    wrapping over the year end), as a day of a common year. Not the window start: the thermal
    model has no vernalisation or photoperiod, so an early sowing heads far too early and
    inflates the frost count.
    """
    # ASSUMPTION: the window is monthly sown-area shares; its middle stands for the typical sowing.
    w = window or {}
    first, last = w.get("start_month"), w.get("end_month")
    if not all(isinstance(m, int) and not isinstance(m, bool) and 1 <= m <= 12 for m in (first, last)):
        return None
    start = date(2001, first, 1)
    end_year = 2001 if last >= first else 2002
    end = date(end_year + (last == 12), 1 if last == 12 else last + 1, 1) - timedelta(days=1)
    return (start + (end - start) // 2).timetuple().tm_yday


def _leap(y: int) -> bool:
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


def _stage_limits(cal: CropCalendar, stage: str) -> tuple[float, float]:
    """(lower, upper) GDD limits of the window; for grain filling the lower limit is the end of
    flowering (the window starts the day after)."""
    if stage == "flowering":
        return cal.hi_start, cal.hi_start + cal.flowering
    if stage == "grain_filling":
        return cal.hi_start + cal.flowering, cal.maturity
    raise ValueError(f"unknown stage {stage!r}")


def stage_windows(cal: CropCalendar, sowing_doy: int,
                  days: Mapping[date, tuple[float | None, float | None]],
                  stage: str) -> dict[int, tuple[date, date]]:
    """Window (first day, last day) of ``stage`` per sowing year; only complete campaigns.

    A year is dropped when a day is missing or has a null Tmin/Tmax before the end of the
    window, or when the series ends before it.
    """
    lower, upper = _stage_limits(cal, stage)
    out: dict[int, tuple[date, date]] = {}
    if not days:
        return out
    for year in range(min(days).year, max(days).year + 1):
        day = sowing_date(year, sowing_doy)
        cum = 0.0
        start: date | None = None
        end: date | None = None
        ok = True
        last_day = day + timedelta(days=SEASON_DAYS - 1)
        while end is None:
            tt = days.get(day)
            if (day > last_day or tt is None or tt[0] is None or tt[1] is None
                    or not math.isfinite(tt[0]) or not math.isfinite(tt[1])):
                ok = False
                break
            cum += daily_gdd_m3(tt[0], tt[1], cal.tbase, cal.tupp)
            if start is None and cum >= lower:
                # grain filling begins the day after flowering ends
                start = day + timedelta(days=1) if stage == "grain_filling" else day
            if cum >= upper:
                end = day
            day += timedelta(days=1)
        if ok and start is not None and end is not None and start <= end:
            out[year] = (start, end)
    return out


def late_frost_frequency(tmin_by_day: Mapping[date, float],
                         windows_by_year: Mapping[int, tuple[date, date]],
                         threshold_c: float) -> FrostFrequency:
    hits = []
    for year in sorted(windows_by_year):
        first, last = windows_by_year[year]
        d = first
        while d <= last:
            t = tmin_by_day.get(d)
            if t is not None and t <= threshold_c:
                hits.append(year)
                break
            d += timedelta(days=1)
    return FrostFrequency(len(hits), len(windows_by_year), tuple(hits))


def _span(w: tuple[date, date]):
    d = w[0]
    while d <= w[1]:
        yield d
        d += timedelta(days=1)


def _risk_field(history: FrostHistory, threshold_c: float) -> int | None:
    """Index into ``history.risk`` (0: 0 C field, 1: -2 C field) of the validated field for a stage threshold,
    or None for raw Tmin."""
    if not history.risk:
        return None
    if threshold_c <= HARD_FROST_SPLIT_C and history.risk_validated[1]:
        return 1
    if history.risk_validated[0]:
        return 0
    return None


def _unknown(why: str) -> dict:
    return {"code": "frost_late_unknown", "gate": "frost", "params": {"why": why}}


def unknown_reason(slug: str, why: str) -> dict:
    """A ``frost_late_unknown`` for ``slug``: ``why`` when it has a stage model, else the reason it has none."""
    if slug in STAGE_THRESHOLDS:
        return _unknown(why)
    return _unknown("no_stage_model" if slug in FAO_ONLY_THRESHOLDS else "no_threshold")


def late_frost_reason(slug: str, *, cal: CropCalendar | None, sowing_doy: int | None,
                      history: FrostHistory | None, status: str) -> dict:
    """ONE planning-fit reason: seasons complete for every stage, hit when any stage is hit."""
    stages = STAGE_THRESHOLDS.get(slug)
    if stages is None:
        return _unknown("no_stage_model" if slug in FAO_ONLY_THRESHOLDS else "no_threshold")
    if cal is None or sowing_doy is None:
        return _unknown("no_stage_model")
    if status in _PENDING:
        return _unknown("history_pending")
    if status != "ready" or history is None:
        # ASSUMPTION: any status other than ready/loading/absent (incl. failed) is unavailable.
        return _unknown("history_unavailable")
    raw_tmin = {d: t[0] for d, t in history.days.items() if t[0] is not None}
    # A season counts only when complete for EVERY evaluated stage; it is hit when ANY stage is hit.
    complete: set[int] | None = None
    hit: set[int] = set()
    used: list[int | None] = []
    for st in stages:
        windows = stage_windows(cal, sowing_doy, history.days, st.stage)
        idx = _risk_field(history, st.threshold_c)
        used.append(idx)
        if idx is None:
            tmin = raw_tmin
        else:
            tmin = {d: r[idx] for d, r in history.risk.items() if r[idx] is not None}
            # A validated field null on a window day (gap) leaves that season out.
            windows = {y: w for y, w in windows.items()
                       if all(d in tmin for d in _span(w))}
        freq = late_frost_frequency(tmin, windows, st.threshold_c)
        complete = set(windows) if complete is None else complete & set(windows)
        hit.update(freq.hit_years)
    if not complete:
        return _unknown("history_unavailable")
    years_hit = len(hit & complete)
    years_total = len(complete)
    risky = years_hit / years_total >= RISK_SHARE - 1e-9
    # ParamValue is scalar-only: stages and thresholds travel as "+"- and "/"-joined strings, in stage order.
    params = {"years_hit": years_hit, "years_total": years_total,
              "stage": "+".join(st.stage for st in stages),
              "thresholds_c": "/".join(f"{st.threshold_c:g}" for st in stages),
              "threshold_source": stages[0].source,
              "tmin_source": TMIN_SOURCE, "tmin_caveat": history.tmin_caveat}
    if all(i is not None for i in used):
        params["tmin_basis"] = "risk_margin"
        params["tmin_caveat"] = "risk_margin"
        labels = {history.risk_labels[i] for i in used if history.risk_labels[i]}
        # ASSUMPTION: when stages use two fields with different labels, the label is omitted.
        if len(labels) == 1:
            params["tmin_label"] = labels.pop()
    else:
        # mixed (some stages validated, some raw): the raw caveat stays
        params["tmin_basis"] = "mixed" if any(i is not None for i in used) else "raw"
    return {"code": "frost_late_risk" if risky else "frost_late_info", "gate": "frost", "params": params}
