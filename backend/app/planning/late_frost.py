"""Late-frost frequency of a parcel from its daily history (pure core, no I/O).

For each past year the crop's sensitive stage window is found with the engine's own thermal-time
calendar (``crop_calendar``: AquaCrop GDD method 3, counted from sowing, sowing day included) over
that year's daily Tmin/Tmax; the year is "hit" when any day of the window (both ends included) has
Tmin <= the stage threshold. Frequency = hit years / complete years.

Thresholds, transcribed from the primary sources:

* Wheat: Shroyer, J.P., Mikesell, M.E. and Paulsen, G.M., "Spring Freeze Injury to Kansas Wheat",
  Kansas State University Cooperative Extension Service, publication C-646, March 1995, table 1,
  2 h of exposure: jointing -4 C, boot -2 C, heading -1 C, flowering -1 C, milk -2 C, dough -2 C.
  Used here: flowering -1 C, grain filling -2 C.
* Other field crops: Snyder, R.L. and de Melo-Abreu, J.P. (2005), "Frost protection: fundamentals,
  practice and economics", FAO, vol. 1, table 4.5 (after Ventskevich, 1958), flowering column
  (a range; the warmer end is used), e.g. barley and oat -1 C, pea and lentil -2 C,
  sunflower -2 C, maize -1 C.

The frequency is a MINIMUM: the source thresholds refer to 2 h of exposure while the daily Tmin is
only a proxy of that, and the reanalysis Tmin has a warm bias (cold hollows are not resolved).
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
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


def sowing_date(year: int, doy: int) -> date:
    """Day-of-year in ``year``; 366 in a common year clamps to 31 December."""
    # ASSUMPTION: doy is a 1-based day of year; values past the year end clamp to its last day.
    return date(year, 1, 1) + timedelta(days=min(doy, 366 if _leap(year) else 365) - 1)


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


def _unknown(why: str) -> dict:
    return {"code": "frost_late_unknown", "gate": "frost", "params": {"why": why}}


def unknown_reason(slug: str, why: str) -> dict:
    """A ``frost_late_unknown`` for ``slug``: ``why`` when it has a stage model, else the reason it has none."""
    if slug in STAGE_THRESHOLDS:
        return _unknown(why)
    return _unknown("no_stage_model" if slug in FAO_ONLY_THRESHOLDS else "no_threshold")


def late_frost_reason(slug: str, *, cal: CropCalendar | None, sowing_doy: int | None,
                      history: FrostHistory | None, status: str) -> dict:
    """ONE planning-fit reason: the worst stage (highest hit share, earliest on a tie)."""
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
    tmin = {d: t[0] for d, t in history.days.items() if t[0] is not None}
    best: tuple[StageThreshold, FrostFrequency] | None = None
    for st in stages:
        windows = stage_windows(cal, sowing_doy, history.days, st.stage)
        freq = late_frost_frequency(tmin, windows, st.threshold_c)
        if freq.years_total == 0:
            continue
        # strict > keeps the earliest stage on a tie
        if best is None or freq.years_hit * best[1].years_total > best[1].years_hit * freq.years_total:
            best = (st, freq)
    if best is None:
        return _unknown("history_unavailable")
    st, freq = best
    risky = freq.years_hit / freq.years_total >= RISK_SHARE - 1e-9
    return {"code": "frost_late_risk" if risky else "frost_late_info", "gate": "frost",
            "params": {"years_hit": freq.years_hit, "years_total": freq.years_total, "stage": st.stage,
                       "threshold_c": st.threshold_c, "threshold_source": st.source,
                       "tmin_source": TMIN_SOURCE, "tmin_caveat": history.tmin_caveat}}
