"""Sowing date for one season from that season's weather (deterministic rule).

The window opens on the temperature crossing of the crop's rule and stays open
``window_days``; the crop is sown on the first window day that meets the rain
trigger and the tempero check, or on the last day (``forced``) if none does.
29 February is never a sowing day: AquaCrop-OSPy cannot plant on it.
Used for historical runs (no forecast). Rules and their sources:
``app.services.sowing_rules``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Literal

from app.services.sowing_rules import SowingRule


class WindowUnavailable(ValueError):
    """The window cannot be computed (missing weather days); the message says why."""


@dataclass(frozen=True)
class SowingDecision:
    sowing_date: date
    how: Literal["triggered", "forced"]
    window_start: date
    window_end: date
    tempero_checked: bool
    reasons: list[str] = field(default_factory=list)


def _tmean(w) -> float:
    return (w.tmin_c + w.tmax_c) / 2.0


def _running_mean(days: dict, d: date, n: int) -> float | None:
    vals = []
    for k in range(n):
        w = days.get(d - timedelta(days=k))
        if w is None:
            return None
        vals.append(_tmean(w))
    return sum(vals) / n


def window_for_season(days: dict, harvest_year: int, rule: SowingRule) -> tuple[date, date]:
    """(start, end) of the sowing window of the season harvested in ``harvest_year``.

    ``days`` maps date -> DailyWeather. Autumn crops are sown in the year
    before harvest. A missing day in the search or the window raises.
    """
    year = harvest_year - 1 if rule.season == "autumn" else harvest_year
    search = date(year, *rule.search_from)
    cap = date(year, *rule.latest_start) if rule.latest_start else None
    last_search = cap or date(year, 12, 31)
    start = None
    d = search
    while d <= last_search:
        m = _running_mean(days, d, rule.running_mean_days)
        if m is None:
            raise WindowUnavailable(f"weather gap around {d.isoformat()} in the window search")
        if (rule.crossing == "falls" and m <= rule.threshold_c) or (
                rule.crossing == "rises" and m >= rule.threshold_c):
            start = d
            break
        d += timedelta(days=1)
    if start is None:
        if cap is None:
            raise WindowUnavailable(
                f"10-day mean never {rule.crossing} to {rule.threshold_c} degC in {year}")
        start = cap
    end = start + timedelta(days=rule.window_days - 1)
    for k in range(rule.window_days):
        if start + timedelta(days=k) not in days:
            raise WindowUnavailable(f"weather gap on {(start + timedelta(days=k)).isoformat()}")
    return start, end


def _rain_ok(days: dict, d: date, rule: SowingRule) -> bool:
    if rule.rain_mm is None:
        return True
    total = 0.0
    for k in range(rule.rain_days):
        w = days.get(d - timedelta(days=k))
        if w is None:
            return False
        total += w.precip_mm
    return total >= rule.rain_mm


def _is_29_feb(d: date) -> bool:
    return (d.month, d.day) == (2, 29)


def _tempero_ok(theta: float, limits: tuple[float, float], rule: SowingRule) -> bool:
    wet, dry = limits
    if rule.tempero == "none":
        return True
    if theta > wet:
        return False
    return rule.tempero == "not_wet" or theta >= dry


def decide_sowing(
    days: dict,
    start: date,
    end: date,
    rule: SowingRule,
    topsoil: dict[date, float] | None = None,
    limits: tuple[float, float] | None = None,
) -> SowingDecision:
    """First window day meeting the rain trigger and tempero, else the last day.

    ``topsoil`` maps date -> volumetric water content of the top 10 cm;
    ``limits`` is (wet, dry) from the soil module. Without either, tempero is
    not checked and the decision says so.
    """
    check_tempero = rule.tempero != "none" and bool(topsoil) and limits is not None
    reasons = []
    if rule.tempero != "none" and not check_tempero:
        reasons.append("tempero not checked: no topsoil water or no soil limits")
    d = start
    while d <= end:
        if _is_29_feb(d):
            reasons.append("29 February skipped: the crop model cannot plant on it")
            d += timedelta(days=1)
            continue
        rain = _rain_ok(days, d, rule)
        theta = topsoil.get(d) if check_tempero else None
        soil_ok = (not check_tempero) or (theta is not None and _tempero_ok(theta, limits, rule))
        if rain and soil_ok:
            return SowingDecision(d, "triggered", start, end, check_tempero, reasons)
        d += timedelta(days=1)
    last = end - timedelta(days=1) if _is_29_feb(end) else end
    reasons.append("no window day met the trigger: sown on the last day")
    return SowingDecision(last, "forced", start, end, check_tempero, reasons)
