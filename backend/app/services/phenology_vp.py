"""Vernalization-photoperiod phenology engine for cereals (APSIM-Wheat style), pure computation.

Equations: APSIM-Wheat 7.5 documentation (Zheng et al. 2015), numbering as in the PDF.
Daylength: the general formula of APSIM day_length (science.f90, APSIM710) with the declination
of eq. 29. Daily vernalisation is bounded >= 0 (CWVernalPhase.cpp: l_bound(vd, 0)).

Series index 0 is the sowing day; germination (start of vernalisation) is the next day.
Milestones, from a series of daily weather:

- emergence: first day the cumulative GDD (sowing day included) reaches the emergence threshold;
- floral initiation (FI): first day ``d`` with the sum over ``emergence+1..d`` of
  ``GDD * min(f_D, f_V)`` >= ``t1_adj``;
- heading (``flowering_start``): first day after FI with plain GDD summed over ``FI+1..h`` >= ``t2``;
- maturity: first day after heading with plain GDD summed over ``h+1..m`` >= ``mat_p``.

GDD is the engine's own method (``crop_calendar.daily_gdd_m3``); nothing is copied. No I/O.
"""
from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import numpy as np

from app.services import crop_calendar as cc
from app.services.engines import aquacrop_engine as aq

HSNOW = 0.0  # snow depth (cm); the released APSIM default
HORIZON_DAYS = 330  # days simulated from sowing
MILESTONES = ("emergence", "flowering_start", "maturity")


@dataclass(frozen=True)
class VPParams:
    """Fitted parameters: photoperiod sensitivity ``rp``, vernalisation sensitivity ``rv``,
    heading thermal time after FI ``t2``, maturity thermal time after heading ``mat_p``,
    and the adjusted thermal time to FI ``t1_adj``. ``mat_p`` is None when no maturity
    parameter was fitted for the group: maturity is then never predicted."""
    rp: float
    rv: float
    t2: float
    mat_p: float | None
    t1_adj: float = 401.0


# ---------------------------------------------------------------- equations
def crown_temps(tmin, tmax):
    """Eq. 1-3 with Hsnow = 0. Returns (Tc_min, Tc_max, Tc)."""
    tmin = np.asarray(tmin, dtype=float)
    tmax = np.asarray(tmax, dtype=float)
    k = 0.4 + 0.0018 * (HSNOW - 15.0) ** 2
    tcmax = np.where(tmax < 0.0, 2.0 + tmax * k, tmax)
    tcmin = np.where(tmin < 0.0, 2.0 + tmin * k, tmin)
    return tcmin, tcmax, (tcmax + tcmin) / 2.0


def delta_v(tmin, tmax):
    """Eq. 9, bounded >= 0. Zero unless Tmax < 30 and Tmin < 15."""
    tmin = np.asarray(tmin, dtype=float)
    tmax = np.asarray(tmax, dtype=float)
    _, _, tc = crown_temps(tmin, tmax)
    v1 = 1.4 - 0.0778 * tc
    v2 = 0.5 + 13.44 * tc / (tmax - tmin + 3.0) ** 2
    dv = np.maximum(np.minimum(v1, v2), 0.0)
    return np.where((tmax < 30.0) & (tmin < 15.0), dv, 0.0)


def cumulative_v(tmin, tmax, start_idx):
    """Cumulative V (eq. 11) along the last axis; V is 0 before ``start_idx`` and from it on
    includes that day's dV - dVd (value at end of day). Devernalisation (eq. 10) uses the V of the
    previous day: dVd = min(0.5*(Tmax-30), V) when Tmax > 30 and V < 10."""
    tmin = np.asarray(tmin, dtype=float)
    tmax = np.asarray(tmax, dtype=float)
    n = tmin.shape[-1]
    dv = delta_v(tmin, tmax)
    start = np.broadcast_to(np.asarray(start_idx), tmin.shape[:-1])
    v = np.zeros(tmin.shape[:-1])
    out = np.zeros(tmin.shape)
    for t in range(n):
        active = start <= t
        dvd = np.where((tmax[..., t] > 30.0) & (v < 10.0),
                       np.minimum(0.5 * (tmax[..., t] - 30.0), v), 0.0)
        v = np.where(active, v + dv[..., t] - dvd, v)
        out[..., t] = v
    return out


def f_v(V, rv):
    """Eq. 12, bounded to [0, 1]."""
    V = np.asarray(V, dtype=float)
    return np.clip(1.0 - (0.0054545 * rv + 0.0003) * (50.0 - V), 0.0, 1.0)


def declination_rad(doy):
    """Eq. 29: 23.45 * sin(2*pi/365.25 * (N - 82.25)) degrees, in radians."""
    return np.radians(23.45 * np.sin(2.0 * np.pi / 365.25 * (np.asarray(doy, dtype=float) - 82.25)))


def daylength_from_dec(lat_deg: float, dec_rad, twilight_deg: float = -6.0):
    """Hours between the sun being at ``twilight_deg`` altitude (am and pm), per APSIM day_length."""
    dec = np.asarray(dec_rad, dtype=float)
    if abs(lat_deg) == 90.0:
        coshra = np.sign(-dec) * np.sign(lat_deg) * np.ones_like(dec)
    else:
        lat = np.radians(lat_deg)
        slsd = np.sin(lat) * np.sin(dec)
        clcd = np.cos(lat) * np.cos(dec)
        altmn = np.arcsin(np.clip(slsd - clcd, -1.0, 1.0))
        altmx = np.arcsin(np.clip(slsd + clcd, -1.0, 1.0))
        alt = np.clip(np.radians(twilight_deg), altmn, altmx)
        coshra = np.clip((np.sin(alt) - slsd) / clcd, -1.0, 1.0)
    return np.arccos(coshra) * 24.0 / np.pi


def daylength_h(lat_deg: float, doy, twilight_deg: float = -6.0):
    """Daylength (h) with the -6 degree twilight of the vernalisation-photoperiod model."""
    return daylength_from_dec(lat_deg, declination_rad(doy), twilight_deg)


def f_d(daylength_h, rp):
    """Eq. 8, bounded to [0, 1]."""
    lp = np.asarray(daylength_h, dtype=float)
    return np.clip(1.0 - 0.002 * rp * (20.0 - lp) ** 2, 0.0, 1.0)


# ---------------------------------------------------------------- milestones
def _check_finite(w: Any) -> None:
    """A non-finite temperature would poison the running sums: refuse, naming the day."""
    for name in ("tmin_c", "tmax_c"):
        v = getattr(w, name)
        if not isinstance(v, (int, float)) or not math.isfinite(v):
            raise ValueError(f"non-finite {name} on {w.day.isoformat()}: {v!r}")


def _first_at_least(cum: np.ndarray, target: float, after: int) -> int | None:
    """First index > ``after`` with ``cum >= target``, else None."""
    hit = np.nonzero(cum[after + 1:] >= target)[0]
    return int(hit[0]) + after + 1 if hit.size else None


def _milestone_indices(days: Sequence[Any], lat: float, tbase: float, tupp: float,
                       emergence_gdd: float, p: VPParams) -> dict[str, int | None]:
    days = days[:HORIZON_DAYS]
    n = len(days)
    out: dict[str, int | None] = {k: None for k in MILESTONES}
    if n == 0:
        return out
    for w in days:
        _check_finite(w)
    tmin = np.array([w.tmin_c for w in days], dtype=float)
    tmax = np.array([w.tmax_c for w in days], dtype=float)
    gdd = np.array([cc.daily_gdd_m3(w.tmin_c, w.tmax_c, tbase, tupp) for w in days], dtype=float)
    cum = np.cumsum(gdd)
    em = _first_at_least(cum, emergence_gdd, -1)
    out["emergence"] = em
    if em is None:
        return out
    doy = np.array([w.day.timetuple().tm_yday for w in days])
    v = cumulative_v(tmin, tmax, 1)
    factor = np.minimum(f_d(daylength_h(lat, doy), p.rp), f_v(v, p.rv))
    adj = np.where(np.arange(n) > em, gdd * factor, 0.0)
    fi = _first_at_least(np.cumsum(adj), p.t1_adj, -1)
    if fi is None:
        return out
    head = _first_at_least(cum, cum[fi] + p.t2, fi)
    out["flowering_start"] = head
    if head is None:
        return out
    if p.mat_p is None:
        return out
    out["maturity"] = _first_at_least(cum, cum[head] + p.mat_p, head)
    return out


def vp_milestones(sowing_date: date, days: Sequence[Any], lat: float, tbase: float, tupp: float,
                  emergence_gdd: float, p: VPParams) -> dict[str, date | None]:
    """Emergence, heading and maturity dates over a series that starts on the sowing day
    (``None`` when not reached within the horizon)."""
    idx = _milestone_indices(days, lat, tbase, tupp, emergence_gdd, p)
    return {k: (days[i].day if i is not None else None) for k, i in idx.items()}


def _entry(name: str, emergence_gdd: float, body: dict) -> dict:
    return {"gdd": emergence_gdd, **body} if name == "emergence" else body


def vp_block_milestones(sowing_date: date, observed_days: Sequence[Any],
                        analog_members: Sequence[Sequence[Any]], lat: float, tbase: float, tupp: float,
                        emergence_gdd: float, p: VPParams, as_of: date) -> dict[str, dict]:
    """Same shape and rules as ``crop_calendar.milestone_dates``.

    ``reached`` only from the observed days (date <= ``as_of``). Otherwise ``projected``: P10/P50/P90
    over the full series of each analog member (observed days followed by that member's days after the
    last observed one, so V and the sums carry over without a seam), or ``unavailable``.
    """
    observed = [w for w in observed_days if w.day >= sowing_date]
    last_observed = observed_days[-1].day if observed_days else None
    args = (lat, tbase, tupp, emergence_gdd, p)
    reached = vp_milestones(sowing_date, observed, *args)
    out: dict[str, dict] = {}
    member_dates: list[dict[str, date | None]] | None = None
    for name in MILESTONES:
        if name == "maturity" and p.mat_p is None:
            out[name] = {"status": "unavailable", "reason": "no_maturity_parameter"}
            continue
        got = reached[name]
        if got is not None and got <= as_of:
            out[name] = _entry(name, emergence_gdd, {"status": "reached", "date": got.isoformat()})
            continue
        if not analog_members:
            out[name] = _entry(name, emergence_gdd, {"status": "unavailable", "reason": "no_analog_members"})
            continue
        if len(analog_members) < cc.MIN_MEMBERS:
            out[name] = _entry(name, emergence_gdd, {"status": "unavailable", "reason": "too_few_members"})
            continue
        if member_dates is None:
            member_dates = []
            for member in analog_members:
                tail = [w for w in member
                        if w.day >= sowing_date and (last_observed is None or w.day > last_observed)]
                member_dates.append(vp_milestones(sowing_date, observed + tail, *args))
        dates = [m[name] for m in member_dates if m[name] is not None]
        if not dates:
            body = {"status": "unavailable", "reason": "not_reached"}
        elif len(dates) < len(analog_members):
            body = {"status": "unavailable", "reason": "not_reached_by_all_members"}
        else:
            pct = aq._pct([float(d.toordinal()) for d in dates])
            body = {"status": "projected", "n_members": len(dates),
                    **{f"date_{k}": date.fromordinal(round(v)).isoformat() for k, v in pct.items()}}
        out[name] = _entry(name, emergence_gdd, body)
    return out
