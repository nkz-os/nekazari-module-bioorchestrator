"""Vernalization-photoperiod engine (APSIM-Wheat style): equations, milestones, block format."""
import json
import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from app.services import crop_calendar as cc
from app.services import phenology_vp as vp
from app.services.engines import aquacrop_engine as aq

SOWING = date(2026, 11, 1)
FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "phenology_vp_equivalence.json"
PARAMS = vp.VPParams(rp=3.0, rv=1.5, t2=300.0, mat_p=600.0)
WHEAT = {"lat": 42.0, "tbase": 0.0, "tupp": 26.0, "emergence_gdd": 150.0}


def _w(day, tmin, tmax):
    return aq.DailyWeather(day, tmin, tmax, 0.0, 2.0)


def _series(start, n, tmin, tmax):
    return [_w(start + timedelta(days=i), tmin, tmax) for i in range(n)]


def _seasonal(start, n):
    """Synthetic year: cold winter, warm spring/summer (deterministic)."""
    out = []
    for i in range(n):
        d = start + timedelta(days=i)
        mean = 11.0 - 9.0 * math.cos(2 * math.pi * (d.timetuple().tm_yday + 10) / 365.25)
        out.append(_w(d, mean - 5.0, mean + 5.0))
    return out


# ------------------------------------------------------------ equations (ported)
def test_fd_values():
    assert vp.f_d(np.array([20.0]), 3.0)[0] == 1.0
    assert abs(vp.f_d(np.array([10.0]), 3.0)[0] - 0.4) < 1e-12
    assert vp.f_d(np.array([0.0]), 5.0)[0] == 0.0


def test_fv_values():
    assert vp.f_v(np.array([50.0]), 1.5)[0] == 1.0
    assert abs(vp.f_v(np.array([0.0]), 1.5)[0] - (1 - (0.0054545 * 1.5 + 0.0003) * 50)) < 1e-12
    assert vp.f_v(np.array([60.0]), 1.5)[0] == 1.0


def test_crown_temp_below_zero():
    tcmin, tcmax, tc = vp.crown_temps(np.array([-12.0]), np.array([-10.0]))
    assert abs(tcmax[0] - (2 - 10 * 0.805)) < 1e-9
    assert abs(tcmin[0] - (2 - 12 * 0.805)) < 1e-9
    assert abs(tc[0] - (tcmax[0] + tcmin[0]) / 2) < 1e-12
    _, tcmax, _ = vp.crown_temps(np.array([1.0]), np.array([5.0]))
    assert tcmax[0] == 5.0


def test_delta_v_zero_when_hot():
    assert vp.delta_v(np.array([16.0]), np.array([25.0]))[0] == 0.0
    assert vp.delta_v(np.array([5.0]), np.array([31.0]))[0] == 0.0


def test_delta_v_nonnegative_and_value():
    assert vp.delta_v(np.array([-12.0]), np.array([-10.0]))[0] == 0.0
    assert abs(vp.delta_v(np.array([0.0]), np.array([10.0]))[0]
               - min(1.4 - 0.0778 * 5, 0.5 + 13.44 * 5 / 13 ** 2)) < 1e-12


def test_devernalisation_only_below_10():
    tn = np.array([[0.0, 0.0, 20.0]])
    tx = np.array([[10.0, 10.0, 34.0]])
    v = vp.cumulative_v(tn, tx, np.array([0]))[0]
    dv = vp.delta_v(tn[0, :2], tx[0, :2])
    assert abs(v[1] - dv.sum()) < 1e-12
    assert abs(v[2] - (v[1] - min(0.5 * 4, v[1]))) < 1e-12
    n = 30
    tn = np.zeros((1, n + 1))
    tx = np.full((1, n + 1), 10.0)
    tx[0, -1] = 34.0
    tn[0, -1] = 20.0
    v = vp.cumulative_v(tn, tx, np.array([0]))[0]
    assert v[-2] >= 10.0 and v[-1] == v[-2]
    tn = np.array([[0.0] * 5 + [20.0]])
    tx = np.array([[10.0] * 5 + [34.0]])
    v = vp.cumulative_v(tn, tx, np.array([0]))[0]
    assert 2.0 < v[4] < 10.0
    assert abs(v[5] - (v[4] - 2.0)) < 1e-12


def test_cumulative_v_start_idx():
    tn = np.zeros((2, 5))
    tx = np.full((2, 5), 10.0)
    v = vp.cumulative_v(tn, tx, np.array([1, 3]))
    d = vp.delta_v(np.array([0.0]), np.array([10.0]))[0]
    assert v[0, 0] == 0.0 and abs(v[0, 4] - 4 * d) < 1e-12
    assert v[1, 2] == 0.0 and abs(v[1, 4] - 2 * d) < 1e-12


def test_daylength_equator_equinox():
    d = vp.daylength_from_dec(0.0, np.array([0.0]), -6.0)[0]
    assert abs(d - 2 * math.degrees(math.acos(-math.sin(math.radians(6)))) / 15) < 1e-9
    assert abs(d - 12.80) < 0.01


def test_daylength_clamped_no_nan():
    for lat in (70.0, -70.0, 90.0, -90.0):
        d = vp.daylength_h(lat, np.arange(1, 367))
        assert np.isfinite(d).all() and d.min() >= 0 and d.max() <= 24


def _fortran_day_length(doy, lat, sun_angle):
    """Transcription of APSIM day_length (science.f90) with its own declination."""
    pi = 3.14159265359
    dg = 2 * pi / 360
    dec = 23.45116 * dg * math.sin(2 * pi / 365.25 * (doy - 79.25))
    slsd = math.sin(lat * dg) * math.sin(dec)
    clcd = math.cos(lat * dg) * math.cos(dec)

    def clamp(x, lo, hi):
        return max(lo, min(hi, x))
    altmn = math.asin(clamp(slsd - clcd, -1, 1))
    altmx = math.asin(clamp(slsd + clcd, -1, 1))
    alt = clamp(sun_angle * dg, altmn, altmx)
    coshra = clamp((math.sin(alt) - slsd) / clcd, -1, 1)
    return math.acos(coshra) * 24 / (2 * pi) * 2


def test_daylength_matches_source_case():
    for lat in (0.0, 30.0, 42.0, 52.0, 60.0):
        for doy in (1, 80, 172, 266, 355):
            for tw in (-6.0, 0.0):
                assert abs(vp.daylength_h(lat, np.array([doy]), tw)[0]
                           - _fortran_day_length(doy, lat, tw)) < 0.3
    doy, lat = 172, 52.0
    dec = math.radians(23.45116) * math.sin(2 * math.pi / 365.25 * (doy - 79.25))
    assert abs(vp.daylength_from_dec(lat, np.array([dec]), -6.0)[0]
               - _fortran_day_length(doy, lat, -6.0)) < 1e-4


# ------------------------------------------------------------ milestones
def test_vp_milestones_ordered_on_a_full_year():
    days = _seasonal(SOWING, 330)
    m = vp.vp_milestones(SOWING, days, p=PARAMS, **WHEAT)
    assert set(m) == {"emergence", "flowering_start", "maturity"}
    assert all(m[k] is not None for k in m)
    assert SOWING < m["emergence"] < m["flowering_start"] < m["maturity"]


def test_vp_milestones_none_when_not_reached():
    m = vp.vp_milestones(SOWING, _series(SOWING, 10, 0.0, 5.0), p=PARAMS, **WHEAT)
    assert m == {"emergence": None, "flowering_start": None, "maturity": None}


def test_vp_milestones_horizon_is_330_days():
    # Constant mild weather: never reaches maturity within the horizon with a huge threshold.
    big = vp.VPParams(rp=3.0, rv=1.5, t2=300.0, mat_p=1e6)
    m = vp.vp_milestones(SOWING, _seasonal(SOWING, 500), p=big, **WHEAT)
    assert m["maturity"] is None and m["flowering_start"] is not None


def test_vernalisation_delays_floral_initiation():
    # Warm winter (no vernalisation) is later than a cold one, same GDD.
    cold = [_w(SOWING + timedelta(days=i), 1.0, 9.0) for i in range(330)]
    warm = [_w(SOWING + timedelta(days=i), 9.0, 17.0) for i in range(330)]
    # same GDD needs a comparable thermal sum: compare vernalised fraction instead
    v_cold = vp.cumulative_v([d.tmin_c for d in cold], [d.tmax_c for d in cold], 1)
    v_warm = vp.cumulative_v([d.tmin_c for d in warm], [d.tmax_c for d in warm], 1)
    assert v_cold[-1] > v_warm[-1]


# ------------------------------------------------------------ block format
def _members(first_day, n_members, n_days, tmin=6.0, tmax=16.0):
    return [_series(first_day, n_days, tmin + 0.2 * k, tmax + 0.2 * k) for k in range(n_members)]


def test_projection_recent_sowing():
    observed = _series(SOWING, 10, 0.0, 6.0)
    last = observed[-1].day
    members = [_seasonal(SOWING, 330)[10:] for _ in range(cc.MIN_MEMBERS)]
    assert members[0][0].day == last + timedelta(days=1)
    block = vp.vp_block_milestones(SOWING, observed, members, p=PARAMS, as_of=last, **WHEAT)
    assert set(block) == {"emergence", "flowering_start", "maturity"}
    assert block["emergence"]["gdd"] == 150.0
    assert "gdd" not in block["flowering_start"] and "gdd" not in block["maturity"]
    for k in block:
        assert block[k]["status"] == "projected", k
        assert block[k]["n_members"] == len(members)
        assert block[k]["date_p10"] <= block[k]["date_p50"] <= block[k]["date_p90"]
    assert "senescence_start" not in block


def test_reached_uses_observed_only():
    full = _seasonal(SOWING, 330)
    ref = vp.vp_milestones(SOWING, full, p=PARAMS, **WHEAT)
    assert ref["flowering_start"] is not None
    # Observed up to heading (inclusive): emergence and heading reached, maturity projected.
    n_obs = (ref["flowering_start"] - SOWING).days + 1
    observed = full[:n_obs]
    members = [full[n_obs:] for _ in range(cc.MIN_MEMBERS)]
    block = vp.vp_block_milestones(SOWING, observed, members, p=PARAMS, as_of=observed[-1].day, **WHEAT)
    assert block["emergence"] == {"gdd": 150.0, "status": "reached", "date": ref["emergence"].isoformat()}
    assert block["flowering_start"] == {"status": "reached", "date": ref["flowering_start"].isoformat()}
    assert block["maturity"]["status"] == "projected"
    assert block["maturity"]["date_p50"] == ref["maturity"].isoformat()
    # A date reached by the weather but after as_of is not "reached".
    early = vp.vp_block_milestones(SOWING, observed, members, p=PARAMS,
                                   as_of=ref["emergence"] - timedelta(days=1), **WHEAT)
    assert early["flowering_start"]["status"] != "reached"


def test_unavailable_reasons():
    observed = _series(SOWING, 5, 0.0, 6.0)
    last = observed[-1].day
    kw = {"p": PARAMS, "as_of": last, **WHEAT}
    assert vp.vp_block_milestones(SOWING, observed, [], **kw)["maturity"] == {
        "status": "unavailable", "reason": "no_analog_members"}
    few = _members(last + timedelta(days=1), cc.MIN_MEMBERS - 1, 300)
    assert vp.vp_block_milestones(SOWING, observed, few, **kw)["maturity"]["reason"] == "too_few_members"
    cold = _members(last + timedelta(days=1), cc.MIN_MEMBERS, 20, 0.0, 3.0)
    assert vp.vp_block_milestones(SOWING, observed, cold, **kw)["emergence"]["reason"] == "not_reached"
    mixed = _members(last + timedelta(days=1), cc.MIN_MEMBERS - 1, 300) + [cold[0]]
    assert vp.vp_block_milestones(SOWING, observed, mixed, **kw)["emergence"]["reason"] == "not_reached_by_all_members"


def test_non_finite_temperature_refused():
    observed = _series(SOWING, 5, 0.0, 6.0) + [_w(SOWING + timedelta(days=5), float("nan"), 6.0)]
    with pytest.raises(ValueError, match="non-finite tmin_c"):
        vp.vp_block_milestones(SOWING, observed, [], p=PARAMS, as_of=observed[-1].day, **WHEAT)


# ------------------------------------------------------------ equivalence with the experiment
def test_equivalence_fixture():
    if not FIXTURE.exists():
        pytest.skip(f"equivalence fixture not present yet: {FIXTURE.name}")
    data = json.loads(FIXTURE.read_text())
    cases = data["cases"] if isinstance(data, dict) else data
    assert len(cases) == 20
    for c in cases:
        days = [_w(date.fromisoformat(d), tn, tx) for d, tn, tx in zip(c["days"], c["tmin"], c["tmax"])]
        p = vp.VPParams(rp=c["rp"], rv=c["rv"], t2=c["t2"], mat_p=c["mat_p"])
        m = vp.vp_milestones(date.fromisoformat(c["sowing"]), days, c["lat"], c["tbase"], c["tupp"],
                             c["emergence"], p)
        assert m["flowering_start"] == date.fromisoformat(c["heading"]), c.get("case_id")
        assert m["maturity"] == date.fromisoformat(c["maturity"]), c.get("case_id")
