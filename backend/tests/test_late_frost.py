from datetime import date, timedelta

import pytest

from app.planning import late_frost as lf
from app.species_registry import list_species
from app.services.crop_calendar import CropCalendar


def _cal():
    return CropCalendar(
        crop_slug="wheat", aquacrop_crop="Wheat", cultivar="Wheat", tbase=0.0, tupp=40.0, emergence=0.0,
        hi_start=100.0, flowering=50.0, senescence=200.0, maturity=300.0, model_version="t",
        calibration={}, sources=())


def _days(start, n, tmin=5.0, tmax=15.0):
    return {start + timedelta(days=i): (tmin, tmax) for i in range(n)}


def test_sowing_date_clamps_doy_366_in_common_year():
    assert lf.sowing_date(2023, 366) == date(2023, 12, 31)
    assert lf.sowing_date(2024, 366) == date(2024, 12, 31)
    assert lf.sowing_date(2023, 305) == date(2023, 11, 1)


def test_stage_windows_flowering():
    w = lf.stage_windows(_cal(), 1, _days(date(2000, 1, 1), 60), "flowering")
    assert w == {2000: (date(2000, 1, 10), date(2000, 1, 15))}


def test_stage_windows_grain_filling():
    # flowering ends day 15; grain filling starts day 16, ends when 300 GDD (day 30)
    w = lf.stage_windows(_cal(), 1, _days(date(2000, 1, 1), 60), "grain_filling")
    assert w == {2000: (date(2000, 1, 16), date(2000, 1, 30))}


def test_autumn_sowing_window_in_next_year():
    days = _days(date(2000, 12, 25), 80)
    w = lf.stage_windows(_cal(), 360, days, "flowering")
    assert list(w) == [2000]
    assert w[2000][0].year == 2001 and w[2000][0].month == 1


def test_season_with_gap_is_excluded():
    days = _days(date(2000, 1, 1), 60)
    days[date(2000, 1, 5)] = (None, 15.0)
    assert lf.stage_windows(_cal(), 1, days, "flowering") == {}


def test_missing_day_is_excluded():
    days = _days(date(2000, 1, 1), 60)
    del days[date(2000, 1, 5)]
    assert lf.stage_windows(_cal(), 1, days, "flowering") == {}


def test_unfinished_season_is_excluded():
    assert lf.stage_windows(_cal(), 1, _days(date(2000, 1, 1), 12), "flowering") == {}


def test_frequency_counts_hits_inclusive():
    wins = {2000: (date(2000, 1, 10), date(2000, 1, 15)), 2001: (date(2001, 1, 10), date(2001, 1, 15))}
    tmin = {date(2000, 1, 15): -0.9, date(2001, 1, 15): -1.0}
    assert lf.late_frost_frequency(tmin, wins, -1.0) == lf.FrostFrequency(1, 2, (2001,))


def _hist(years_hit_flower, years_hit_grain, n=10, caveat="warm_bias"):
    """n sowing years from 2000, each a full season; hits placed in flowering/grain windows."""
    days = {}
    for y in range(2000, 2000 + n):
        days.update(_days(date(y, 1, 1), 365 if y % 4 else 366))
    for i in range(years_hit_flower):
        days[date(2000 + i, 1, 12)] = (-1.5, 21.5)
    for i in range(years_hit_grain):
        days[date(2000 + i, 1, 20)] = (-2.5, 22.5)
    return lf.FrostHistory(days=days, tmin_caveat=caveat)


def test_reason_info_when_no_year_hit():
    r = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=_hist(0, 0), status="ready")
    assert r["code"] == "frost_late_info"
    h = _hist(0, 0)
    d = dict(h.days)
    for y in (2000, 2002):
        d[date(y, 1, 12)] = (-1.5, 21.5)
    r = lf.late_frost_reason("barley", cal=_cal(), sowing_doy=1,
                             history=lf.FrostHistory(d, "warm_bias"), status="ready")
    assert r["code"] == "frost_late_risk"  # 2 of 10
    assert r["gate"] == "frost"
    assert r["params"] == {"years_hit": 2, "years_total": 10, "stage": "flowering", "threshold_c": -1.0,
                           "threshold_source": "fao_2005_t45", "tmin_source": "ERA5",
                           "tmin_caveat": "warm_bias"}


def test_reason_risk_two_of_ten():
    d = dict(_hist(0, 0).days)
    for y in (2000, 2002):
        d[date(y, 1, 12)] = (-1.5, 21.5)
    r = lf.late_frost_reason("barley", cal=_cal(), sowing_doy=1,
                             history=lf.FrostHistory(d, "warm_bias"), status="ready")
    assert r["code"] == "frost_late_risk"


def test_reason_exactly_one_in_five_and_one_in_six():
    d5 = dict(_hist(0, 0, n=5).days)
    d5[date(2000, 1, 12)] = (-1.5, 21.5)
    r = lf.late_frost_reason("barley", cal=_cal(), sowing_doy=1,
                             history=lf.FrostHistory(d5, "warm_bias"), status="ready")
    assert (r["code"], r["params"]["years_hit"], r["params"]["years_total"]) == ("frost_late_risk", 1, 5)
    d6 = dict(_hist(0, 0, n=6).days)
    d6[date(2000, 1, 12)] = (-1.5, 21.5)
    r = lf.late_frost_reason("barley", cal=_cal(), sowing_doy=1,
                             history=lf.FrostHistory(d6, "warm_bias"), status="ready")
    assert (r["code"], r["params"]["years_total"]) == ("frost_late_info", 6)


def test_reason_picks_worst_stage_tie_earliest():
    r = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=_hist(1, 3), status="ready")
    assert r["params"]["stage"] == "grain_filling" and r["params"]["threshold_c"] == -2.0
    assert r["params"]["threshold_source"] == "kstate_c646"
    r = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=_hist(2, 2), status="ready")
    assert r["params"]["stage"] == "flowering"


def test_reason_unknown_whys():
    h = _hist(0, 0)

    def why(slug, **kw):
        args = dict(cal=_cal(), sowing_doy=1, history=h, status="ready")
        args.update(kw)
        r = lf.late_frost_reason(slug, **args)
        assert r["code"] == "frost_late_unknown" and r["gate"] == "frost"
        return r["params"]["why"]

    assert why("durum_wheat") == "no_threshold"
    assert why("oat") == "no_stage_model"
    assert why("wheat", status="loading") == "history_pending"
    assert why("wheat", status="absent") == "history_pending"
    assert why("wheat", status="failed") == "history_unavailable"
    assert why("wheat", history=None) == "history_unavailable"
    assert why("wheat", history=lf.FrostHistory({}, "warm_bias")) == "history_unavailable"
    assert why("wheat", sowing_doy=None) == "no_stage_model"
    assert why("wheat", cal=None) == "no_stage_model"


def test_fao_only_slugs_exist():
    known = set(list_species())
    assert set(lf.FAO_ONLY_THRESHOLDS) <= known
    assert set(lf.STAGE_THRESHOLDS) <= known


def test_hit_on_first_day_of_window_is_inclusive():
    days = _days(date(2000, 1, 1), 60)
    days[date(2000, 1, 10)] = (-1.0, 21.0)  # 10 GDD, first window day
    w = lf.stage_windows(_cal(), 1, days, "flowering")
    tmin = {d: t[0] for d, t in days.items()}
    assert lf.late_frost_frequency(tmin, w, -1.0).years_hit == 1


def test_season_longer_than_365_days_is_excluded():
    # 100/366 GDD/day: 100 GDD is first reached on day 366, one past the season limit
    days = _days(date(2000, 1, 1), 400, tmin=0.0, tmax=2 * 100 / 366)
    assert lf.stage_windows(_cal(), 1, days, "flowering") == {}


def test_non_finite_values_exclude_season():
    days = _days(date(2000, 1, 1), 60)
    days[date(2000, 1, 5)] = (float("nan"), 15.0)
    assert lf.stage_windows(_cal(), 1, days, "flowering") == {}
