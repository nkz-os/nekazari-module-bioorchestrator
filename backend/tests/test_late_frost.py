from datetime import date, timedelta

from app.planning import late_frost as lf
from app.services.crop_calendar import CropCalendar
from app.species_registry import list_species


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


def _hist_disjoint(n):
    """n full seasons from 2000: flowering hit in 2000-2001, grain filling in 2002-2003."""
    days = {}
    for y in range(2000, 2000 + n):
        days.update(_days(date(y, 1, 1), 365 if y % 4 else 366))
    for y in (2000, 2001):
        days[date(y, 1, 12)] = (-1.5, 21.5)
    for y in (2002, 2003):
        days[date(y, 1, 20)] = (-2.5, 22.5)
    return lf.FrostHistory(days=days, tmin_caveat="warm_bias")


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
    assert r["params"] == {"years_hit": 2, "years_total": 10, "stage": "flowering", "thresholds_c": "-1",
                           "threshold_source": "fao_2005_t45", "tmin_source": "ERA5",
                           "tmin_caveat": "warm_bias", "tmin_basis": "raw"}


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


def test_reason_counts_years_hit_in_any_stage():
    # flowering hit in 2000-2001, grain filling in 2002-2003: disjoint, 4 of 20 seasons
    r = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=_hist_disjoint(20), status="ready")
    assert r["code"] == "frost_late_risk"
    p = r["params"]
    assert (p["years_hit"], p["years_total"]) == (4, 20)
    assert p["stage"] == "flowering+grain_filling" and p["thresholds_c"] == "-1/-2"
    assert p["threshold_source"] == "kstate_c646"


def test_reason_a_year_hit_in_both_stages_counts_once():
    r = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=_hist(2, 2), status="ready")
    assert (r["params"]["years_hit"], r["params"]["years_total"]) == (2, 10)


def test_reason_below_share_is_info_across_stages():
    r = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=_hist_disjoint(21), status="ready")
    assert (r["code"], r["params"]["years_hit"], r["params"]["years_total"]) == ("frost_late_info", 4, 21)


def test_reason_season_incomplete_for_grain_filling_is_excluded():
    # the last season stops after flowering ends (day 15) but before grain filling ends (day 30)
    days = dict(_hist_disjoint(5).days)
    for d in [d for d in days if d > date(2004, 1, 20)]:
        del days[d]
    r = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1,
                             history=lf.FrostHistory(days, "warm_bias"), status="ready")
    assert r["params"]["years_total"] == 4  # 2004 is complete for flowering only
    b = lf.late_frost_reason("barley", cal=_cal(), sowing_doy=1,
                             history=lf.FrostHistory(days, "warm_bias"), status="ready")
    assert b["params"]["years_total"] == 5  # barley evaluates flowering only


def test_reason_unknown_whys():
    h = _hist(0, 0)

    def why(slug, **kw):
        args = {"cal": _cal(), "sowing_doy": 1, "history": h, "status": "ready"}
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


def test_window_mid_doy_is_the_window_middle():
    assert lf.window_mid_doy({"start_month": 10, "end_month": 1}) == 335  # 1 Dec: Oct 1 .. Jan 31
    assert lf.window_mid_doy({"start_month": 10, "end_month": 12}) == 319  # 15 Nov: Oct 1 .. Dec 31
    assert lf.window_mid_doy({"start_month": 3, "end_month": 4}) == 90  # 31 Mar: Mar 1 .. Apr 30
    assert lf.window_mid_doy({"start_month": 11, "end_month": 11}) == 319  # 15 Nov


def test_window_mid_doy_without_a_usable_window():
    for window in (None, {}, {"start_month": 10}, {"start_month": 13, "end_month": 1},
                   {"start_month": "10", "end_month": 1}, {"start_month": True, "end_month": 1}):
        assert lf.window_mid_doy(window) is None


# --- validated frost-risk Tmin -------------------------------------------------

def _with_risk(h, frost_hits=(), hard_hits=(), validated=(True, True), labels=("L0", "L2"), gap=None):
    """Risk series = raw Tmin, with -3 C on the given (year, day) cells of each field."""
    risk = {d: (t[0], t[0]) for d, t in h.days.items()}
    for y, day in frost_hits:
        risk[date(y, 1, day)] = (-3.0, risk[date(y, 1, day)][1])
    for y, day in hard_hits:
        risk[date(y, 1, day)] = (risk[date(y, 1, day)][0], -3.0)
    if gap:
        risk[gap[0]] = gap[1]
    return lf.FrostHistory(h.days, h.tmin_caveat, risk=risk, risk_validated=validated, risk_labels=labels)


def test_risk_field_hits_where_raw_tmin_does_not():
    h = _with_risk(_hist(0, 0), frost_hits=[(2000, 12)])  # barley -1: 0 C field
    r = lf.late_frost_reason("barley", cal=_cal(), sowing_doy=1, history=h, status="ready")
    assert r["params"]["years_hit"] == 1
    assert (r["params"]["tmin_basis"], r["params"]["tmin_caveat"], r["params"]["tmin_label"]) == ("risk_margin", "risk_margin", "L0")


def test_wheat_uses_each_field_and_label_omitted_when_they_differ():
    h = _with_risk(_hist(0, 0), frost_hits=[(2000, 12)], hard_hits=[(2001, 20)])
    p = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=h, status="ready")["params"]
    assert p["years_hit"] == 2 and p["tmin_basis"] == "risk_margin" and "tmin_label" not in p
    # a 0 C field hit in the grain window (-2 C stage) is not a hit: that stage reads the hard field
    h = _with_risk(_hist(0, 0), frost_hits=[(2000, 20)])
    assert lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=h, status="ready")["params"]["years_hit"] == 0


def test_hard_stage_falls_back_to_frost_field_when_hard_not_validated():
    h = _with_risk(_hist(0, 0), frost_hits=[(2000, 20)], validated=(True, False))
    p = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=h, status="ready")["params"]
    assert p["years_hit"] == 1 and p["tmin_basis"] == "risk_margin" and p["tmin_label"] == "L0"


def test_not_validated_is_raw_with_old_caveat():
    h = _with_risk(_hist(0, 0), frost_hits=[(2000, 12)], validated=(False, False))
    p = lf.late_frost_reason("barley", cal=_cal(), sowing_doy=1, history=h, status="ready")["params"]
    assert p["years_hit"] == 0 and p["tmin_basis"] == "raw" and p["tmin_caveat"] == "warm_bias" and "tmin_label" not in p


def test_mixed_keeps_raw_caveat():
    # flowering (-1 C) needs the 0 C field, not validated -> raw; grain filling (-2 C) uses the validated hard field
    h = _with_risk(_hist(0, 0), validated=(False, True), labels=(None, "L2"))
    h = lf.FrostHistory(h.days, "unvalidated_region", risk=h.risk, risk_validated=h.risk_validated, risk_labels=h.risk_labels)
    p = lf.late_frost_reason("wheat", cal=_cal(), sowing_doy=1, history=h, status="ready")["params"]
    assert (p["tmin_basis"], p["tmin_caveat"]) == ("mixed", "unvalidated_region")


def test_validated_gap_excludes_season():
    h = _with_risk(_hist(0, 0), gap=(date(2003, 1, 12), (None, None)))
    p = lf.late_frost_reason("barley", cal=_cal(), sowing_doy=1, history=h, status="ready")["params"]
    assert p["years_total"] == 9


def test_history_without_risk_fields_is_raw():
    p = lf.late_frost_reason("barley", cal=_cal(), sowing_doy=1, history=_hist(0, 0), status="ready")["params"]
    assert p["tmin_basis"] == "raw" and p["tmin_caveat"] == "warm_bias"
