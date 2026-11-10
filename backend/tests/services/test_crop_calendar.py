"""Cereal phenology calendar: the engine's own parameters, its GDD method, milestone dates."""
from datetime import date, timedelta

import pytest

from app.services import crop_calendar as cc
from app.services import yield_calibration as yc
from app.services.engines import aquacrop_engine as aq

NAVARRA = (42.7, -1.6)  # synthetic point inside a calibrated province
OUTSIDE = (48.0, 2.0)   # no calibrated region
SOWING = date(2026, 11, 1)


def _w(day, tmin, tmax):
    return aq.DailyWeather(day, tmin, tmax, 0.0, 2.0)


def _series(start, n, tmin, tmax):
    return [_w(start + timedelta(days=i), tmin, tmax) for i in range(n)]


# ---------------------------------------------------------------- GDD method 3
def test_gdd_m3_matches_the_library_function():
    from aquacrop.solution.growing_degree_day import growing_degree_day
    for tbase, tupp in ((0.0, 26.0), (2.0, 28.0)):
        for tmin in range(-10, 40, 3):
            for tmax in range(tmin, 45, 4):
                assert cc.daily_gdd_m3(tmin, tmax, tbase, tupp) == pytest.approx(
                    growing_degree_day(3, tupp, tbase, tmax, tmin))


def test_gdd_m3_tmax_below_base_is_zero():
    assert cc.daily_gdd_m3(-8.0, 1.0, 2.0, 28.0) == 0.0


def test_gdd_m3_tmin_above_cutoff_is_clipped_to_cutoff():
    assert cc.daily_gdd_m3(30.0, 35.0, 0.0, 26.0) == pytest.approx(26.0)


def test_gdd_m3_tmin_below_base_is_not_raised_to_base():
    # Tmax 12 (inside range), Tmin -6 (below base 0): mean 3, not 6
    assert cc.daily_gdd_m3(-6.0, 12.0, 0.0, 26.0) == pytest.approx(3.0)


# ---------------------------------------------------------------- effective calendar
def test_builtin_wheat_calendar_is_the_engines_crop():
    cal = cc.effective_calendar("wheat")
    crop = aq._aquacrop_crop("WheatGDD", date(2026, 11, 1), {})
    for attr, got in (("Emergence", cal.emergence), ("HIstart", cal.hi_start), ("Flowering", cal.flowering),
                      ("Senescence", cal.senescence), ("Maturity", cal.maturity),
                      ("Tbase", cal.tbase), ("Tupp", cal.tupp)):
        assert got == getattr(crop, attr)
    assert cal.calibration["status"] == "builtin" and cal.calibration["region"] is None


def test_calibrated_wheat_uses_the_provincial_overrides_only_rainfed():
    from app.services import yield_calibration as yc
    cal = cc.effective_calendar("wheat", lat=NAVARRA[0], lon=NAVARRA[1], irrigation="rainfed")
    prov = yc.calibrated_phenology("wheat", "rainfed", {"scheme": "es_province", "code": cal.calibration["region"]["code"]})
    assert cal.calibration["status"] == "calibrated" and cal.calibration["scheme"] == "es_province"
    assert cal.calibration["version"]
    assert cal.hi_start == prov["HIstart"] and cal.maturity == prov["Maturity"]
    full = cc.effective_calendar("wheat", lat=NAVARRA[0], lon=NAVARRA[1], irrigation="full")
    assert full.calibration["status"] == "builtin" and full.hi_start == cc.effective_calendar("wheat").hi_start


def test_outside_calibrated_regions_is_builtin():
    cal = cc.effective_calendar("wheat", lat=OUTSIDE[0], lon=OUTSIDE[1], irrigation="rainfed")
    assert cal.calibration["status"] == "builtin"


def test_barley_is_the_es_cultivar_and_calibrated_in_a_province_of_the_file():
    cal = cc.effective_calendar("barley", lat=NAVARRA[0], lon=NAVARRA[1], irrigation="rainfed")
    assert cal.cultivar == "BarleyES" and cal.tbase == 2.0 and cal.tupp == 28.0
    assert cal.calibration["status"] == "calibrated" and cal.calibration["scheme"] == "es_province"
    assert cal.calibration["region"]["code"] == "31"
    assert cal.calibration["version"] == yc._calibration("barley")["version"] == "2026-11-10"
    assert (cal.emergence, cal.hi_start, cal.flowering, cal.senescence, cal.maturity) == (
        117.0, 1003.0, 180.0, 1160.6, 1746.0)
    assert any("Abrha" in s["detail"] for s in cal.sources)
    assert any(s["short"] == "Calibration" for s in cal.sources)
    builtin = (cc.effective_calendar("barley", lat=NAVARRA[0], lon=NAVARRA[1], irrigation="full"),
               cc.effective_calendar("barley", lat=OUTSIDE[0], lon=OUTSIDE[1], irrigation="rainfed"),
               cc.effective_calendar("barley"))
    for b in builtin:
        assert b.calibration["status"] == "builtin"
        assert (b.emergence, b.hi_start, b.senescence, b.maturity) == (117.0, 1003.0, 1210.0, 1979.0)


def test_barley_validation_follows_the_calendar_used():
    cal = cc.effective_calendar("barley", lat=NAVARRA[0], lon=NAVARRA[1], irrigation="rainfed")
    v = cc.validation_for("barley", cal.calibration["status"])
    assert v["status"] == "measured" and v["measured_on"] == "calibrated"
    assert v["note"] and v["groups"]["winter_type"]["heading"]["n"] > 0
    assert cc.validation_for("barley", "builtin") == {"status": "not_validated", "reason": "calendar_not_measured"}
    assert "note" not in cc.validation_for("wheat", "calibrated")


def test_durum_shares_the_wheat_parameters_and_other_crops_are_refused():
    assert cc.effective_calendar("durum_wheat").maturity == cc.effective_calendar("wheat").maturity
    with pytest.raises(ValueError):
        cc.effective_calendar("maize")


def test_effective_calendar_is_cached():
    assert cc.effective_calendar("wheat") is cc.effective_calendar("wheat")


def test_stage_table_has_the_five_contract_stages():
    cal = cc.effective_calendar("wheat")
    t = cc.stage_table(cal)
    assert [s["stage"] for s in t] == ["emergence", "vegetative", "flowering", "grain_filling", "maturity"]
    assert [(s["bbchFrom"], s["bbchTo"]) for s in t] == [(0, 9), (10, 49), (51, 69), (71, 87), (89, 89)]
    assert [(s["gddMin"], s["gddMax"]) for s in t] == [
        (0, cal.emergence), (cal.emergence, cal.hi_start), (cal.hi_start, cal.hi_start + cal.flowering),
        (cal.hi_start + cal.flowering, cal.maturity), (cal.maturity, cal.maturity)]
    assert all(s["baseTemp"] == cal.tbase for s in t)


# ---------------------------------------------------------------- milestone dates
def _cal(emergence=10.0, hi=30.0, sen=50.0, mat=70.0):
    base = cc.effective_calendar("wheat")
    from dataclasses import replace
    return replace(base, tbase=0.0, tupp=26.0, emergence=emergence, hi_start=hi, flowering=5.0,
                   senescence=sen, maturity=mat)


def test_all_reached_within_observed_weather():
    # 10 GDD/day (tmin 0, tmax 20): cumulative 10 at day 1 of sowing
    obs = _series(SOWING, 10, 0.0, 20.0)
    out = cc.milestone_dates(SOWING, obs, [], _cal())
    assert out["emergence"] == {"gdd": 10.0, "status": "reached", "date": "2026-11-01"}
    assert out["flowering_start"]["date"] == "2026-11-03"
    assert out["senescence_start"]["date"] == "2026-11-05"
    assert out["maturity"]["date"] == "2026-11-07"


def test_days_before_sowing_do_not_count():
    obs = _series(SOWING - timedelta(days=5), 5 + 10, 0.0, 20.0)
    out = cc.milestone_dates(SOWING, obs, [], _cal())
    assert out["emergence"]["date"] == "2026-11-01"


def test_projected_percentiles_from_members():
    obs = _series(SOWING, 2, 0.0, 20.0)  # cum 20: emergence reached day 1
    last = obs[-1].day
    # members: 5, 10, 20 GDD/day after the observed days
    members = [obs + _series(last + timedelta(days=1), 40, g, g) for g in (5.0, 10.0, 20.0)]
    out = cc.milestone_dates(SOWING, obs, members, _cal())
    assert out["emergence"]["status"] == "reached"
    m = out["flowering_start"]  # needs 30: 10 more -> day 2 / 1 / 1 after the last observed day; P90 of (4th, 3rd, 3rd) is 3.8 -> 4th
    assert m["status"] == "projected" and m["gdd"] == 30.0
    assert m["date_p10"] == "2026-11-03" and m["date_p50"] == "2026-11-03" and m["date_p90"] == "2026-11-04"
    assert m["n_members"] == 3


def test_unavailable_without_members():
    obs = _series(SOWING, 2, 0.0, 20.0)
    out = cc.milestone_dates(SOWING, obs, [], _cal())
    assert out["maturity"] == {"gdd": 70.0, "status": "unavailable", "reason": "no_analog_members"}


def test_unavailable_when_members_never_reach_it():
    obs = _series(SOWING, 2, 0.0, 20.0)
    last = obs[-1].day
    members = [obs + _series(last + timedelta(days=1), 3, 2.0, 2.0) for _ in range(3)]
    out = cc.milestone_dates(SOWING, obs, members, _cal())
    assert out["maturity"]["status"] == "unavailable" and out["maturity"]["reason"] == "not_reached"


def test_unavailable_when_only_some_members_reach_it():
    obs = _series(SOWING, 2, 0.0, 20.0)
    last = obs[-1].day
    fast = obs + _series(last + timedelta(days=1), 20, 40.0, 40.0)
    slow = obs + _series(last + timedelta(days=1), 20, 0.5, 0.5)
    out = cc.milestone_dates(SOWING, obs, [fast, fast, slow], _cal())
    assert out["maturity"]["status"] == "unavailable" and out["maturity"]["reason"] == "not_reached_by_all_members"


def test_too_few_members_is_unavailable():
    obs = _series(SOWING, 2, 0.0, 20.0)
    last = obs[-1].day
    members = [obs + _series(last + timedelta(days=1), 20, 40.0, 40.0)] * 2
    out = cc.milestone_dates(SOWING, obs, members, _cal())
    assert out["maturity"] == {"gdd": 70.0, "status": "unavailable", "reason": "too_few_members"}


def test_preseason_observed_before_sowing_projects_all():
    obs = _series(SOWING - timedelta(days=3), 3, 0.0, 20.0)  # all before sowing
    last = obs[-1].day
    members = [obs + _series(last + timedelta(days=1), 30, 0.0, 20.0) for _ in range(3)]
    out = cc.milestone_dates(SOWING, obs, members, _cal())
    assert out["emergence"]["status"] == "projected" and out["emergence"]["date_p50"] == "2026-11-01"


def test_block_wraps_calendar_and_milestones():
    cal = cc.effective_calendar("wheat")
    obs = _series(SOWING, 3, 0.0, 20.0)
    block = cc.phenology_block(SOWING, obs, [], cal)
    assert block["gdd_method"] == "aquacrop_m3" and block["origin"] == "sowing"
    assert block["basis"] == "thermal_time" and block["notes"] == ["no_stress_delay"]
    assert block["upper_cutoff"] == cal.tupp and block["base_temp"] == cal.tbase
    assert set(block["milestones"]) == {"emergence", "flowering_start", "senescence_start", "maturity"}
    assert block["model"]["id"] == "aquacrop_calendar" and block["calibration"]["status"] == "builtin"


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), None])
@pytest.mark.parametrize("field", ["tmin", "tmax"])
def test_non_finite_temperature_raises_naming_the_day(bad, field):
    obs = _series(SOWING, 3, 0.0, 20.0)
    w = obs[1]
    obs[1] = _w(w.day, bad if field == "tmin" else 0.0, bad if field == "tmax" else 20.0)
    with pytest.raises(ValueError, match="2026-11-02"):
        cc.milestone_dates(SOWING, obs, [], _cal())


def test_non_finite_temperature_in_a_member_raises():
    obs = _series(SOWING, 2, 0.0, 20.0)
    last = obs[-1].day
    bad = obs + [_w(last + timedelta(days=1), float("nan"), 10.0)] + _series(last + timedelta(days=2), 30, 5.0, 15.0)
    with pytest.raises(ValueError, match="2026-11-03"):
        cc.milestone_dates(SOWING, obs, [bad, bad, bad], _cal())


def test_validation_publishes_the_measured_error_of_this_crop():
    import json

    from app.services import yield_calibration as yc
    raw = json.loads((yc.DATA_DIR / "phenology_validation.json").read_text(encoding="utf-8"))
    v = cc.validation_for("barley", "calibrated")
    assert v["status"] == "measured" and v["version"] == raw["version"] and v["sign"] == raw["sign"]
    assert v["dataset"] == raw["dataset"] and v["comparisons"] == raw["comparisons"]
    assert v["groups"] == raw["crops"]["barley"] and v["measured_on"] == "calibrated"
    assert v["note"] == raw["notes"]["barley"]
    for slug in ("wheat", "durum_wheat"):
        w = cc.validation_for(slug, "calibrated")
        assert w["status"] == "measured" and w["groups"] == raw["crops"][slug] and w["measured_on"] == "calibrated"
    block = cc.phenology_block(SOWING, _series(SOWING, 3, 0.0, 20.0), [], cc.effective_calendar("barley"))
    assert block["validation"] == cc.validation_for("barley", "builtin")


@pytest.mark.parametrize("slug,measured_on,other",
                         [("wheat", "calibrated", "builtin"), ("durum_wheat", "calibrated", "builtin"),
                          ("barley", "calibrated", "builtin")])
def test_validation_is_withheld_for_a_calendar_it_was_not_measured_on(slug, measured_on, other):
    assert cc.validation_for(slug, measured_on)["status"] == "measured"
    assert cc.validation_for(slug, other) == {"status": "not_validated", "reason": "calendar_not_measured"}


def test_slug_missing_from_measured_on_is_not_measured(monkeypatch):
    monkeypatch.setattr(cc, "_validation_file", lambda: {"version": "x", "sign": "s", "dataset": {},
                                                          "comparisons": {}, "crops": {"wheat": {"g": {}}},
                                                          "measured_on": {}})
    assert cc.validation_for("wheat", "calibrated") == {"status": "not_validated", "reason": "calendar_not_measured"}


def test_phenology_block_of_a_builtin_wheat_calendar_is_not_validated():
    cal = cc.effective_calendar("wheat")
    assert cal.calibration["status"] == "builtin"
    block = cc.phenology_block(SOWING, _series(SOWING, 3, 0.0, 20.0), [], cal)
    assert block["validation"] == {"status": "not_validated", "reason": "calendar_not_measured"}


def test_validation_falls_back_when_the_file_is_missing_or_broken(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(cc, "VALIDATION_PATH", tmp_path / "absent.json")
    cc._validation_file.cache_clear()
    try:
        assert cc.validation_for("wheat", "calibrated") == {"status": "not_validated"}
        bad = tmp_path / "bad.json"
        bad.write_text("{not json", encoding="utf-8")
        monkeypatch.setattr(cc, "VALIDATION_PATH", bad)
        cc._validation_file.cache_clear()
        assert cc.validation_for("wheat", "calibrated") == {"status": "not_validated"}
        assert "phenology_validation_unavailable" in caplog.text
    finally:
        cc._validation_file.cache_clear()


def test_crop_without_an_entry_is_not_validated(monkeypatch):
    monkeypatch.setattr(cc, "_validation_file", lambda: {"version": "x", "sign": "s", "dataset": {},
                                                          "comparisons": {}, "crops": {}})
    assert cc.validation_for("wheat", "calibrated") == {"status": "not_validated"}
