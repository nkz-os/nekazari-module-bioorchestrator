from datetime import date, timedelta

import pytest

from app.services.engines.aquacrop_engine import DailyWeather
from app.services.sowing_rules import RULES, rule_for
from app.services.sowing_window import (
    WindowUnavailable,
    decide_sowing,
    window_for_season,
)

WHEAT = rule_for("WheatGDD")
MAIZE = rule_for("MaizeGDD")


def series(start, n, tmean, rain=None):
    """Days with Tmean = tmean(i) and precipitation rain(i) (default dry)."""
    out = {}
    for i in range(n):
        d = start + timedelta(days=i)
        t = tmean(i)
        out[d] = DailyWeather(day=d, tmin_c=t - 5, tmax_c=t + 5,
                              precip_mm=(rain(i) if rain else 0.0), et0_mm=2.0)
    return out


def cooling(i):  # 25 degC on 21 Jul, -0.1 degC a day
    return 25.0 - 0.1 * i


def test_every_rule_names_its_sources():
    for rule in RULES.values():
        assert rule.sources.get("window_start") and rule.sources.get("window_days")


def test_unknown_crop_has_no_rule():
    with pytest.raises(KeyError):
        rule_for("SunflowerGDD")


def test_autumn_window_opens_when_the_10_day_mean_falls_to_17():
    days = series(date(2019, 7, 21), 250, cooling)
    start, end = window_for_season(days, 2020, WHEAT)
    # Tmean(i) <= 17 from i = 80; the 10-day mean (i-9..i) reaches 17 at i = 84.5 -> 85.
    assert start == date(2019, 7, 21) + timedelta(days=85)
    assert end == start + timedelta(days=44)


def test_autumn_window_opens_on_1_december_if_it_never_cools():
    days = series(date(2019, 7, 21), 250, lambda i: 20.0)
    start, _ = window_for_season(days, 2020, WHEAT)
    assert start == date(2019, 12, 1)


def test_spring_window_opens_when_the_mean_rises_to_13():
    days = series(date(2019, 12, 22), 200, lambda i: 5.0 + 0.1 * i)
    start, end = window_for_season(days, 2020, MAIZE)
    assert start.year == 2020 and start.month in (3, 4)
    assert end == start + timedelta(days=59)


def test_spring_window_unavailable_if_it_never_warms():
    days = series(date(2019, 12, 22), 400, lambda i: 8.0)
    with pytest.raises(WindowUnavailable, match="never rises"):
        window_for_season(days, 2020, MAIZE)


def test_weather_gap_in_window_is_unavailable():
    days = series(date(2019, 7, 21), 250, cooling)
    del days[date(2019, 10, 20)]
    with pytest.raises(WindowUnavailable, match="gap"):
        window_for_season(days, 2020, WHEAT)


def test_rain_trigger_sows_on_the_first_day_with_20_mm_in_6_days():
    start = date(2019, 10, 1)
    days = series(start - timedelta(days=10), 80, lambda i: 12.0,
                  rain=lambda i: 8.0 if i in (24, 26, 27) else 0.0)  # 15, 17, 18 Oct
    dec = decide_sowing(days, start, start + timedelta(days=44), WHEAT)
    assert dec.sowing_date == date(2019, 10, 18) and dec.how == "triggered"
    assert dec.tempero_checked is False


def test_no_trigger_is_forced_on_the_last_day():
    start = date(2019, 10, 1)
    days = series(start - timedelta(days=10), 80, lambda i: 12.0)
    dec = decide_sowing(days, start, start + timedelta(days=44), WHEAT)
    assert dec.how == "forced" and dec.sowing_date == start + timedelta(days=44)


def test_too_wet_soil_delays_sowing_after_the_rain():
    start = date(2019, 10, 1)
    days = series(start - timedelta(days=10), 80, lambda i: 12.0,
                  rain=lambda i: 25.0 if i == 20 else 0.0)  # 25 mm on 10 Oct
    topsoil = {start + timedelta(days=k): 0.40 for k in range(45)}
    for k in range(12, 45):  # dries below the wet limit from 13 Oct
        topsoil[start + timedelta(days=k)] = 0.30
    dec = decide_sowing(days, start, start + timedelta(days=44), WHEAT, topsoil, (0.35, 0.13))
    assert dec.sowing_date == date(2019, 10, 13) and dec.tempero_checked is True


def test_too_dry_soil_blocks_autumn_but_not_irrigated_maize():
    start = date(2019, 10, 1)
    days = series(start - timedelta(days=10), 80, lambda i: 12.0,
                  rain=lambda i: 25.0 if i == 20 else 0.0)
    dry = {start + timedelta(days=k): 0.10 for k in range(60)}
    wheat = decide_sowing(days, start, start + timedelta(days=44), WHEAT, dry, (0.35, 0.13))
    assert wheat.how == "forced"
    maize = decide_sowing(days, start, start + timedelta(days=59), MAIZE, dry, (0.35, 0.13))
    assert maize.how == "triggered" and maize.sowing_date == start


def test_29_february_is_never_the_sowing_day():
    start = date(2020, 2, 29)  # leap year; no rain trigger for maize
    days = series(start - timedelta(days=10), 80, lambda i: 14.0)
    dec = decide_sowing(days, start, start + timedelta(days=59), MAIZE)
    assert dec.sowing_date == date(2020, 3, 1) and dec.how == "triggered"


def test_forced_sowing_on_29_february_moves_to_the_28th():
    start = date(2020, 1, 15)
    days = series(start - timedelta(days=10), 80, lambda i: 12.0)
    dec = decide_sowing(days, start, date(2020, 2, 29), WHEAT)
    assert dec.how == "forced" and dec.sowing_date == date(2020, 2, 28)
