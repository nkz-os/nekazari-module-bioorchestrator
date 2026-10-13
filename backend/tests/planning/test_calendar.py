# backend/tests/planning/test_calendar.py
"""Calendar of a plan: fortnights, the main sowing window, and a cover coherent with both."""
from datetime import date

from app.planning import calendar as cal

F = cal.Fortnight
WHEAT = {"sowing_window": {"start_month": 10, "end_month": 11}, "typical_sowing_doy": None, "source": "table"}
MAIZE = {"sowing_window": {"start_month": 4, "end_month": 5}, "typical_sowing_doy": None, "source": "table"}
GGCMI = {"sowing_window": None, "typical_sowing_doy": 288, "source": "ggcmi"}  # 15 Oct
NONE = {"sowing_window": None, "typical_sowing_doy": None, "source": "crop_season_slot"}


def test_fortnights_split_each_month_at_the_15th():
    assert cal.fortnight_of(date(2026, 10, 20)) == F(date(2026, 10, 16), date(2026, 10, 31))
    assert cal.fortnight_of(date(2027, 2, 3)) == F(date(2027, 2, 1), date(2027, 2, 15))
    assert cal.fortnight_of(date(2028, 2, 20)).end == date(2028, 2, 29)  # leap year


def test_a_campaign_has_24_fortnights_from_august_to_july():
    fns = cal.campaign_fortnights(2027)
    assert len(fns) == 24 and fns[0].start == date(2026, 8, 1) and fns[-1].end == date(2027, 7, 31)


def test_an_autumn_window_falls_in_the_year_before_the_campaign():
    fns = cal.sowing_window(WHEAT, 2027)
    assert fns[0].start == date(2026, 10, 1) and fns[-1].end == date(2026, 11, 30) and len(fns) == 4


def test_a_typical_day_becomes_its_whole_month():
    fns = cal.sowing_window(GGCMI, 2027)
    assert (fns[0].start, fns[-1].end) == (date(2026, 10, 1), date(2026, 10, 31))


def test_no_window_is_none_never_a_guess():
    assert cal.sowing_window(NONE, 2027) is None


def test_default_is_the_middle_fortnight_and_options_add_two_each_side():
    out = cal.build_calendar(campaign=2027, sowing=WHEAT, perennial=False, main_sowing=None,
                             previous=None, with_cover=False)
    main = out["main"]
    assert main["default"] == {"start": "2026-10-16", "end": "2026-10-31"}  # 4 fortnights: earlier middle
    assert main["selected"] == main["default"]
    assert main["options"][0] == {"start": "2026-09-01", "end": "2026-09-15"}
    assert main["options"][-1] == {"start": "2026-12-16", "end": "2026-12-31"}
    assert main["perennial_years_default"] is None and out["cover"] is None


def test_unknown_window_offers_every_fortnight_and_selects_none():
    main = cal.build_calendar(campaign=2027, sowing=NONE, perennial=False, main_sowing=None,
                              previous=None, with_cover=False)["main"]
    assert main["window"] is None and main["default"] is None and main["selected"] is None
    assert len(main["options"]) == 24


def test_a_moved_main_sowing_is_selected():
    main = cal.build_calendar(campaign=2027, sowing=WHEAT, perennial=False, main_sowing=date(2026, 11, 1),
                              previous=None, with_cover=False)["main"]
    assert main["selected"] == {"start": "2026-11-01", "end": "2026-11-15"}


def test_cover_from_the_previous_harvest_to_the_main_sowing():
    prev = cal.PreviousCrop("wheat", date(2026, 7, 10), "harvest_recorded")
    out = cal.build_calendar(campaign=2027, sowing=MAIZE, perennial=False, main_sowing=None,
                             previous=prev, with_cover=True)
    cover = out["cover"]
    # Jul 16 starts after the harvest but belongs to the previous campaign: the cover starts Aug 1.
    assert cover["fits"] and cover["default"] == {"start": "2026-08-01", "end": "2026-08-15"}
    assert cover["ends"] == out["main"]["selected"]["start"] == "2027-04-16"
    assert cover["weeks"] == (date(2027, 4, 16) - date(2026, 8, 1)).days // 7 and not cover["short"]


def test_a_short_cover_is_flagged_not_refused():
    prev = cal.PreviousCrop("wheat", date(2026, 8, 20), "planned_end")
    cover = cal.build_calendar(campaign=2027, sowing=WHEAT, perennial=False, main_sowing=None,
                               previous=prev, with_cover=True)["cover"]
    assert cover["fits"] and cover["short"] and cover["weeks"] < cal.SHORT_COVER_WEEKS


def test_a_cover_that_does_not_fit_says_so():
    prev = cal.PreviousCrop("maize", date(2026, 10, 25), "typical_maturity")  # after the wheat default
    cover = cal.build_calendar(campaign=2027, sowing=WHEAT, perennial=False, main_sowing=None,
                               previous=prev, with_cover=True)["cover"]
    assert cover["fits"] is False and cover["default"] is None and cover["weeks"] is None


def test_without_a_previous_crop_the_cover_starts_with_the_campaign():
    cover = cal.build_calendar(campaign=2027, sowing=MAIZE, perennial=False, main_sowing=None,
                               previous=None, with_cover=True)["cover"]
    assert cover["default"] == {"start": "2026-08-01", "end": "2026-08-15"}
    assert cover["options"][0] == cover["default"]


def test_a_perennial_carries_its_default_years():
    main = cal.build_calendar(campaign=2027, sowing=WHEAT, perennial=True, main_sowing=None,
                              previous=None, with_cover=False)["main"]
    assert main["perennial_years_default"] == cal.DEFAULT_PERENNIAL_YEARS


def test_a_window_straddling_the_campaign_boundary_is_unknown():
    # Jun -> Sep maps to Jun of C and Sep of C-1: nothing is left, never a default from the next campaign.
    straddle = {"sowing_window": {"start_month": 6, "end_month": 9}, "typical_sowing_doy": None, "source": "t"}
    assert cal.sowing_window(straddle, 2027) is None
    main = cal.build_calendar(campaign=2027, sowing=straddle, perennial=False, main_sowing=None,
                              previous=None, with_cover=True)
    assert main["main"]["default"] is None and len(main["main"]["options"]) == 24
    assert main["cover"]["fits"] is False  # no main sowing chosen yet: nothing to end a cover at


def test_a_spring_window_maps_to_the_campaign_year():
    fns = cal.sowing_window(MAIZE, 2027)
    assert (fns[0].start, fns[-1].end) == (date(2027, 4, 1), date(2027, 5, 31))
