"""Calendar of a plan (pure): fortnights, the main sowing window and a cover coherent with them.

Owner decisions (2026-10-13, W2/W6/W7): sowings are chosen by fortnight; the dates are proposed
coherently (a cover runs from the end of the previous crop to the main sowing); a cover shorter
than SHORT_COVER_WEEKS is warned about and allowed, and one with no fortnight at all does not fit.
A campaign C runs from Aug 1 of C-1 to Jul 31 of C.
"""
from __future__ import annotations

import calendar as _cal
from dataclasses import dataclass
from datetime import date, timedelta

from app.planning.history import AUTUMN_FROM_MONTH

SHORT_COVER_WEEKS = 8
OPTIONS_MARGIN = 2
DEFAULT_PERENNIAL_YEARS = 4


@dataclass(frozen=True)
class Fortnight:
    start: date
    end: date

    def as_dict(self) -> dict:
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}


@dataclass(frozen=True)
class PreviousCrop:
    crop: str | None
    end: date | None
    end_source: str  # harvest_recorded | planned_end | typical_maturity | campaign_start


def fortnight_of(day: date) -> Fortnight:
    if day.day <= 15:
        return Fortnight(day.replace(day=1), day.replace(day=15))
    last = _cal.monthrange(day.year, day.month)[1]
    return Fortnight(day.replace(day=16), day.replace(day=last))


def campaign_start(campaign: int) -> date:
    return date(campaign - 1, AUTUMN_FROM_MONTH, 1)


def campaign_end(campaign: int) -> date:
    return date(campaign, AUTUMN_FROM_MONTH, 1) - timedelta(days=1)


def campaign_fortnights(campaign: int) -> list[Fortnight]:
    out, day = [], campaign_start(campaign)
    while day <= campaign_end(campaign):
        f = fortnight_of(day)
        out.append(f)
        day = f.end + timedelta(days=1)
    return out


def _month_year(month: int, campaign: int) -> int:
    return campaign - 1 if month >= AUTUMN_FROM_MONTH else campaign


def _month_of_doy(doy: int) -> int:
    return (date(2025, 1, 1) + timedelta(days=max(1, min(doy, 365)) - 1)).month


def sowing_window(sowing: dict, campaign: int) -> list[Fortnight] | None:
    """Fortnights of the crop's sowing window in the campaign; None when unknown or outside it.

    ASSUMPTION (spec §4): an end month means its whole month; a typical day means its whole month.
    """
    window = sowing.get("sowing_window") or None
    if window and window.get("start_month") and window.get("end_month"):
        first, last = window["start_month"], window["end_month"]
    elif sowing.get("typical_sowing_doy"):
        first = last = _month_of_doy(int(sowing["typical_sowing_doy"]))
    else:
        return None
    start = date(_month_year(first, campaign), first, 1)
    end_year = _month_year(last, campaign)
    end = date(end_year, last, _cal.monthrange(end_year, last)[1])
    lo, hi = max(start, campaign_start(campaign)), min(end, campaign_end(campaign))
    if lo > hi:
        return None
    return [f for f in campaign_fortnights(campaign) if lo <= f.start <= hi]


def build_calendar(*, campaign: int, sowing: dict, perennial: bool, main_sowing: date | None,
                   previous: PreviousCrop | None, with_cover: bool) -> dict:
    fns = campaign_fortnights(campaign)
    window = sowing_window(sowing, campaign)
    if window:
        default = window[(len(window) - 1) // 2]
        i, j = fns.index(window[0]), fns.index(window[-1])
        options = fns[max(0, i - OPTIONS_MARGIN): min(len(fns), j + OPTIONS_MARGIN + 1)]
    else:
        default, options = None, fns
    selected = fortnight_of(main_sowing) if main_sowing else default
    main = {
        "window": {"start": window[0].start.isoformat(), "end": window[-1].end.isoformat()} if window else None,
        "window_source": sowing.get("source") if window else None,
        "default": default.as_dict() if default else None,
        "selected": selected.as_dict() if selected else None,
        "options": [f.as_dict() for f in options],
        "perennial_years_default": DEFAULT_PERENNIAL_YEARS if perennial else None,
    }
    prev = previous or PreviousCrop(None, None, "campaign_start")
    prev_end = prev.end or (campaign_start(campaign) - timedelta(days=1))
    out = {"fortnights": [f.as_dict() for f in fns], "main": main,
           "previous": {"crop": prev.crop, "end": prev.end.isoformat() if prev.end else None,
                        "end_source": prev.end_source if prev.end else "campaign_start"},
           "cover": None}
    if with_cover:
        out["cover"] = _cover(fns, prev_end, selected)
    return out


def _cover(fns: list[Fortnight], prev_end: date, main: Fortnight | None) -> dict:
    if main is None:  # no main sowing chosen yet: nothing to end the cover at
        return {"fits": False, "default": None, "options": [], "ends": None, "weeks": None, "short": False}
    options = [f for f in fns if f.start > prev_end and f.start < main.start]
    if not options:
        return {"fits": False, "default": None, "options": [], "ends": main.start.isoformat(),
                "weeks": None, "short": False}
    weeks = (main.start - options[0].start).days // 7
    return {"fits": True, "default": options[0].as_dict(), "options": [f.as_dict() for f in options],
            "ends": main.start.isoformat(), "weeks": weeks, "short": weeks < SHORT_COVER_WEEKS}
