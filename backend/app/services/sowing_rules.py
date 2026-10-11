"""Sowing rules per crop: when the sowing window opens and closes, and what triggers sowing.

Every value carries its source. A crop without a rule has no sowing window;
nothing here is a default.

- Window start (temperature): FAO AquaCrop 7.3 Reference Manual, Annex II,
  note 2 (p. 69, from FAO-56): winter wheat when the 10-day running mean of
  daily mean air temperature falls to 17 degC or on 1 December, whichever is
  first; grain maize when it rises to 13 degC.
- Rain trigger for autumn cereals: >= 20 mm over 6 consecutive days
  (Grosse-Heilmann et al. 2026, Earth 7:27, p. 5, Mediterranean durum wheat);
  adopted for all autumn cereals by the platform owner.
- Window length: platform owner's criterion (45 days autumn cereals, 60 days
  irrigated maize); no published value exists.
- Barley uses the winter-wheat rule: an extrapolation chosen by the platform
  owner, declared as such.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Season = Literal["autumn", "spring"]
Tempero = Literal["range", "not_wet", "none"]

FAO_ANNEX_II = "FAO AquaCrop 7.3 Reference Manual, Annex II, note 2, p. 69 (FAO-56)"
RAIN_TRIGGER_SOURCE = (
    "Grosse-Heilmann et al. 2026, Earth 7:27, p. 5; adopted for autumn cereals by the platform owner"
)
OWNER = "platform owner's criterion"


@dataclass(frozen=True)
class SowingRule:
    crop: str
    season: Season               # autumn: sown in the year before harvest
    threshold_c: float           # 10-day running mean of daily mean temperature
    crossing: Literal["falls", "rises"]
    search_from: tuple[int, int]  # (month, day) where the search for the crossing starts
    latest_start: tuple[int, int] | None  # (month, day) cap on the window start
    window_days: int
    rain_mm: float | None        # trigger: rain over the last `rain_days` days
    rain_days: int | None
    tempero: Tempero             # range: dry <= theta <= wet; not_wet: theta <= wet
    sources: dict[str, str] = field(default_factory=dict)
    running_mean_days: int = 10


_AUTUMN_CEREAL = {
    "season": "autumn", "threshold_c": 17.0, "crossing": "falls", "search_from": (8, 1),
    "latest_start": (12, 1), "window_days": 45, "rain_mm": 20.0, "rain_days": 6,
    "tempero": "range",
}

RULES: dict[str, SowingRule] = {
    "WheatGDD": SowingRule(crop="WheatGDD", **_AUTUMN_CEREAL, sources={
        "window_start": FAO_ANNEX_II, "rain_trigger": RAIN_TRIGGER_SOURCE,
        "window_days": OWNER, "tempero": "soil module tillage limits"}),
    "BarleyGDD": SowingRule(crop="BarleyGDD", **_AUTUMN_CEREAL, sources={
        "window_start": FAO_ANNEX_II + "; winter-wheat rule extrapolated to barley by the platform owner",
        "rain_trigger": RAIN_TRIGGER_SOURCE, "window_days": OWNER,
        "tempero": "soil module tillage limits"}),
    "MaizeGDD": SowingRule(
        crop="MaizeGDD", season="spring", threshold_c=13.0, crossing="rises",
        search_from=(1, 1), latest_start=None, window_days=60, rain_mm=None,
        rain_days=None, tempero="not_wet", sources={
            "window_start": FAO_ANNEX_II, "window_days": OWNER,
            "tempero": "soil module wet tillage limit (irrigated: dryness is corrected by irrigation)"}),
}


def rule_for(crop: str) -> SowingRule:
    """The sowing rule of an AquaCrop crop; KeyError when it has none."""
    return RULES[crop]
