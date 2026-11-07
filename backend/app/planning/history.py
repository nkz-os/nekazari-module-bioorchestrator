"""Crop history and plan cells from the platform's crop-cycles answer (pure).

An annual cycle occupies the campaign it ends in (the platform rule). A herbaceous perennial
(alfalfa) occupies every campaign from the one its sowing opens to the one its end closes,
or up to the campaign being looked at while it has no end yet.
"""
from __future__ import annotations

import math
from datetime import UTC, date, datetime
from typing import Any

from app.planning.rotation import PastCrop
from app.species_registry import get_species_info, resolve_species

_PROVENANCE = ("actual", "declared", "planned")
_DECLARED_YIELD_TERMS = (
    "https://nkz-os.org/ns/declaredYieldKgHa",
    "nkz:declaredYieldKgHa",
    "declaredYieldKgHa",
)
_VARIETY_TERMS = (
    "https://nkz-os.org/ns/variety",
    "nkz:variety",
    "variety",
    "https://uri.etsi.org/ngsi-ld/default-context/variety",
)
# From this month a campaign of the current year is over and planning moves to the next one. The platform
# applies the same rollover (SDK crop_cycles, field-operations declared crops): keep the three equal.
ROLLOVER_MONTH = 7
AUTUMN_FROM_MONTH = 8  # a sowing from August on opens the next calendar year's campaign


def plain(value: Any) -> Any:
    """keyValues value, normalized Property or typed literal -> plain value."""
    if isinstance(value, dict):
        if "value" in value:
            value = value["value"]
        if isinstance(value, dict) and "@value" in value:
            value = value["@value"]
    return value


def cycles_of(payload: dict | None) -> list[dict]:
    if not payload:
        return []
    return [c for c in (payload.get("cycles") or []) if isinstance(c, dict) and c.get("status") != "cancelled"]


def species_slug(raw: Any) -> str | None:
    return resolve_species(raw) if isinstance(raw, str) and raw else None


def campaign_of_start(day: date) -> int:
    return day.year + 1 if day.month >= AUTUMN_FROM_MONTH else day.year


def iso_date(value: Any) -> date | None:
    value = plain(value)
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def first_campaign(cycle: dict) -> int | None:
    if cycle.get("lifecycle") == "perennial":
        start = iso_date((cycle.get("start") or {}).get("date"))
        if start:
            return campaign_of_start(start)
    label = cycle.get("campaign_label")
    return label if isinstance(label, int) else None


def cycle_campaigns(cycle: dict, until: int) -> list[int]:
    first = first_campaign(cycle)
    if first is None:
        return []
    if cycle.get("lifecycle") != "perennial":
        return [first]
    end = iso_date((cycle.get("end") or {}).get("date"))
    last = end.year if end else until
    return list(range(first, max(first, last) + 1))


_CLOSED = frozenset({"harvested", "terminated"})


def _history_cycles(payload: dict | None) -> list[dict]:
    """Cycles that count as history: a declared-only one only once closed, like the platform SDK."""
    return [c for c in cycles_of(payload) if not declared_only(c) or c.get("status") in _CLOSED]


def declared_only(cycle: dict) -> bool:
    """A crop known only by the campaign a person declared: no dates at all."""
    return (cycle.get("declared_campaign") is not None
            and not (cycle.get("start") or {}).get("date") and not (cycle.get("end") or {}).get("date"))


def _without_shadowed_declarations(cycles: list[dict]) -> list[dict]:
    """Facts beat declarations: a declared-only main crop yields to a dated one of the same campaign."""
    def main(c: dict) -> bool:
        return (c.get("role") or "main_crop") == "main_crop"

    dated = {first_campaign(c) for c in cycles if main(c) and not declared_only(c)}
    return [c for c in cycles if not (main(c) and declared_only(c) and first_campaign(c) in dated)]


def past_crops(payload: dict | None, before_campaign: int) -> list[PastCrop]:
    out = []
    for cycle in _without_shadowed_declarations(_history_cycles(payload)):
        slug = species_slug(cycle.get("species"))
        if not slug:
            continue
        for campaign in cycle_campaigns(cycle, until=before_campaign - 1):
            if campaign < before_campaign:
                out.append(PastCrop(slug, campaign))
    return out


def history_complete(payload: dict | None) -> bool:
    """True only when every cycle can be placed: a known species and a known campaign.

    A cycle that cannot be placed may hide a rotation restriction, so rotation rules
    must not be read as "no restriction" from a partial history.
    """
    if payload is None:
        return False
    return all(species_slug(c.get("species")) and first_campaign(c) is not None for c in cycles_of(payload))


HISTORY_LOOKBACK = 2  # campaigns before the planned one that rotation CompletionBar asks for
HISTORY_WINDOW = 5  # past campaigns exposed in the parcel history editor and FaST 5-year window


def _positive_float(value: Any) -> float | None:
    val = plain(value)
    if isinstance(val, (int, float)) and not isinstance(val, bool) and math.isfinite(val) and val > 0:
        return float(val)
    return None


def cycle_variety(cycle: dict, crop: dict | None = None) -> str | None:
    for raw in (cycle.get("variety"), *( [crop.get(t) for t in _VARIETY_TERMS] if crop else [] )):
        val = plain(raw)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def cycle_yield_kg_ha(cycle: dict, crop: dict | None = None, area_ha: float | None = None) -> tuple[float | None, str | None]:
    """(yield_kg_ha, 'measured' | 'declared' | None). Measured harvest weight beats declared yield when area_ha > 0."""
    weight = _positive_float(cycle.get("harvested_weight_kg"))
    area = _positive_float(area_ha)
    if weight is not None and area is not None:
        return round(weight / area, 1), "measured"
    declared = _positive_float(cycle.get("declared_yield_kg_ha"))
    if declared is None and crop:
        for term in _DECLARED_YIELD_TERMS:
            declared = _positive_float(crop.get(term))
            if declared is not None:
                break
    if declared is not None:
        return round(declared, 1), "declared"
    return None, None


def campaign_is_over(campaign: int, today: date) -> bool:
    """The rule the platform applies to a declared past crop: the planning rollover (July) closes a campaign."""
    return campaign < today.year or (campaign == today.year and today.month >= ROLLOVER_MONTH)


def missing_history_campaigns(payload: dict | None, campaign: int, today: date | None = None) -> list[int] | None:
    """Of the campaigns just before `campaign`, those with no main crop (dated or declared) placed in them.

    Only campaigns a past crop can be declared for are offered (the platform refuses one that is not over).
    None when the history could not be read: unknown is not "complete" and not "all missing".
    """
    if payload is None:
        return None
    today = today or datetime.now(UTC).date()
    covered: set[int] = set()
    for cycle in _history_cycles(payload):
        if (cycle.get("role") or "main_crop") == "main_crop":
            covered.update(cycle_campaigns(cycle, until=campaign - 1))
    return [c for c in range(campaign - HISTORY_LOOKBACK, campaign)
            if c not in covered and campaign_is_over(c, today)]


def cell_from_cycle(cycle: dict, crop: dict | None, area_ha: float | None = None) -> dict:
    slug = species_slug(cycle.get("species"))
    info = get_species_info(slug) if slug else None
    start, end = cycle.get("start") or {}, cycle.get("end") or {}
    provenance = start.get("provenance")
    if provenance not in _PROVENANCE and declared_only(cycle):
        provenance = "declared"
    yield_kg_ha, yield_source = cycle_yield_kg_ha(cycle, crop, area_ha)
    return {
        "crop_id": cycle.get("crop_id"),
        "species": slug,
        "species_raw": cycle.get("species"),
        "names": dict((info or {}).get("common_names") or {}),
        "variety": cycle_variety(cycle, crop),
        "role": cycle.get("role") or "main_crop",
        "provenance": provenance if provenance in _PROVENANCE else "unknown",
        "status": cycle.get("status"),
        "start": start.get("date"),
        "end": end.get("date"),
        "lifecycle": cycle.get("lifecycle") if cycle.get("lifecycle") in ("annual", "perennial") else "unknown",
        "first_campaign": first_campaign(cycle),
        "yield_kg_ha": yield_kg_ha,
        "yield_source": yield_source,
    }
