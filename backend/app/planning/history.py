"""Crop history and plan cells from the platform's crop-cycles answer (pure).

An annual cycle occupies the campaign it ends in (the platform rule). A herbaceous perennial
(alfalfa) occupies every campaign from the one its sowing opens to the one its end closes,
or up to the campaign being looked at while it has no end yet.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from app.planning.rotation import PastCrop
from app.species_registry import get_species_info, resolve_species

_PROVENANCE = ("actual", "declared", "planned")
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


def past_crops(payload: dict | None, before_campaign: int) -> list[PastCrop]:
    out = []
    for cycle in cycles_of(payload):
        slug = species_slug(cycle.get("species"))
        if not slug:
            continue
        for campaign in cycle_campaigns(cycle, until=before_campaign - 1):
            if campaign < before_campaign:
                out.append(PastCrop(slug, campaign))
    return out


def cell_from_cycle(cycle: dict, crop: dict | None) -> dict:
    slug = species_slug(cycle.get("species"))
    info = get_species_info(slug) if slug else None
    start, end = cycle.get("start") or {}, cycle.get("end") or {}
    provenance = start.get("provenance")
    return {
        "crop_id": cycle.get("crop_id"),
        "species": slug,
        "species_raw": cycle.get("species"),
        "names": dict((info or {}).get("common_names") or {}),
        "variety": plain((crop or {}).get("variety")),
        "role": cycle.get("role") or "main_crop",
        "provenance": provenance if provenance in _PROVENANCE else "unknown",
        "status": cycle.get("status"),
        "start": start.get("date"),
        "end": end.get("date"),
        "lifecycle": cycle.get("lifecycle") if cycle.get("lifecycle") in ("annual", "perennial") else "unknown",
        "first_campaign": first_campaign(cycle),
    }
