"""TrialSite geo resolution for prod backfill (Track B wave 1a).

Loads ``backend/data/trial_site_geo_registry.json`` — built from CHELSA-enriched
JSON-LD plus municipality geocoding for sites missing coordinates in Neo4j.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from app.ingestion.normalization_registry import normalize_location

_REGISTRY_PATH = Path(__file__).resolve().parents[2] / "data" / "trial_site_geo_registry.json"

AGGREGATE_PATTERNS = (
    "average",
    "media ",
    "múltiples",
    "multiple",
    "országos",
    "magyarország",
    "national",
    "mean of",
    "zones)",
    "bassin",
    "castilla y león",
    "andalucía",
    "portugal",
    "france",
    "poland",
    "spain (",
    "hungary (",
    "unknown",
    "not specified",
    "uk national",
    "átlag",
    "gotland",
    "halland",
    "skåne",
    "småland",
    "östergötland",
    "debube",
    "békési sík",
)


def is_aggregate_site_name(name: str | None) -> bool:
    if not name:
        return True
    low = name.strip().lower()
    return any(p in low for p in AGGREGATE_PATTERNS)


@lru_cache(maxsize=1)
def load_geo_registry() -> dict:
    if not _REGISTRY_PATH.is_file():
        raise FileNotFoundError(f"Missing trial site geo registry: {_REGISTRY_PATH}")
    return json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))


def resolve_trial_site_geo(name: str | None) -> dict | None:
    """Return geo patch dict for a TrialSite ``name``, or None if not enrichable."""
    if not name or is_aggregate_site_name(name):
        return None

    data = load_geo_registry()
    sites: dict = data.get("sites", {})
    key = name.strip().lower()
    if key in sites:
        return dict(sites[key])

    loc = normalize_location(name)
    if loc:
        canonical = (loc.get("name") or "").strip().lower()
        if canonical in sites:
            entry = dict(sites[canonical])
            if not entry.get("climateClass") and loc.get("climateClass"):
                entry["climateClass"] = loc["climateClass"]
            return entry

    return None


def geo_updates_for_neo4j(entry: dict) -> dict:
    """Map registry entry to Neo4j SET properties."""
    out: dict = {
        "latitude": entry.get("latitude"),
        "longitude": entry.get("longitude"),
        "geoConfidence": entry.get("geoConfidence"),
        "geoSource": entry.get("geoSource"),
    }
    if entry.get("climateClass"):
        out["climateClass"] = entry["climateClass"]
    for field in (
        "elevationM",
        "annualRainfallMm",
        "annualET0Mm",
        "frostDaysPerYear",
    ):
        if entry.get(field) is not None:
            out[field] = entry[field]
    return {k: v for k, v in out.items() if v is not None}
