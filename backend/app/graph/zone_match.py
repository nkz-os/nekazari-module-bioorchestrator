"""Parcel zone matching of the regional evidence tier (no I/O).

A Spanish parcel is placed in GENVCE's own climatic zones (``app.kg.zone_definitions``) from its CHELSA
v2.1 1981-2010 cell: April mean air temperature (``monthly_tas_c[3]``) and annual precipitation
(``annual_rainfall_mm``, the sum of the monthly normals), with the cell's own unit conversions. The
result is a :class:`ZoneContext`; the DAO uses its allow/deny keys as the zone pool of the regional
queries, and ``zone_match_block`` is what the response says about it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.kg.zone_definitions import (
    ZONE_MATCH_BASIS,
    ZONE_MATCH_CAVEAT,
    ParcelZones,
    ZoneDefinitions,
    default_zone_definitions,
)

POOL_MATCHED = "matched"
POOL_FALLBACK = "fallback"
ZONE_COUNTRY = "ES"  # GENVCE is the Spanish network: its zone definitions apply to Spanish parcels only

STATUS_MATCHED = "matched"
STATUS_COUNTRY_LEVEL = "country_level"
STATUS_UNAVAILABLE = "unavailable"

_APRIL = 3  # index of April in the monthly normals (Jan..Dec)


@dataclass(frozen=True)
class ZoneContext:
    """What the zone pools need. ``ready`` False: the parcel climate was not available (``reason``)."""

    ready: bool
    reason: str | None = None
    allow: tuple[str, ...] = ()
    deny: tuple[str, ...] = ()
    zones: ParcelZones | None = None
    cell: str | None = None
    april_tas_c: float | None = None
    annual_rainfall_mm: float | None = None


def applies(country: str | None, lat: Any, lon: Any) -> bool:
    """Zone matching is asked for: a Spanish parcel with a point."""
    return (country or "").strip().upper() == ZONE_COUNTRY and lat is not None and lon is not None


def normalise_regime(irrigation_regime: str | None) -> str | None:
    text = (irrigation_regime or "").strip().casefold()
    if text.startswith("secano"):
        return "secano"
    if text.startswith("regad"):
        return "regadio"
    return None


def build_context(cell: dict | None, irrigation_regime: str | None, cell_key: str | None = None,
                  definitions: ZoneDefinitions | None = None) -> ZoneContext:
    """Zone context from a CHELSA cell (None, or a cell without April / rainfall: not ready)."""
    if not cell:
        return ZoneContext(False, "parcel_climate_unavailable")
    monthly = cell.get("monthly_tas_c")
    april = monthly[_APRIL] if isinstance(monthly, (list, tuple)) and len(monthly) == 12 else None
    rain = cell.get("annual_rainfall_mm")
    if april is None or rain is None:
        return ZoneContext(False, "parcel_climate_incomplete", cell=cell_key)
    zones = (definitions or default_zone_definitions()).classify_parcel(
        april_tas_c=float(april), annual_rain_mm=float(rain), regime=normalise_regime(irrigation_regime))
    return ZoneContext(True, None, tuple(sorted(zones.allow)), tuple(sorted(zones.deny)), zones, cell_key,
                       float(april), float(rain))


def zone_match_block(ctx: ZoneContext | None, status: str, matched_keys: list[str] | None = None,
                     definitions: ZoneDefinitions | None = None) -> dict | None:
    """The ``evidence.zone_match`` object of a regional recommendation (None: zone matching not asked)."""
    if ctx is None:
        return None
    block: dict[str, Any] = {
        "status": status,
        "basis": ZONE_MATCH_BASIS,
        "caveat": ZONE_MATCH_CAVEAT,
        "reason": None,
        "parcel": None,
        "matched_zones": [],
    }
    if not ctx.ready:
        block["status"] = STATUS_UNAVAILABLE
        block["reason"] = ctx.reason
        return block
    block["parcel"] = {"april_mean_temp_c": round(ctx.april_tas_c, 2) if ctx.april_tas_c is not None else None,
                       "annual_rainfall_mm": round(ctx.annual_rainfall_mm) if ctx.annual_rainfall_mm is not None else None}
    if status == STATUS_MATCHED:
        defs = definitions or default_zone_definitions()
        block["matched_zones"] = defs.describe_keys(matched_keys or [])
    else:
        block["reason"] = "no_zone_matched_evidence_for_crop"
    return block
