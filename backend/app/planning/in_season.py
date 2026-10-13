"""In-season forecast inputs (F5): the crop of the parcel's current cycle, a sowing that is a
fact (``actual`` or ``declared``, never a plan), its regime and country; and the gate.

Pure: the caller loads the parcel, its crops and the core's crop-cycle timeline. The core
already folds completed Field-Operations sowings into the cycle start, so they are not read
again here; when the core does not answer the reason is ``cycles_unavailable``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from app.graph.dao import _geometry_centroid
from app.planning.aquacrop_provider import VALIDATED
from app.planning.farm import parcel_regime
from app.services.country_lookup import country_at
from app.species_registry import catalog_code, get_species_info, resolve_species

MAX_SEASON_DAYS = 366
FACT = ("actual", "declared")


@dataclass(frozen=True)
class InSeasonInputs:
    crop_slug: str
    eppo: str
    regime: str
    regime_source: str
    country: str | None
    sowing_date: date
    sowing_provenance: str


def _country(parcel: dict) -> str | None:
    location = parcel.get("location")
    geometry = location.get("value", location) if isinstance(location, dict) else None
    point = _geometry_centroid(geometry.get("coordinates")) if isinstance(geometry, dict) else None
    if point is None:
        return None
    lon, lat = point
    return country_at(lat, lon)


def resolve_in_season(parcel: dict, crops: dict[str, dict], cycles: dict | None,
                      today: date) -> InSeasonInputs | str:
    """The inputs of an in-season forecast, or the reason code why there is none."""
    if cycles is None:
        return "cycles_unavailable"
    current = cycles.get("current")
    if not current:
        return "not_sown" if cycles.get("pending") else "no_committed_crop"
    start = current.get("start") or {}
    if start.get("provenance") not in FACT:
        return "not_sown"
    if not start.get("date"):
        return "no_sowing_date"
    sowing = date.fromisoformat(str(start["date"])[:10])
    crop = crops.get(current.get("crop_id")) or {}
    slug = resolve_species(str(current.get("species") or crop.get("species") or ""))
    eppo = (get_species_info(slug) or {}).get("eppo_code") if slug else None
    if not eppo:
        return "unknown_crop"
    eppo = catalog_code(eppo)
    regime, regime_source = parcel_regime(parcel, [crop] if crop else [])
    if regime is None:
        return "no_regime"
    country = _country(parcel)
    if (eppo, regime, country) not in VALIDATED:
        return "not_validated"
    if sowing > today:
        return "sowing_in_future"
    if sowing < today - timedelta(days=MAX_SEASON_DAYS):
        return "season_too_old"
    return InSeasonInputs(crop_slug=slug, eppo=eppo, regime=regime, regime_source=regime_source,
                          country=country, sowing_date=sowing, sowing_provenance=start["provenance"])
