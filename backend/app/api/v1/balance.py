"""Public reference data for nutrient-balance plans: crop coefficients and regional yields."""
from __future__ import annotations

import functools
import logging
import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.v1.attribution import attach_attributions
from app.core.dependencies import get_neo4j_driver
from app.graph.balance_dao import BalanceDao
from app.kg.registries import DEFAULT_REGISTRIES_PATH, Registries, load_registries
from app.services.crop_balance import payload_from_node
from app.services.eurostat_yields import PlausibleRange, yields_payload
from app.services.nuts_regions import (
    is_superseded,
    load_province_table,
    load_region_names,
    nuts2_for_municipality,
)
from app.services.region_lookup import es_province_at
from app.species_registry import catalog_code
from neo4j import AsyncDriver

logger = logging.getLogger(__name__)
router = APIRouter()

DriverDep = Annotated[AsyncDriver, Depends(get_neo4j_driver)]
_EPPO = r"^[A-Za-z0-9]{5,6}$"
_NUTS2 = r"^[A-Za-z]{2}[A-Za-z0-9]{2}$"


@functools.lru_cache(maxsize=1)
def _registries() -> Registries:
    return load_registries(DEFAULT_REGISTRIES_PATH)


def _plausible_range(eppo: str) -> PlausibleRange | None:
    """The grain-yield envelope of a crop from the BioOrch ranges, in t/ha; None when it has none.

    The registry states kg/ha; any other unit is not converted (no guessing) and gives no range.
    """
    found = _registries().range_for(eppo, "crop_yield", {"purpose": "grain"})
    if found is None:
        return None
    if found.unit != "kg/ha":
        logger.warning("yield range %s has unit %r, expected kg/ha: no plausibility check",
                       found.id, found.unit)
        return None
    return PlausibleRange(min_t_ha=found.min / 1000, max_t_ha=found.max / 1000,
                          range_id=found.id, status=found.status)


@router.get("/agriculture/crop-balance-coefficients")
async def crop_balance_coefficients(driver: DriverDep, eppo: str = Query(..., pattern=_EPPO)):
    """N-P-K balance coefficients of a crop, each with its source, reference and transformation."""
    node = await BalanceDao(driver).get_crop_coefficients(catalog_code(eppo.strip().upper()))
    if node is None:
        raise HTTPException(status_code=404, detail="No balance coefficients for this crop")
    body = payload_from_node(node)
    return attach_attributions(body, body["source_ids"])


@router.get("/agriculture/regional-yields")
async def regional_yields(
    driver: DriverDep,
    eppo: str = Query(..., pattern=_EPPO),
    nuts: str = Query(..., pattern=_NUTS2),
    recent: int = Query(5, ge=1, le=25),
):
    """Annual Eurostat series (EU standard humidity) and the median of the recent years.

    Where the crop has a BioOrch grain-yield range, each year is checked against it: a year above
    the maximum is flagged and leaves the median, a year below the minimum is flagged and stays.
    The ranges are at the source's moisture basis (cereals around 13%) while Eurostat uses the EU
    standard humidity (14%); the difference is negligible against envelopes this wide.
    """
    stored_eppo = catalog_code(eppo.strip().upper())
    rows = await BalanceDao(driver).get_regional_yields(stored_eppo, nuts.strip().upper())
    if not rows:
        raise HTTPException(status_code=404, detail="No regional yields for this crop and region")
    body = yields_payload(rows, recent, _plausible_range(stored_eppo))
    return attach_attributions(body, (r.get("sourceId") for r in rows))


_INE = r"^[0-9]{5}$"
_COUNTRY = r"^[A-Za-z]{2}$"


@functools.lru_cache(maxsize=1)
def _province_table() -> dict[str, str]:
    return load_province_table()


_REGION_TTL_S = 600.0
_REGION_CACHE: dict[str, tuple[float, list[dict]]] = {}
_monotonic = time.monotonic


def _names_or_none() -> dict[str, str] | None:
    """Region names, or None (logged) when the snapshot cannot be read: names are decoration."""
    try:
        return load_region_names()
    except Exception as exc:  # noqa: BLE001
        logger.error("NUTS region names unavailable: %s: %s", type(exc).__name__, exc)
        return None


def warm_nuts_reference() -> None:
    """Load the province table and region names at startup; never raises."""
    try:
        _province_table()
    except Exception as exc:  # noqa: BLE001
        logger.error("NUTS province table unavailable: %s: %s", type(exc).__name__, exc)
    _names_or_none()


@router.get("/agriculture/nuts-region")
async def nuts_region(
    municipality: str | None = Query(None, pattern=_INE),
    lat: float | None = Query(None, ge=-90, le=90),
    lon: float | None = Query(None, ge=-180, le=180),
):
    """NUTS 2 region of a Spanish municipality (INE code) or of a point, through its province.

    Exactly one locator: `municipality`, or `lat` with `lon`. No GISCO geometry: the point case
    uses the public-domain province boundaries. The municipality's existence is not checked.
    """
    has_point = lat is not None or lon is not None
    if (municipality is not None) == has_point or (has_point and (lat is None or lon is None)):
        raise HTTPException(status_code=422,
                            detail="Give either municipality or both lat and lon")
    if municipality is not None:
        nuts = nuts2_for_municipality(municipality, _province_table())
        if nuts is None:
            raise HTTPException(status_code=404, detail="Unknown province")
        names = _names_or_none() or {}
        return {"municipality": municipality, "country": "ES", "nuts": nuts,
                "name": names.get(nuts), "nuts_version": 2024,
                "method": "ine_province"}
    province = es_province_at(lat, lon)
    nuts = _province_table().get(province["code"]) if province else None
    if nuts is None:
        raise HTTPException(status_code=404, detail="No NUTS 2 region for this point")
    names = _names_or_none() or {}
    return {"lat": lat, "lon": lon, "country": "ES", "nuts": nuts,
            "name": names.get(nuts), "nuts_version": 2024,
            "method": "natural_earth_province"}


@router.get("/agriculture/nuts-regions")
async def nuts_regions(driver: DriverDep, country: str = Query(..., pattern=_COUNTRY)):
    """Current NUTS 2 regions of a country that have Eurostat yields, for a manual pick.

    Codes Eurostat labels as belonging to another NUTS version are left out; `latest_year` is the
    newest year with a yield. Cached in-process for 10 minutes per country.
    """
    country = country.upper()
    now = _monotonic()
    hit = _REGION_CACHE.get(country)
    if hit is not None and now - hit[0] < _REGION_TTL_S:
        regions = hit[1]
    else:
        names = _names_or_none()
        if names is None:
            raise HTTPException(status_code=503, detail="Region names unavailable")
        rows = await BalanceDao(driver).list_yield_regions(country)
        regions = [{"nuts": r["nuts"], "name": names[r["nuts"]], "eppos": r["eppos"],
                    "latest_year": r.get("latest_year")}
                   for r in rows if r["nuts"] in names and not is_superseded(names[r["nuts"]])]
        _REGION_CACHE[country] = (now, regions)
    if not regions:
        raise HTTPException(status_code=404, detail="No regions with yields")
    return attach_attributions({"country": country, "regions": regions},
                               ["eurostat_apro_cpshr"])
