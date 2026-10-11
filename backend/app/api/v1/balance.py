"""Public reference data for nutrient-balance plans: crop coefficients and regional yields."""
from __future__ import annotations

import functools
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.v1.attribution import attach_attributions
from app.core.dependencies import get_neo4j_driver
from app.graph.balance_dao import BalanceDao
from app.kg.registries import DEFAULT_REGISTRIES_PATH, Registries, load_registries
from app.services.crop_balance import payload_from_node
from app.services.eurostat_yields import PlausibleRange, yields_payload
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
