"""Public reference data for nutrient-balance plans: crop coefficients and regional yields."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.v1.attribution import attach_attributions
from app.core.dependencies import get_neo4j_driver
from app.graph.balance_dao import BalanceDao
from app.services.crop_balance import payload_from_node
from app.services.eurostat_yields import yields_payload
from app.species_registry import catalog_code
from neo4j import AsyncDriver

router = APIRouter()

DriverDep = Annotated[AsyncDriver, Depends(get_neo4j_driver)]
_EPPO = r"^[A-Za-z0-9]{5,6}$"
_NUTS2 = r"^[A-Za-z]{2}[A-Za-z0-9]{2}$"


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
    """Annual Eurostat series (EU standard humidity) and the median of the recent years."""
    rows = await BalanceDao(driver).get_regional_yields(
        catalog_code(eppo.strip().upper()), nuts.strip().upper())
    if not rows:
        raise HTTPException(status_code=404, detail="No regional yields for this crop and region")
    return attach_attributions(yields_payload(rows, recent), (r.get("sourceId") for r in rows))
