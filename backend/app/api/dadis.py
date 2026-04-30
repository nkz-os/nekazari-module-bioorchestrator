"""DAD-IS API Endpoints — FAO breed database proxy.

Protected by NKZAuthMiddleware.
"""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.services.dadis_service import DadisAPIError, DadisClient, get_dadis_client

router = APIRouter()


class BreedsFilter(BaseModel):
    classification: str = "all"
    countryIds: list[str] | None = None
    speciesIds: list[int] | None = None


@router.post("/breeds")
async def get_breeds(
    filters: BreedsFilter, client: DadisClient = Depends(get_dadis_client)
) -> Any:
    """Get breeds from DAD-IS based on filters."""
    try:
        return await client.get_breeds(
            classification=filters.classification,
            country_ids=filters.countryIds,
            species_ids=filters.speciesIds,
        )
    except DadisAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/breeds/{breed_id}")
async def get_breed_by_id(
    breed_id: str,
    lang: str = Query("en"),
    client: DadisClient = Depends(get_dadis_client),
) -> Any:
    """Get details of a specific breed."""
    try:
        return await client.get_breed_by_id(breed_id=breed_id, lang=lang)
    except DadisAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/countries")
async def get_countries(client: DadisClient = Depends(get_dadis_client)) -> Any:
    """Get all available countries from DAD-IS."""
    try:
        return await client.get_countries()
    except DadisAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/species")
async def get_species(client: DadisClient = Depends(get_dadis_client)) -> Any:
    """Get all available species from DAD-IS."""
    try:
        return await client.get_species()
    except DadisAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))
