"""IUCN Red List API Endpoints — species assessments proxy.

Protected by NKZAuthMiddleware.
"""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.services.iucn_service import IucnAPIError, IucnClient, get_iucn_client

router = APIRouter()


# ── Species (specific routes first, catch-all /{species_name} LAST) ─────────

@router.get("/species/page/{page}")
async def get_species_page(
    page: int, client: IucnClient = Depends(get_iucn_client)
) -> Any:
    """Get a paginated list of species assessments."""
    try:
        return await client.get_species_page(page=page)
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/species/id/{species_id}")
async def get_species_by_id(
    species_id: int, client: IucnClient = Depends(get_iucn_client)
) -> Any:
    """Get assessment information for a species by IUCN ID."""
    try:
        return await client.get_species_by_id(species_id=species_id)
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/species/region/{region_code}")
async def get_species_by_region(
    region_code: str,
    page: int = Query(0),
    client: IucnClient = Depends(get_iucn_client),
) -> Any:
    """Get species assessments for a marine region."""
    try:
        return await client.get_species_by_region(
            region_code=region_code, page=page
        )
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/species/category/{category_code}")
async def get_species_by_category(
    category_code: str, client: IucnClient = Depends(get_iucn_client)
) -> Any:
    """Get species by IUCN Red List category (CR, EN, VU, NT, LC, DD)."""
    try:
        return await client.get_species_by_category(category_code=category_code)
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/species/{species_name:path}")
async def get_species_by_name(
    species_name: str, client: IucnClient = Depends(get_iucn_client)
) -> Any:
    """Get assessment information for a species by scientific name.

    MUST be registered last among /species/* routes — :path captures all segments.
    """
    try:
        return await client.get_species_by_name(species_name=species_name)
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


# ── Assessments ────────────────────────────────────────────────────────────

@router.get("/assessment/{assessment_id}")
async def get_assessment(
    assessment_id: int, client: IucnClient = Depends(get_iucn_client)
) -> Any:
    """Get detailed information for a specific assessment."""
    try:
        return await client.get_assessment(assessment_id=assessment_id)
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


# ── Country ────────────────────────────────────────────────────────────────

@router.get("/country/{country_code}")
async def get_country_species(
    country_code: str, client: IucnClient = Depends(get_iucn_client)
) -> Any:
    """Get the list of species for a country (ISO alpha-2 code)."""
    try:
        return await client.get_country_species(country_code=country_code)
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


# ── Reference data ─────────────────────────────────────────────────────────

@router.get("/threats")
async def get_threats(client: IucnClient = Depends(get_iucn_client)) -> Any:
    """Get the full list of IUCN threat types."""
    try:
        return await client.get_threats()
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/habitats")
async def get_habitats(client: IucnClient = Depends(get_iucn_client)) -> Any:
    """Get the full list of IUCN habitat types."""
    try:
        return await client.get_habitats()
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/conservation-measures")
async def get_conservation_measures(
    client: IucnClient = Depends(get_iucn_client),
) -> Any:
    """Get the full list of IUCN conservation measures."""
    try:
        return await client.get_conservation_measures()
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


# ── Conservation measures (specific first, catch-all LAST) ──────────────────

@router.get("/measures/species/id/{species_id}")
async def get_measures_for_species_id(
    species_id: int, client: IucnClient = Depends(get_iucn_client)
) -> Any:
    """Get conservation measures for a species by IUCN ID."""
    try:
        return await client.get_measures_for_species_id(species_id=species_id)
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/measures/species/{species_name:path}")
async def get_measures_for_species_name(
    species_name: str, client: IucnClient = Depends(get_iucn_client)
) -> Any:
    """Get conservation measures for a species by scientific name.

    MUST be registered after /measures/species/id/{id} — :path captures all segments.
    """
    try:
        return await client.get_measures_for_species_name(species_name=species_name)
    except IucnAPIError as e:
        raise HTTPException(status_code=502, detail=str(e))
