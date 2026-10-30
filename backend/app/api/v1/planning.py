"""Crop planning endpoints (contract app.planning.models, version CONTRACT_VERSION)."""
from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Annotated, Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.api.v1.graph import _require_tenant_id
from app.common.attribution import attach_attributions
from app.core.knowledge import KnowledgeSource, get_knowledge_sources
from app.graph.dao import GraphDAO
from app.planning.cover_reference import SOURCE_ID, load_reference_data, reference_for
from app.planning.farm import default_target_campaign, farm_payload
from app.planning.farm_source import load_farm
from app.planning.in_season import parcel_in_season
from app.planning.models import (
    AquacropResponse,
    CalendarResponse,
    EstimatesResponse,
    FarmResponse,
    FitResponse,
    InSeasonResponse,
    Management,
    PlanIn,
    PlanWriteResponse,
    Regime,
    SowingMethod,
    VarietiesResponse,
)
from app.planning.plan_write import PlanError, write_plan
from app.planning.prefetch import schedule_evidence_prefetch
from app.planning.reference import EMPTY_REFERENCE, load_reference
from app.planning.service import (
    CalendarInputError,
    InvalidParcelId,
    ParcelNotFound,
    UnknownCrop,
    parcel_aquacrop,
    parcel_calendar,
    parcel_estimates,
    parcel_fit,
    parcel_urn,
    parcel_varieties,
)
from app.services.sim_jobs import shared_sim_jobs
from app.species_registry import resolve_species

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/planning", tags=["planning"])
SourcesDep = Annotated[list[KnowledgeSource], Depends(get_knowledge_sources)]
CampaignQ = Query(..., ge=2000, le=2100)


def _checked_parcel_urn(raw: str) -> str:
    try:
        return parcel_urn(raw)
    except InvalidParcelId as e:
        raise HTTPException(status_code=422, detail="invalid parcel id") from e


@router.get("/parcels/{parcel_id:path}/fit", response_model=FitResponse)
async def fit(request: Request, parcel_id: str, sources: SourcesDep,
              campaign: int = CampaignQ, regime: Regime | None = None):
    tenant = _require_tenant_id(request)
    parcel_id = _checked_parcel_urn(parcel_id)
    source = sources[0]
    dao = GraphDAO(source.driver)
    try:
        result = await parcel_fit(dao=dao, source=source, parcel_id=parcel_id, tenant_id=tenant,
                                  campaign=campaign, regime=regime)
    except ParcelNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    # While the farmer reads the list, warm the evidence of the first crops (varieties and estimates).
    schedule_evidence_prefetch(dao, parcel_id=result["parcel_id"], tenant_id=tenant,
                               regime=result["facts"]["regime"], species=result["species"])
    return result


@router.get("/parcels/{parcel_id:path}/varieties", response_model=VarietiesResponse)
async def varieties(request: Request, parcel_id: str, sources: SourcesDep, regime: Regime, management: Management,
                    crop: str = Query(..., min_length=2, max_length=64)):
    tenant = _require_tenant_id(request)
    parcel_id = _checked_parcel_urn(parcel_id)
    source = sources[0]
    try:
        return await parcel_varieties(dao=GraphDAO(source.driver), source=source, parcel_id=parcel_id,
                                      tenant_id=tenant, crop=crop, regime=regime, management=management)
    except UnknownCrop as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except ParcelNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/parcels/{parcel_id:path}/estimates", response_model=EstimatesResponse)
async def estimates(request: Request, parcel_id: str, sources: SourcesDep, regime: Regime, management: Management,
                    crop: str = Query(..., min_length=2, max_length=64),
                    variety: str | None = Query(None, max_length=64), sowing_date: date | None = None):
    tenant = _require_tenant_id(request)
    parcel_id = _checked_parcel_urn(parcel_id)
    source = sources[0]
    try:
        return await parcel_estimates(dao=GraphDAO(source.driver), source=source, parcel_id=parcel_id,
                                      tenant_id=tenant, crop=crop, variety=variety, regime=regime,
                                      management=management, sowing_date=sowing_date)
    except UnknownCrop as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except ParcelNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/parcels/{parcel_id:path}/estimates/aquacrop", response_model=AquacropResponse)
async def estimates_aquacrop(request: Request, parcel_id: str, sources: SourcesDep, regime: Regime,
                             crop: str = Query(..., min_length=2, max_length=64),
                             sowing_date: date | None = None,
                             retry: bool = Query(False), job_id: str | None = Query(None, max_length=64),
                             sowing_method: SowingMethod | None = None):
    """Asynchronous AquaCrop estimates: starts or joins the simulation job; poll until it settles.

    The engine takes neither management nor variety, so neither is a parameter here.
    ``sowing_method`` 'direct' simulates the stubble mulch; omitted, the recorded sowing decides,
    else bare soil.
    ``retry=true`` relaunches a failed (or lost) job; polling never does. ``job_id`` (from the
    previous answer) makes a poll read the job directly, skipping the parcel lookup.
    """
    tenant = _require_tenant_id(request)
    parcel_id = _checked_parcel_urn(parcel_id)
    try:
        return await parcel_aquacrop(dao=GraphDAO(sources[0].driver), jobs=shared_sim_jobs(), tenant_id=tenant,
                                     parcel_id=parcel_id, crop=crop, regime=regime, sowing_date=sowing_date,
                                     retry=retry, job_id=job_id, sowing_method=sowing_method)
    except UnknownCrop as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except ParcelNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/parcels/{parcel_id:path}/estimates/in-season", response_model=InSeasonResponse)
async def estimates_in_season(request: Request, parcel_id: str, sources: SourcesDep,
                              retry: bool = Query(False), job_id: str | None = Query(None, max_length=64)):
    """In-season AquaCrop forecast of a sown parcel (F5, contract 2.1); poll until it settles.

    Crop, sowing (a fact, never a plan) and regime come from the parcel's current cycle.
    ``job_id`` is accepted for symmetry with ``/estimates/aquacrop``; the job is always found
    from the resolved inputs. ``retry=true`` relaunches a failed job.
    """
    tenant = _require_tenant_id(request)
    parcel_id = _checked_parcel_urn(parcel_id)
    try:
        return await parcel_in_season(dao=GraphDAO(sources[0].driver), jobs=shared_sim_jobs(),
                                      tenant_id=tenant, parcel_id=parcel_id, retry=retry)
    except ParcelNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/parcels/{parcel_id:path}/calendar", response_model=CalendarResponse)
async def calendar(request: Request, parcel_id: str, sources: SourcesDep, campaign: int = CampaignQ,
                   crop: str = Query(..., min_length=2, max_length=64),
                   cover: str | None = Query(None, min_length=2, max_length=64),
                   main_sowing: date | None = None, regime: Regime | None = None):
    """Fortnights for a plan: the main sowing window and a cover coherent with it (spec W6)."""
    tenant = _require_tenant_id(request)
    parcel_id = _checked_parcel_urn(parcel_id)
    try:
        return await parcel_calendar(dao=GraphDAO(sources[0].driver), parcel_id=parcel_id, tenant_id=tenant,
                                     campaign=campaign, crop=crop, cover=cover, main_sowing=main_sowing,
                                     regime=regime)
    except UnknownCrop as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except CalendarInputError as e:
        raise HTTPException(status_code=422, detail={"code": str(e)}) from e
    except ParcelNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/farm", response_model=FarmResponse)
async def farm(request: Request, sources: SourcesDep,
               campaign: int | None = Query(None, ge=2000, le=2100),
               parcel_id: str | None = Query(None, max_length=256)):
    tenant = _require_tenant_id(request)
    if parcel_id is not None:
        parcel_id = _checked_parcel_urn(parcel_id)
    target = campaign or default_target_campaign(datetime.now(UTC).date())
    try:
        parcels, farms, crops, cycles = await load_farm(tenant, parcel_id)
    except httpx.HTTPStatusError as e:
        if parcel_id is not None and e.response.status_code == 404:
            raise HTTPException(status_code=404, detail="parcel not found") from e
        raise
    except httpx.ConnectError as e:
        raise HTTPException(status_code=502, detail="Orion-LD unreachable") from e
    try:
        constraints = (await load_reference(sources[0])).constraints
    except Exception:
        # A failing step must not take the page down: no rotation rules reads as "rotation unknown".
        logger.exception("planning farm: reference load failed graph=%s tenant=%s", sources[0].graph_id, tenant)
        constraints = EMPTY_REFERENCE.constraints
    return farm_payload(parcels, farms, crops, cycles, target, constraints)


@router.put("/parcels/{parcel_id:path}/plan", response_model=PlanWriteResponse)
async def put_plan(request: Request, parcel_id: str, body: PlanIn):
    tenant = _require_tenant_id(request)
    parcel_id = _checked_parcel_urn(parcel_id)
    try:
        return await write_plan(tenant_id=tenant, parcel_urn=parcel_id, plan=body, today=datetime.now(UTC).date())
    except PlanError as e:
        raise HTTPException(status_code=e.status, detail={"code": e.code}) from e
    except httpx.ConnectError as e:
        raise HTTPException(status_code=502, detail="Orion-LD unreachable") from e


@router.get("/cover-crops/{species}/reference")
async def cover_crop_reference(request: Request, species: str, season: Literal["autumn", "spring"]):
    """Aerial biomass, %N and C/N of a cover species (mean of European trials); ``species`` is a slug or EPPO code.

    An identifier the registry does not know is a 404; a known species without a row for the season is
    ``status: "no_data"`` (never another season or species).
    """
    _require_tenant_id(request)
    slug = resolve_species(species)
    if slug is None:
        raise HTTPException(status_code=404, detail="unknown species")
    return attach_attributions(reference_for(slug, season, load_reference_data()), [SOURCE_ID])
