"""Crop planning endpoints (contract app.planning.models, version CONTRACT_VERSION)."""
from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.api.v1.graph import _require_tenant_id
from app.core.knowledge import KnowledgeSource, get_knowledge_sources
from app.graph.dao import GraphDAO
from app.planning.farm import default_target_campaign, farm_payload
from app.planning.farm_source import load_farm
from app.planning.models import (
    EstimatesResponse,
    FarmResponse,
    FitResponse,
    Management,
    PlanIn,
    PlanWriteResponse,
    Regime,
    VarietiesResponse,
)
from app.planning.plan_write import PlanError, write_plan
from app.planning.reference import EMPTY_REFERENCE, load_reference
from app.planning.service import (
    InvalidParcelId,
    ParcelNotFound,
    UnknownCrop,
    parcel_estimates,
    parcel_fit,
    parcel_urn,
    parcel_varieties,
)

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
    try:
        return await parcel_fit(dao=GraphDAO(source.driver), source=source, parcel_id=parcel_id,
                                tenant_id=tenant, campaign=campaign, regime=regime)
    except ParcelNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


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
