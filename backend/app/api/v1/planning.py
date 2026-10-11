"""Crop planning endpoints (contract app.planning.models, version CONTRACT_VERSION)."""
from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.api.v1.graph import _require_tenant_id
from app.core.knowledge import KnowledgeSource, get_knowledge_sources
from app.graph.dao import GraphDAO
from app.planning.models import (
    EstimatesResponse,
    FitResponse,
    Management,
    Regime,
    VarietiesResponse,
)
from app.planning.service import (
    ParcelNotFound,
    UnknownCrop,
    parcel_estimates,
    parcel_fit,
    parcel_varieties,
)

router = APIRouter(prefix="/planning", tags=["planning"])
SourcesDep = Annotated[list[KnowledgeSource], Depends(get_knowledge_sources)]
CampaignQ = Query(..., ge=2000, le=2100)


@router.get("/parcels/{parcel_id:path}/fit", response_model=FitResponse)
async def fit(request: Request, parcel_id: str, sources: SourcesDep,
              campaign: int = CampaignQ, regime: Regime | None = None):
    tenant = _require_tenant_id(request)
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
    source = sources[0]
    try:
        return await parcel_estimates(dao=GraphDAO(source.driver), source=source, parcel_id=parcel_id,
                                      tenant_id=tenant, crop=crop, variety=variety, regime=regime,
                                      management=management, sowing_date=sowing_date)
    except UnknownCrop as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except ParcelNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
