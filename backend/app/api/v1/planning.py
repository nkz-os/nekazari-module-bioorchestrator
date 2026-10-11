"""Crop planning endpoints (contract app.planning.models, version CONTRACT_VERSION)."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.api.v1.graph import _require_tenant_id
from app.core.knowledge import KnowledgeSource, get_knowledge_sources
from app.graph.dao import GraphDAO
from app.planning.models import FitResponse, Regime
from app.planning.service import ParcelNotFound, parcel_fit

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
