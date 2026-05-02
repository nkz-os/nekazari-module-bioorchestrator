from fastapi import APIRouter

from app.api.v1.graph import router as graph_router
from app.api.dadis import router as dadis_router

# IUCN router removed: IUCN Red List API license prohibits commercial use.
# NKZ-OS operates as SaaS multi-tenant under paid plans → commercial use.
# See: https://www.iucnredlist.org/terms/terms-of-use (Section 4)
# Alternatives: GBIF with CC0+CC-BY filter, iNaturalist via GBIF.

router = APIRouter()
router.include_router(graph_router, prefix="/graph", tags=["graph"])
router.include_router(dadis_router, prefix="/dadis", tags=["dadis"])
