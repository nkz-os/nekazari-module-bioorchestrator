from fastapi import APIRouter

from app.api.v1.graph import router as graph_router
from app.api.dadis import router as dadis_router
from app.api.iucn import router as iucn_router

router = APIRouter()
router.include_router(graph_router, prefix="/graph", tags=["graph"])
router.include_router(dadis_router, prefix="/dadis", tags=["dadis"])
router.include_router(iucn_router, prefix="/iucn", tags=["iucn"])
