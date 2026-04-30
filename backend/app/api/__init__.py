from fastapi import APIRouter

from app.api.v1.graph import router as graph_router
from app.api.dadis import router as dadis_router

router = APIRouter()
router.include_router(graph_router, prefix="/graph", tags=["graph"])
router.include_router(dadis_router, prefix="/dadis", tags=["dadis"])
