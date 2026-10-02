# tests/test_advisories_endpoint.py
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1 import graph as graph_mod


def _app():
    app = FastAPI()

    @app.middleware("http")
    async def _verified_identity(request, call_next):
        request.state.tenant_id = "tenant-a"  # what NKZAuthMiddleware sets
        return await call_next(request)

    app.include_router(graph_mod.router, prefix="/api/graph")
    return app

def test_list_advisories_for_parcel():
    rows = [{"id": "urn:ngsi-ld:CropAdvisory:tenant-a:p1:r1:flowering", "operationType": "tillage"}]
    with patch.object(graph_mod, "OrionClient") as MockOrion:
        inst = MockOrion.return_value
        inst.query_entities = AsyncMock(return_value=rows)
        inst.close = AsyncMock()
        c = TestClient(_app())
        r = c.get("/api/graph/agriculture/advisories?parcel_id=urn:ngsi-ld:AgriParcel:tenant-a:p1")
        assert r.status_code == 200
        assert r.json()["advisories"] == rows
        inst.query_entities.assert_awaited_once()
        MockOrion.assert_called_once_with("tenant-a")
