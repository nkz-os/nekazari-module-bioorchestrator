# tests/test_advisories_endpoint.py
import pytest
from unittest.mock import AsyncMock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.api.v1 import graph as graph_mod

def _app():
    app = FastAPI()
    app.include_router(graph_mod.router, prefix="/api/graph")
    return app

def test_list_advisories_for_parcel():
    rows = [{"id": "urn:ngsi-ld:CropAdvisory:montiko:p1:r1:flowering", "operationType": "tillage"}]
    with patch.object(graph_mod, "OrionClient") as MockOrion:
        inst = MockOrion.return_value
        inst.query_entities = AsyncMock(return_value=rows)
        inst.close = AsyncMock()
        c = TestClient(_app())
        r = c.get("/api/graph/agriculture/advisories?parcel_id=urn:ngsi-ld:AgriParcel:montiko:p1",
                  headers={"X-Tenant-ID": "montiko"})
        assert r.status_code == 200
        assert r.json()["advisories"] == rows
        inst.query_entities.assert_awaited_once()
