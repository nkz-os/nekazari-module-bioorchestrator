from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from neo4j import AsyncDriver
from app.main import app
import app.api.v1.graph as graph_mod


@pytest.fixture
def client(monkeypatch):
    class _DAO:
        def __init__(self, *a, **k): pass
        async def create_crop_plan(self, parcel_id, season, segments, tenant_id):
            return {"status": "committed", "parcel_id": parcel_id, "season": season,
                    "segments": [f"{parcel_id}:{i}" for i in range(len(segments))], "warnings": []}
        async def get_crop_plan(self, parcel_id, season, tenant_id):
            return {"parcel_id": parcel_id, "season": season, "active": None, "segments": []}
        async def advance_segment(self, parcel_id, season, seq, planting_date, tenant_id):
            return {"status": "advanced", "active": f"{parcel_id}:{seq}", "season": season}
    monkeypatch.setattr(graph_mod, "GraphDAO", _DAO)
    # Routes resolve a real Neo4j driver via DriverDep, but the monkeypatched
    # _DAO above ignores it entirely — stub get_driver so the dependency
    # resolves without requiring a live Neo4j connection (none in CI/test env).
    monkeypatch.setattr("app.core.dependencies.get_driver", lambda: MagicMock(spec=AsyncDriver))
    return TestClient(app)


def test_post_crop_plan_commits(client):
    r = client.post("/api/graph/agriculture/crop-plan", json={
        "parcel_id": "urn:ngsi-ld:AgriParcel:montiko:p-1", "season": "2026",
        "segments": [{"crop": "Vicia sativa", "role": "cover_crop"},
                     {"crop": "Zea mays", "role": "main_crop"}],
    }, headers={"X-Tenant-ID": "montiko"})
    assert r.status_code == 200
    assert r.json()["status"] == "committed" and len(r.json()["segments"]) == 2


def test_post_crop_plan_requires_parcel_and_segments(client):
    r = client.post("/api/graph/agriculture/crop-plan", json={"season": "2026"},
                    headers={"X-Tenant-ID": "montiko"})
    assert r.status_code == 400


def test_advance_requires_planting_date(client):
    r = client.post("/api/graph/agriculture/crop-plan/urn:ngsi-ld:AgriParcel:montiko:p-1/segments/1/advance",
                    json={"season": "2026"}, headers={"X-Tenant-ID": "montiko"})
    assert r.status_code == 400


def test_advance_ok(client):
    r = client.post("/api/graph/agriculture/crop-plan/urn:ngsi-ld:AgriParcel:montiko:p-1/segments/1/advance",
                    json={"planting_date": "2026-04-15", "season": "2026"}, headers={"X-Tenant-ID": "montiko"})
    assert r.status_code == 200 and r.json()["status"] == "advanced"
