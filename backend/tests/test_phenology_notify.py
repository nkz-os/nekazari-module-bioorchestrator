# tests/test_phenology_notify.py
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.api.v1 import phenology_notify

@pytest.fixture(autouse=True)
def _reset():
    phenology_notify._LAST_STAGE.clear()

def _app():
    app = FastAPI()
    app.include_router(phenology_notify.router, prefix="/api/graph/internal")
    return app

def _notif(stage="flowering"):
    return {"data": [{
        "id": "urn:ngsi-ld:CropHealthAssessment:montiko:p1", "type": "CropHealthAssessment",
        "hasAgriParcel": {"type": "Relationship", "object": "urn:ngsi-ld:AgriParcel:montiko:p1"},
        "phenologyStage": {"type": "Property", "value": stage},
    }]}

def test_dispatches_on_stage_change():
    with patch.object(phenology_notify, "_dispatch") as disp:
        c = TestClient(_app())
        r = c.post("/api/graph/internal/phenology-update", json=_notif(),
                   headers={"NGSILD-Tenant": "montiko"})
        assert r.status_code == 200 and r.json()["queued"] == 1
        disp.assert_called_once_with(
            "montiko", "urn:ngsi-ld:AgriParcel:montiko:p1",
            {"phenology.current_stage": "flowering"})

def test_dedup_skips_unchanged_stage():
    with patch.object(phenology_notify, "_dispatch") as disp:
        c = TestClient(_app())
        h = {"NGSILD-Tenant": "montiko"}
        c.post("/api/graph/internal/phenology-update", json=_notif("flowering"), headers=h)
        r2 = c.post("/api/graph/internal/phenology-update", json=_notif("flowering"), headers=h)
        assert r2.json()["queued"] == 0
        assert disp.call_count == 1

def test_invalid_payload_400():
    c = TestClient(_app())
    r = c.post("/api/graph/internal/phenology-update", json={"nope": 1},
               headers={"NGSILD-Tenant": "montiko"})
    assert r.status_code == 400
