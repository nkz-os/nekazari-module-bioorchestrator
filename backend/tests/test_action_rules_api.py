import pytest
from fastapi.testclient import TestClient
from app.main import app
import app.api.v1.graph as graph_mod
from app.auth import SKIP_AUTH_PREFIXES


@pytest.fixture
def client(monkeypatch):
    class _DAO:
        def __init__(self, *a, **k): pass
        async def get_action_rules(self, species=None, stage=None, role=None):
            return [{"id": "r1", "category": "termination", "conditions": {}, "action": {}}]
        async def get_action_rule(self, rule_id): return {"id": rule_id, "conditions": {}}
        async def create_action_rule(self, rule): return {"status": "created", "id": rule["id"]}
        async def update_action_rule(self, rule_id, patch): return {"status": "updated", "id": rule_id}
    monkeypatch.setattr(graph_mod, "GraphDAO", _DAO)
    import app.core.dependencies as deps
    monkeypatch.setattr(deps, "get_driver", lambda: object())
    return TestClient(app)


def test_action_rules_is_auth_exempt():
    assert any("/api/graph/action-rules".startswith(p) for p in SKIP_AUTH_PREFIXES)


def test_list_rules(client):
    r = client.get("/api/graph/action-rules?species=Vicia+sativa&role=cover_crop",
                   headers={"X-Tenant-ID": "montiko"})
    assert r.status_code == 200 and r.json()[0]["id"] == "r1"


def test_create_rule(client):
    r = client.post("/api/graph/action-rules", json={"id": "r9", "category": "sowing"},
                    headers={"X-Tenant-ID": "montiko"})
    assert r.status_code == 200 and r.json()["status"] == "created"
