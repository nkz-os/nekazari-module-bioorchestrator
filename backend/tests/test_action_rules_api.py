import os

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


@pytest.fixture
def auth_enabled():
    """Temporarily disable the AUTH_DISABLED override for one test.

    Mirrors the pattern in tests/test_integration_endpoints.py — conftest's
    autouse fixture sets AUTH_DISABLED=true, which would let every request
    through the dev-mode branch regardless of SKIP_AUTH_PREFIXES. Flipping
    it to false forces requests through the real production auth branch so
    we can prove the per-method exemption contract.
    """
    old = os.environ.get("AUTH_DISABLED", "true")
    os.environ["AUTH_DISABLED"] = "false"
    yield
    os.environ["AUTH_DISABLED"] = old


def test_action_rules_is_auth_exempt():
    """action-rules is exempt for GET only — POST/PUT must require auth."""
    methods = SKIP_AUTH_PREFIXES["/api/graph/action-rules"]
    assert "*" not in methods
    assert "GET" in methods
    assert "POST" not in methods
    assert "PUT" not in methods


def test_list_rules(client):
    r = client.get("/api/graph/action-rules?species=Vicia+sativa&role=cover_crop",
                   headers={"X-Tenant-ID": "montiko"})
    assert r.status_code == 200 and r.json()[0]["id"] == "r1"


def test_create_rule(client):
    # In the test env AUTH_DISABLED=true (conftest autouse), so this exercises
    # the dev-mode pass-through, not the action-rules public exemption. The
    # route's own behavior under auth (gateway headers present) is covered
    # below by the production-path tests.
    r = client.post("/api/graph/action-rules", json={"id": "r9", "category": "sowing"},
                    headers={"X-Tenant-ID": "montiko", "X-User-ID": "u1"})
    assert r.status_code == 200 and r.json()["status"] == "created"


# ── Security fix: method-aware exemption (production path) ──────────────────
#
# These tests force AUTH_DISABLED=false so requests actually traverse the
# SKIP_AUTH_PREFIXES check in app/auth.py's dispatch(), proving the real
# contract rather than the dev-mode pass-through.


def test_action_rules_get_is_public(client, auth_enabled):
    """GET /api/graph/action-rules stays public — no auth headers needed."""
    r = client.get("/api/graph/action-rules?species=Vicia+sativa")
    assert r.status_code == 200


def test_action_rules_post_requires_auth(client, auth_enabled):
    """POST /api/graph/action-rules with no auth must now 401 (the fix)."""
    r = client.post("/api/graph/action-rules", json={"id": "r9", "category": "sowing"})
    assert r.status_code == 401


def test_action_rules_put_requires_auth(client, auth_enabled):
    """PUT /api/graph/action-rules/{id} with no auth must now 401 (the fix)."""
    r = client.put("/api/graph/action-rules/r1", json={"category": "sowing"})
    assert r.status_code == 401


def test_action_rules_post_succeeds_with_gateway_auth(client, auth_enabled):
    """POST still works when api-gateway auth headers are present."""
    r = client.post(
        "/api/graph/action-rules",
        json={"id": "r9", "category": "sowing"},
        headers={"X-Tenant-ID": "montiko", "X-User-ID": "u1"},
    )
    assert r.status_code == 200 and r.json()["status"] == "created"


def test_wildcard_prefix_still_exempt_for_post(client, auth_enabled):
    """Non-destructive guarantee: an unrelated '*' prefix (e.g. the Orion-LD
    notification path) stays exempt for POST with no auth — unchanged
    by this fix."""
    r = client.post("/api/ngsi-ld/notify", json={})
    assert r.status_code != 401
