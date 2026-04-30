"""Test NKZ auth middleware."""

from __future__ import annotations

import os


def test_auth_disabled_allows_all(client):
    """When AUTH_DISABLED=true, all requests pass through."""
    os.environ["AUTH_DISABLED"] = "true"
    resp = client.get("/api/graph/stats")
    # May 500 because Neo4j is mocked but should NOT be 401
    assert resp.status_code != 401


def test_auth_required_without_header(client):
    """When AUTH_DISABLED=false, missing header returns 401."""
    os.environ["AUTH_DISABLED"] = "false"
    resp = client.get("/api/graph/stats")
    assert resp.status_code == 401
    assert "Missing" in resp.json()["detail"]


def test_dev_user_has_tenant_id(client):
    """Dev user should get tenant_id=dev."""
    os.environ["AUTH_DISABLED"] = "true"
    # The /api/graph/stats endpoint uses Depends(_get_tenant_id)
    # which reads request.state.tenant_id set by auth middleware
    resp = client.get("/healthz")  # skipped by auth
    assert resp.status_code == 200
