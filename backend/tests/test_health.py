"""Test health check and readiness endpoints."""

from __future__ import annotations


def test_healthz(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_readyz_responds(client):
    resp = client.get("/readyz")
    assert resp.status_code in (200, 503)
    assert "status" in resp.json()


def test_healthz_no_auth_required(client):
    """Health endpoints must be accessible without auth."""
    resp = client.get("/healthz", headers={})
    assert resp.status_code == 200
