"""The middleware must only trust identities it can verify."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from nkz_platform_sdk.crypto import generate_hmac_signature

URN = "urn:ngsi-ld:AgriParcel:tenant-a:p1"
PATH = f"/api/graph/agriculture/parcel-environment?parcel_id={URN}"


@pytest.fixture
def prod_client(monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    monkeypatch.setenv("AUTH_STRICT", "true")
    monkeypatch.setenv("HMAC_SECRET", "hmac-test")
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "internal-test")
    monkeypatch.delenv("KEYCLOAK_JWKS_URL", raising=False)
    monkeypatch.delenv("JWT_ISSUERS", raising=False)
    with patch.dict("sys.modules", {"ikerketa": MagicMock(__version__="0.1.0")}), \
         patch("app.core.dependencies.init_driver", AsyncMock()), \
         patch("app.core.dependencies.close_driver", AsyncMock()), \
         patch("app.core.dependencies.get_driver", return_value=MagicMock()), \
         patch("app.graph.dao.GraphDAO.get_parcel_environment",
               AsyncMock(side_effect=lambda parcel_id, tenant_id: {"tenant_seen": tenant_id})):
        from app.main import app
        yield TestClient(app)


def test_no_identity_is_rejected(prod_client):
    assert prod_client.get(PATH).status_code == 401


def test_spoofed_gateway_headers_without_signature_are_rejected(prod_client):
    r = prod_client.get(PATH, headers={"X-Tenant-ID": "tenant-a", "X-User-ID": "u1"})
    assert r.status_code == 401


def test_gateway_headers_with_valid_signature_are_accepted(prod_client):
    sig = generate_hmac_signature("hmac-test", "", "tenant-a")
    r = prod_client.get(PATH, headers={"X-Tenant-ID": "tenant-a", "X-User-ID": "u1", "X-Auth-Signature": sig})
    assert r.status_code == 200
    assert r.json()["tenant_seen"] == "tenant-a"


def test_gateway_signature_for_other_tenant_is_rejected(prod_client):
    sig = generate_hmac_signature("hmac-test", "", "tenant-b")
    r = prod_client.get(PATH, headers={"X-Tenant-ID": "tenant-a", "X-User-ID": "u1", "X-Auth-Signature": sig})
    assert r.status_code == 401


def test_stale_gateway_signature_is_rejected(prod_client):
    sig = generate_hmac_signature("hmac-test", "", "tenant-a", int(time.time()) - 301)
    r = prod_client.get(PATH, headers={"X-Tenant-ID": "tenant-a", "X-User-ID": "u1", "X-Auth-Signature": sig})
    assert r.status_code == 401


def test_internal_secret_is_accepted(prod_client):
    r = prod_client.get(PATH, headers={
        "X-Internal-Service-Secret": "internal-test", "X-Tenant-ID": "tenant-a", "X-User-ID": "crop-health-worker"})
    assert r.status_code == 200
    assert r.json()["tenant_seen"] == "tenant-a"


def test_wrong_internal_secret_is_rejected(prod_client):
    r = prod_client.get(PATH, headers={"X-Internal-Service-Secret": "nope", "X-Tenant-ID": "tenant-a"})
    assert r.status_code == 401


def test_empty_internal_secret_config_never_authenticates(prod_client, monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "")
    r = prod_client.get(PATH, headers={"X-Internal-Service-Secret": "", "X-Tenant-ID": "tenant-a"})
    assert r.status_code == 401


def test_public_reference_route_needs_nothing(prod_client):
    with patch("app.graph.dao.GraphDAO.extrapolate_varieties", AsyncMock(return_value={"ranked_varieties": []})):
        r = prod_client.get("/api/graph/agriculture/extrapolate?crop=TRZAX&climate_class=Csa")
    assert r.status_code != 401
