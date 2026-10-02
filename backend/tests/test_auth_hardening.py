"""Hardening of the identity chain: tenant presence, strict mode, ingress origin."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from fastapi.testclient import TestClient
from nkz_platform_sdk.crypto import generate_hmac_signature

from app import auth
from app.api.v1 import graph as graph_mod

ISS = "https://idp.example/realms/r"
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
URN = "urn:ngsi-ld:AgriParcel:tenant-a:p1"
BEARER = "opaque-user-token"


def _rs256(claims: dict) -> str:
    base = {"iss": ISS, "sub": "u1", "exp": int(time.time()) + 300}
    return jwt.encode({**base, **claims}, _KEY, algorithm="RS256")


def _gateway_headers(tenant: str = "tenant-a", token: str = BEARER) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "X-Tenant-ID": tenant,
        "X-User-ID": "u1",
        "X-Auth-Signature": generate_hmac_signature("hmac-test", token, tenant),
    }


class _OrionSpy:
    calls: list[str] = []  # noqa: RUF012

    def __init__(self, tenant_id, *a, **k):
        _OrionSpy.calls.append(tenant_id)

    async def query_entities(self, **kwargs):
        return []

    async def close(self):
        return None


@pytest.fixture
def prod(monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    monkeypatch.setenv("AUTH_STRICT", "true")
    monkeypatch.setenv("HMAC_SECRET", "hmac-test")
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "internal-test")
    monkeypatch.setenv("KEYCLOAK_JWKS_URL", "https://idp.example/certs")
    monkeypatch.setenv("JWT_ISSUERS", ISS)
    fake = MagicMock()
    fake.get_signing_key_from_jwt.return_value = MagicMock(key=_KEY.public_key())
    monkeypatch.setattr(auth, "_jwks_client", lambda url: fake)
    _OrionSpy.calls = []
    env_dao = AsyncMock(side_effect=lambda parcel_id, tenant_id: {"tenant_seen": tenant_id})
    with patch.dict("sys.modules", {"ikerketa": MagicMock(__version__="0.1.0")}), \
         patch("app.core.dependencies.init_driver", AsyncMock()), \
         patch("app.core.dependencies.close_driver", AsyncMock()), \
         patch("app.core.dependencies.get_driver", return_value=MagicMock()), \
         patch("app.api.v1.parcel_data.OrionClient", _OrionSpy), \
         patch("app.graph.dao.GraphDAO.get_parcel_environment", env_dao):
        from app.main import app
        yield TestClient(app), env_dao


# ── C1: tenant must be non-empty on tenant-consuming handlers ──────────────

def test_parcel_vegetation_token_without_tenant_ignores_header(prod):
    client, _ = prod
    r = client.get(
        "/api/parcel/p1/vegetation",
        headers={"Authorization": f"Bearer {_rs256({})}", "X-Tenant-ID": "tenant-b"},
    )
    assert r.status_code == 401
    assert _OrionSpy.calls == []


def test_graph_parcel_route_token_without_tenant_rejected(prod):
    client, env_dao = prod
    r = client.get(
        f"/api/graph/agriculture/parcel-environment?parcel_id={URN}",
        headers={"Authorization": f"Bearer {_rs256({})}", "X-Tenant-ID": "tenant-b"},
    )
    assert r.status_code == 401
    env_dao.assert_not_called()


def test_graph_parcel_handler_rejects_empty_tenant_without_middleware():
    """Defense in depth: the handler itself refuses an unidentified caller."""
    app = FastAPI()
    app.include_router(graph_mod.router, prefix="/api/graph")
    app.dependency_overrides[graph_mod.get_neo4j_driver] = lambda: MagicMock()
    with patch("app.graph.dao.GraphDAO.get_parcel_environment",
               AsyncMock(return_value={"ok": True})) as dao:
        r = TestClient(app).get(
            f"/api/graph/agriculture/parcel-environment?parcel_id={URN}",
            headers={"X-Tenant-ID": "tenant-b"},
        )
    assert r.status_code == 401
    dao.assert_not_called()


# ── C2: AUTH_STRICT fails closed ────────────────────────────────────────────

def test_auth_strict_non_false_value_verifies(prod, monkeypatch):
    client, _ = prod
    monkeypatch.setenv("AUTH_STRICT", "1")
    forged = jwt.encode(
        {"iss": ISS, "sub": "u1", "tenant_id": "tenant-a", "exp": int(time.time()) + 300},
        "attacker-secret", algorithm="HS256",
    )
    r = client.get("/api/parcel/p1/vegetation", headers={"Authorization": f"Bearer {forged}"})
    assert r.status_code == 401
    assert _OrionSpy.calls == []


@pytest.mark.parametrize("value", ["1", "yes", "True ", ""])
async def test_validate_token_verifies_unless_exactly_false(monkeypatch, value):
    monkeypatch.setenv("AUTH_STRICT", value)
    monkeypatch.setenv("KEYCLOAK_JWKS_URL", "https://idp.example/certs")
    monkeypatch.setenv("JWT_ISSUERS", ISS)
    fake = MagicMock()
    fake.get_signing_key_from_jwt.return_value = MagicMock(key=_KEY.public_key())
    monkeypatch.setattr(auth, "_jwks_client", lambda url: fake)
    forged = jwt.encode({"iss": ISS, "exp": int(time.time()) + 300}, "x", algorithm="HS256")
    with pytest.raises(Exception):  # noqa: B017
        await auth.NKZAuthMiddleware(app=MagicMock())._validate_token(forged)


async def test_auth_strict_false_decodes_unverified_and_logs_critical(monkeypatch, caplog):
    monkeypatch.setenv("AUTH_STRICT", " False ")
    forged = jwt.encode({"tenant_id": "tenant-a"}, "x", algorithm="HS256")
    with caplog.at_level("CRITICAL", logger="app.auth"):
        payload = await auth.NKZAuthMiddleware(app=MagicMock())._validate_token(forged)
    assert payload["tenant_id"] == "tenant-a"
    assert any(r.levelname == "CRITICAL" for r in caplog.records)


# ── I1: gateway identity requires a Bearer token ───────────────────────────

def test_gateway_hmac_over_empty_token_rejected(prod):
    client, env_dao = prod
    r = client.get(
        f"/api/graph/agriculture/parcel-environment?parcel_id={URN}",
        headers={
            "X-Tenant-ID": "tenant-a", "X-User-ID": "u1",
            "X-Auth-Signature": generate_hmac_signature("hmac-test", "", "tenant-a"),
        },
    )
    assert r.status_code == 401
    env_dao.assert_not_called()


def test_gateway_with_bearer_accepted(prod):
    client, _ = prod
    r = client.get(
        f"/api/graph/agriculture/parcel-environment?parcel_id={URN}",
        headers=_gateway_headers(),
    )
    assert r.status_code == 200
    assert r.json()["tenant_seen"] == "tenant-a"


# ── I2: internal identity only for in-cluster traffic ──────────────────────

@pytest.mark.parametrize("hdr", ["X-Forwarded-For", "X-Real-Ip"])
def test_internal_secret_via_ingress_rejected(prod, hdr):
    client, env_dao = prod
    r = client.get(
        f"/api/graph/agriculture/parcel-environment?parcel_id={URN}",
        headers={"X-Internal-Service-Secret": "internal-test", "X-Tenant-ID": "tenant-a",
                 hdr: "203.0.113.1"},
    )
    assert r.status_code == 401
    env_dao.assert_not_called()


# ── T3: non-ASCII credentials are rejected, never a 500 ────────────────────

def test_non_ascii_internal_secret_is_401(prod):
    client, _ = prod
    r = client.get(
        f"/api/graph/agriculture/parcel-environment?parcel_id={URN}",
        headers={"X-Internal-Service-Secret": "s\xe9cret".encode("latin-1"),
                 "X-Tenant-ID": "tenant-a"},
    )
    assert r.status_code == 401


def test_non_ascii_gateway_signature_is_401(prod):
    client, _ = prod
    r = client.get(
        f"/api/graph/agriculture/parcel-environment?parcel_id={URN}",
        headers={"Authorization": f"Bearer {BEARER}", "X-Tenant-ID": "tenant-a", "X-User-ID": "u1",
                 "X-Auth-Signature": f"\xe9:{int(time.time())}".encode("latin-1")},
    )
    assert r.status_code == 401


# ── I5a: extrapolate must not accept a caller tenant ───────────────────────

def test_extrapolate_ignores_tenant_query_param(prod):
    client, _ = prod
    seen = {}

    async def fake_extrapolate(self, **kwargs):
        seen.update(kwargs)
        return {"ranked_varieties": []}

    with patch("app.graph.dao.GraphDAO.extrapolate_varieties", fake_extrapolate):
        r = client.get(
            f"/api/graph/agriculture/extrapolate?crop=TRZAX&parcel_id={URN}&tenant_id=tenant-b",
            headers=_gateway_headers(),
        )
    assert r.status_code == 200
    assert seen["tenant_id"] == "tenant-a"


# ── 401 detail never echoes the exception ──────────────────────────────────

def test_jwt_failure_detail_is_fixed(prod):
    client, _ = prod
    r = client.get("/api/parcel/p1/vegetation", headers={"Authorization": "Bearer not-a-jwt"})
    assert r.status_code == 401
    assert r.json() == {"detail": "Invalid or missing credentials"}
