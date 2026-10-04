"""Endpoint x role matrix for the graph/catalog write paths, through the real middleware.

unauthenticated -> 401, wrong role -> 403, right role -> passes the auth layer.
"""
from __future__ import annotations

import sys
import time
import types
from unittest.mock import AsyncMock, MagicMock, patch

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException
from fastapi.testclient import TestClient
from nkz_platform_sdk.crypto import generate_hmac_signature
from starlette.requests import Request

from app import auth
from app.graph.dao import GraphDAO
from tests.gateway_token import gateway_token

ISS = "https://idp.example/realms/r"
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
TENANT = "tenant-a"

ADMIN = "PlatformAdmin"
CONTRIBUTOR_ROLES = ("TechnicalConsultant", "TenantAdmin", "PlatformAdmin")

_PHENO = {"species": "Zea mays", "stage": "initial", "kc": "0.5"}
_CONTRIB_BODY = {"crop_id": "urn:ngsi-ld:AgriCrop:maize", "params": {"kc": 0.5}, "provenance": {}}

# (name, method, path, request kwargs); roles allowed are attached in ALL_ENDPOINTS
ADMIN_ONLY = [
    ("catalog-ingest", "POST", "/api/crop/catalog/ingest", {"params": {"source": "ecocrop"}}),
    ("catalog-derive-thermal", "POST", "/api/crop/catalog/derive-thermal", {}),
    ("action-rules-create", "POST", "/api/graph/action-rules",
     {"json": {"id": "r9", "category": "sowing"}}),
    ("action-rules-update", "PUT", "/api/graph/action-rules/r1", {"json": {"category": "sowing"}}),
    ("pipeline-run", "POST", "/api/pipeline/run", {"json": {}}),
    ("navarra-ingest", "POST", "/api/ingestion/navarra-agraria", {"params": {"dry_run": "true"}}),
]
CONTRIBUTIONS = [
    ("catalog-contribute", "POST", "/api/crop/catalog/contribute", {"json": _CONTRIB_BODY}),
    ("phenology-contribute", "POST", "/api/graph/phenology-params/contribute", {"params": _PHENO}),
]
ALL_ENDPOINTS = [(*e, (ADMIN,)) for e in ADMIN_ONLY] + [
    (*e, CONTRIBUTOR_ROLES) for e in CONTRIBUTIONS
]
ALL_ROLES = ("Farmer", "TechnicalConsultant", "TenantAdmin", "PlatformAdmin")


def _gateway_headers(
    roles: str, user: str = "u1", tenant: str = TENANT, extra_claims: dict | None = None
) -> dict:
    """Headers as the api-gateway sends them: a signed token plus identity headers.

    ``roles`` is a comma-separated string; it goes into the token claims and
    (as the gateway does) into X-User-Roles. Only the claims are trusted.
    """
    token = gateway_token(sub=user, tenant=tenant, roles=[r for r in roles.split(",") if r])
    if extra_claims:
        token = jwt.encode(
            {**jwt.decode(token, options={"verify_signature": False}), **extra_claims},
            "gateway-test-key-0123456789abcdef0123456789",
            algorithm="HS256",
        )
    return {
        "Authorization": f"Bearer {token}",
        "X-Tenant-ID": tenant,
        "X-User-ID": user,
        "X-User-Roles": roles,
        "X-Auth-Signature": generate_hmac_signature("hmac-test", token, tenant),
    }


class _Result:
    def __init__(self, created: int):
        self.created = created

    async def single(self):
        return {"status": "pending_review", "source": "Contributed: anonymous",
                "created": self.created}


class _Session:
    def __init__(self, sink, created):
        self.sink = sink
        self.created = created

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def run(self, query, **params):
        self.sink.append({"query": query, **params})
        return _Result(self.created)


class _Driver:
    def __init__(self):
        self.runs: list[dict] = []
        self.created = 1  # rows the MATCH found; 0 simulates an unknown crop

    def session(self):
        return _Session(self.runs, self.created)


class _FakeDAO(GraphDAO):
    """GraphDAO with the rule writes stubbed; contributions run the real query."""

    async def create_action_rule(self, rule):
        return {"status": "created", "id": rule["id"]}

    async def update_action_rule(self, rule_id, patch_):
        return {"status": "updated", "id": rule_id}


@pytest.fixture
def driver() -> _Driver:
    return _Driver()


@pytest.fixture
def prod(monkeypatch, driver):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    monkeypatch.setenv("AUTH_STRICT", "true")
    monkeypatch.setenv("HMAC_SECRET", "hmac-test")
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "internal-test")
    monkeypatch.setenv("KEYCLOAK_JWKS_URL", "https://idp.example/certs")
    monkeypatch.setenv("JWT_ISSUERS", ISS)
    jwks = MagicMock()
    jwks.get_signing_key_from_jwt.return_value = MagicMock(key=_KEY.public_key())
    monkeypatch.setattr(auth, "_jwks_client", lambda url: jwks)

    pipeline = types.ModuleType("ikerketa.pipeline")
    pipeline.run_pipeline = lambda **kw: types.SimpleNamespace(
        failure_count=0, entities_before_dedup=0, entities_after_dedup=0,
        relationships_total=0, crossref_matches=0, total_duration_seconds=0.0, errors=[],
    )
    report = types.ModuleType("ikerketa.report")
    report.generate_report = lambda result: "report"
    navarra = MagicMock()
    navarra.return_value.ingest = AsyncMock(return_value={"nodes": 0})
    ecocrop = MagicMock()
    ecocrop.return_value.ingest = AsyncMock(return_value={"ingested": 0})
    orion = MagicMock()
    orion.return_value.close = AsyncMock()
    orion.return_value.append_entity_attrs = AsyncMock()

    with patch.dict(sys.modules, {
            "ikerketa": MagicMock(__version__="0.1.0"),
            "ikerketa.pipeline": pipeline,
            "ikerketa.report": report}), \
         patch("app.core.dependencies.init_driver", AsyncMock()), \
         patch("app.core.dependencies.close_driver", AsyncMock()), \
         patch("app.core.dependencies.get_driver", return_value=driver), \
         patch("app.api.v1.graph.GraphDAO", _FakeDAO), \
         patch("app.api.v1.catalog.OrionClient", orion), \
         patch("app.api.v1.catalog.EcoCropIngester", ecocrop), \
         patch("subprocess.Popen", MagicMock()) as popen, \
         patch("app.ingestion.navarra_ingester.NavarraIngester", navarra), \
         patch("app.main._store_pipeline_history", AsyncMock()):
        from app.main import app

        client = TestClient(app, raise_server_exceptions=False)
        client.popen = popen
        client.orion = orion
        yield client


def _call(client, method, path, kwargs, headers=None):
    return client.request(method, path, headers=headers, **kwargs)


# ── unauthenticated ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("name,method,path,kwargs,roles", ALL_ENDPOINTS)
def test_unauthenticated_is_401(prod, name, method, path, kwargs, roles):
    assert _call(prod, method, path, kwargs).status_code == 401


@pytest.mark.parametrize("name,method,path,kwargs,roles", ALL_ENDPOINTS)
def test_forged_role_header_without_signature_is_401(prod, name, method, path, kwargs, roles):
    headers = {"X-Tenant-ID": TENANT, "X-User-ID": "u1", "X-User-Roles": ADMIN}
    assert _call(prod, method, path, kwargs, headers).status_code == 401


@pytest.mark.parametrize("name,method,path,kwargs,roles", ALL_ENDPOINTS)
def test_forged_role_header_with_wrong_signature_is_401(prod, name, method, path, kwargs, roles):
    headers = _gateway_headers(ADMIN)
    token = headers["Authorization"].removeprefix("Bearer ")
    headers["X-Auth-Signature"] = generate_hmac_signature("another-secret", token, TENANT)
    assert _call(prod, method, path, kwargs, headers).status_code == 401


# ── role matrix (gateway-signed identity) ────────────────────────────────────

@pytest.mark.parametrize("role", ALL_ROLES)
@pytest.mark.parametrize("name,method,path,kwargs,allowed", ALL_ENDPOINTS)
def test_role_matrix(prod, name, method, path, kwargs, allowed, role):
    resp = _call(prod, method, path, kwargs, _gateway_headers(role))
    if role in allowed:
        assert resp.status_code == 200, f"{name} as {role}: {resp.status_code} {resp.text}"
    else:
        assert resp.status_code == 403, f"{name} as {role}: {resp.status_code}"


@pytest.mark.parametrize("name,method,path,kwargs,allowed", ALL_ENDPOINTS)
def test_no_roles_is_403(prod, name, method, path, kwargs, allowed):
    resp = _call(prod, method, path, kwargs, _gateway_headers(""))
    assert resp.status_code == 403


@pytest.mark.parametrize("name,method,path,kwargs,allowed", ALL_ENDPOINTS)
def test_unrelated_role_string_is_403(prod, name, method, path, kwargs, allowed):
    # role names are matched exactly, never by substring or case
    resp = _call(prod, method, path, kwargs, _gateway_headers("platformadmin,PlatformAdmins"))
    assert resp.status_code == 403


@pytest.mark.parametrize("name,method,path,kwargs,allowed", ALL_ENDPOINTS)
def test_roles_header_cannot_grant_what_the_signed_token_lacks(prod, name, method, path, kwargs, allowed):
    # A signed Farmer token with a forged X-User-Roles header stays a Farmer.
    headers = _gateway_headers("Farmer")
    headers["X-User-Roles"] = "PlatformAdmin"
    assert _call(prod, method, path, kwargs, headers).status_code == 403


@pytest.mark.parametrize("name,method,path,kwargs,allowed", ALL_ENDPOINTS)
def test_roles_come_from_signed_token_claims_not_the_header(prod, name, method, path, kwargs, allowed):
    headers = _gateway_headers("PlatformAdmin")
    del headers["X-User-Roles"]
    assert _call(prod, method, path, kwargs, headers).status_code == 200


def test_gateway_role_parity_realm_and_client_roles(prod):
    # Same union the gateway builds: realm_access + resource_access + roles claim.
    for claims in (
        {"realm_access": {"roles": [ADMIN]}},
        {"realm_access": {"roles": []}, "resource_access": {"nekazari-frontend": {"roles": [ADMIN]}}},
        {"realm_access": {"roles": []}, "roles": [ADMIN]},
    ):
        headers = _gateway_headers("", extra_claims=claims)
        resp = prod.post("/api/pipeline/run", json={}, headers=headers)
        assert resp.status_code == 200, claims


def test_gateway_user_id_header_is_ignored(prod, driver):
    headers = _gateway_headers("TenantAdmin", user="user-7")
    headers["X-User-ID"] = "spoofed"
    resp = prod.post("/api/graph/phenology-params/contribute", params=_PHENO, headers=headers)
    assert resp.status_code == 200
    assert driver.runs[0]["contributed_by"] == "user-7"


def test_gateway_signed_non_jwt_token_is_not_an_identity(prod):
    token = "opaque-token"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Tenant-ID": TENANT,
        "X-User-ID": "u1",
        "X-User-Roles": ADMIN,
        "X-Auth-Signature": generate_hmac_signature("hmac-test", token, TENANT),
    }
    assert prod.post("/api/pipeline/run", json={}, headers=headers).status_code == 401


def test_unverified_jwt_grants_no_roles(prod, monkeypatch):
    # AUTH_STRICT=false skips signature verification, so claims must not grant roles.
    monkeypatch.setenv("AUTH_STRICT", "false")
    resp = prod.post("/api/pipeline/run", json={}, headers=_bearer({"realm_access": {"roles": [ADMIN]}}))
    assert resp.status_code == 403


@pytest.mark.parametrize("name,method,path,kwargs,allowed", ALL_ENDPOINTS)
def test_internal_service_identity_has_no_write_role(prod, name, method, path, kwargs, allowed):
    headers = {
        "X-Internal-Service-Secret": "internal-test",
        "X-Tenant-ID": TENANT,
        "X-User-ID": "crop-health-worker",
        "X-User-Roles": ADMIN,  # a service cannot grant itself a role
    }
    assert _call(prod, method, path, kwargs, headers).status_code == 403


def test_wrong_role_is_rejected_before_parameter_validation(prod):
    # A caller without the role learns nothing about the request schema.
    resp = prod.post(
        "/api/graph/phenology-params/contribute", headers=_gateway_headers("Farmer")
    )
    assert resp.status_code == 403


# ── direct-ingress JWT path: roles come from the verified token claims ───────

def _jwt(claims: dict) -> str:
    base = {"iss": ISS, "sub": "kc-user", "tenant_id": TENANT, "exp": int(time.time()) + 300}
    return jwt.encode({**base, **claims}, _KEY, algorithm="RS256")


def _bearer(claims: dict) -> dict:
    return {"Authorization": f"Bearer {_jwt(claims)}"}


@pytest.mark.parametrize(
    "claims",
    [
        {"realm_access": {"roles": [ADMIN]}},
        {"resource_access": {"nekazari-frontend": {"roles": [ADMIN]}}},
        {"roles": [ADMIN]},
    ],
    ids=["realm_access", "resource_access", "roles-claim"],
)
def test_jwt_roles_are_read_from_verified_claims(prod, claims):
    resp = prod.post("/api/graph/action-rules", json={"id": "r9", "category": "sowing"},
                     headers=_bearer(claims))
    assert resp.status_code == 200


def test_jwt_without_admin_role_is_403(prod):
    resp = prod.post("/api/graph/action-rules", json={"id": "r9", "category": "sowing"},
                     headers=_bearer({"realm_access": {"roles": ["Farmer"]}}))
    assert resp.status_code == 403


def test_jwt_without_any_roles_is_403(prod):
    resp = prod.post("/api/pipeline/run", json={}, headers=_bearer({}))
    assert resp.status_code == 403


def test_unsigned_jwt_is_401(prod):
    forged = jwt.encode({"iss": ISS, "sub": "x", "tenant_id": TENANT,
                         "exp": int(time.time()) + 300, "roles": [ADMIN]},
                        "not-the-idp-key", algorithm="HS256")
    resp = prod.post("/api/pipeline/run", json={}, headers={"Authorization": f"Bearer {forged}"})
    assert resp.status_code == 401


# ── dev mode keeps working ───────────────────────────────────────────────────

def test_auth_disabled_dev_user_is_platform_admin(prod, monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "true")
    resp = prod.post("/api/graph/action-rules", json={"id": "r9", "category": "sowing"})
    assert resp.status_code == 200


# ── contributions record who submitted ───────────────────────────────────────

def test_catalog_contribution_records_subject_and_tenant(prod, driver):
    resp = prod.post(
        "/api/crop/catalog/contribute", json=_CONTRIB_BODY,
        headers=_gateway_headers("TechnicalConsultant", user="user-42", tenant="tenant-z"),
    )
    assert resp.status_code == 200
    run = driver.runs[0]
    assert run["contributed_by"] == "user-42"
    assert run["contributor_tenant"] == "tenant-z"
    assert "contributorTenant" in run["query"]


def test_both_contribution_paths_use_the_same_property_names(prod, driver):
    prod.post("/api/crop/catalog/contribute", json=_CONTRIB_BODY,
              headers=_gateway_headers("TenantAdmin"))
    prod.post("/api/graph/phenology-params/contribute", params=_PHENO,
              headers=_gateway_headers("TenantAdmin"))
    catalog_run, phenology_run = driver.runs
    for run in (catalog_run, phenology_run):
        assert "contributedBy" in run["query"] and "contributorTenant" in run["query"]
        assert run["contributed_by"] == "u1" and run["contributor_tenant"] == TENANT


def test_phenology_contribution_records_subject_and_tenant(prod, driver):
    resp = prod.post(
        "/api/graph/phenology-params/contribute",
        params={**_PHENO, "contact_email": "someone@example.org"},
        headers=_gateway_headers("TenantAdmin", user="user-7", tenant="tenant-y"),
    )
    assert resp.status_code == 200
    run = driver.runs[0]
    assert run["contributed_by"] == "user-7"
    assert run["contributor_tenant"] == "tenant-y"
    assert "contributedBy" in run["query"] and "contributorTenant" in run["query"]
    assert run["contact_email"] == "someone@example.org"


def test_contributor_identity_comes_from_the_token_not_the_query(prod, driver):
    resp = prod.post(
        "/api/graph/phenology-params/contribute",
        params={**_PHENO, "contributed_by": "spoofed", "contributor_tenant": "spoofed"},
        headers=_gateway_headers("TenantAdmin", user="user-7", tenant="tenant-y"),
    )
    assert resp.status_code == 200
    assert driver.runs[0]["contributed_by"] == "user-7"
    assert driver.runs[0]["contributor_tenant"] == "tenant-y"


# ── catalog contribute: allow-listed keys, crop must exist, Orion is admin-only ──

def _contribute(client, body, roles="TechnicalConsultant"):
    return client.post("/api/crop/catalog/contribute", json=body, headers=_gateway_headers(roles))


@pytest.mark.parametrize(
    "reserved",
    ["status", "contributedBy", "contributorTenant", "contributedAt", "sourceDoi", "uri", "isDefault"],
)
def test_catalog_contribute_rejects_reserved_keys(prod, driver, reserved):
    body = {**_CONTRIB_BODY, "params": {"kc": 0.5, reserved: "approved"}}
    resp = _contribute(prod, body)
    assert resp.status_code == 400
    assert reserved in resp.json()["detail"]
    assert driver.runs == []  # nothing written


@pytest.mark.parametrize("value", ["0.5", None, True, float("inf"), {"a": 1}, [1]])
def test_catalog_contribute_rejects_non_numeric_values(prod, driver, value):
    body = {**_CONTRIB_BODY, "params": {"kc": value}}
    resp = prod.post("/api/crop/catalog/contribute", content=__import__("json").dumps(body, allow_nan=True),
                     headers={**_gateway_headers("TechnicalConsultant"), "Content-Type": "application/json"})
    assert resp.status_code == 400
    assert driver.runs == []


def test_catalog_contribute_rejects_non_scalar_provenance(prod, driver):
    body = {**_CONTRIB_BODY, "provenance": {"doi": {"nested": "map"}}}
    assert _contribute(prod, body).status_code == 400
    assert driver.runs == []


def test_catalog_contribute_unknown_crop_is_404_and_pushes_nothing(prod, driver):
    driver.created = 0
    body = {**_CONTRIB_BODY, "params": {"kcIni": 0.3}}
    resp = _contribute(prod, body, roles="PlatformAdmin")
    assert resp.status_code == 404
    prod.orion.return_value.append_entity_attrs.assert_not_called()


def test_catalog_contribute_does_not_touch_orion_for_non_admins(prod, driver):
    body = {**_CONTRIB_BODY, "params": {"kcIni": 0.3, "kcMid": 1.1, "kcEnd": 0.6}}
    for roles in ("TechnicalConsultant", "TenantAdmin"):
        resp = _contribute(prod, body, roles=roles)
        assert resp.status_code == 200
        assert resp.json()["applied_to_catalog"] is False
    assert len(driver.runs) == 2  # still recorded for review
    prod.orion.assert_not_called()


def test_catalog_contribute_by_platform_admin_applies_to_orion(prod, driver):
    body = {**_CONTRIB_BODY, "params": {"kcIni": 0.3, "kcMid": 1.1, "d1": 5}}
    resp = _contribute(prod, body, roles="PlatformAdmin")
    assert resp.status_code == 200
    assert resp.json()["applied_to_catalog"] is True
    prod.orion.return_value.append_entity_attrs.assert_awaited_once_with(
        _CONTRIB_BODY["crop_id"],
        {"kcIni": {"type": "Property", "value": 0.3}, "kcMid": {"type": "Property", "value": 1.1}},
    )


def test_derive_thermal_spawns_nothing_for_unauthorised_caller(prod):
    resp = prod.post("/api/crop/catalog/derive-thermal", headers=_gateway_headers("Farmer"))
    assert resp.status_code == 403
    prod.popen.assert_not_called()


# ── dependency units ─────────────────────────────────────────────────────────

def _request(user=None) -> Request:
    request = Request({"type": "http", "headers": []})
    if user is not None:
        request.state.user = user
    return request


async def test_get_current_user_without_identity_is_401():
    from app.core.dependencies import get_current_user

    with pytest.raises(HTTPException) as exc:
        await get_current_user(_request())
    assert exc.value.status_code == 401


async def test_get_current_user_rejects_identity_without_subject():
    from app.core.dependencies import get_current_user

    with pytest.raises(HTTPException) as exc:
        await get_current_user(_request({"roles": [ADMIN]}))
    assert exc.value.status_code == 401


async def test_get_current_user_returns_verified_identity():
    from app.core.dependencies import get_current_user

    user = {"sub": "u1", "tenant_id": TENANT, "roles": ["Farmer"]}
    assert await get_current_user(_request(user)) == user


async def test_require_roles_allows_any_listed_role():
    from app.core.dependencies import require_roles

    dep = require_roles("TechnicalConsultant", "PlatformAdmin")
    user = {"sub": "u1", "roles": ["Farmer", "TechnicalConsultant"]}
    assert await dep(user) == user


async def test_require_roles_rejects_other_roles_with_403():
    from app.core.dependencies import require_roles

    dep = require_roles("PlatformAdmin")
    for roles in ([], ["Farmer"], ["TenantAdmin"], None):
        with pytest.raises(HTTPException) as exc:
            await dep({"sub": "u1", "roles": roles})
        assert exc.value.status_code == 403


def test_require_roles_needs_at_least_one_role():
    from app.core.dependencies import require_roles

    with pytest.raises(ValueError):
        require_roles()
