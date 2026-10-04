"""Auth-exempt prefixes must never let a write through unauthenticated.

SKIP_AUTH_PREFIXES lists public reference-data prefixes. A "*" entry exempts
every HTTP method, so a write route added under it would skip authentication.
Only three prefixes may keep "*", each guarded by something other than the
bearer token.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.auth import SKIP_AUTH_PREFIXES

# Prefixes where an unauthenticated non-GET is intended, and why it is safe:
#   /api/graph/agriculture/ — writes need an identity (auth_policy.requires_identity)
#   /api/ngsi-ld/           — Orion notify receiver, guarded by the internal secret
#   /api/graph/internal/    — in-cluster receiver, guarded by the internal secret
WRITE_EXEMPT_ALLOWLIST = frozenset(
    {"/api/graph/agriculture/", "/api/ngsi-ld/", "/api/graph/internal/"}
)
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def test_no_exempt_prefix_allows_writes_except_allowlist():
    offenders = {
        prefix: sorted(methods)
        for prefix, methods in SKIP_AUTH_PREFIXES.items()
        if prefix not in WRITE_EXEMPT_ALLOWLIST and not set(methods) <= SAFE_METHODS
    }
    assert offenders == {}, f"prefixes exempting writes from auth: {offenders}"


def test_allowlist_has_no_stale_entries():
    assert WRITE_EXEMPT_ALLOWLIST <= set(SKIP_AUTH_PREFIXES)


def _exempt_writes() -> list[tuple[str, str]]:
    """(method, path) of every registered non-GET route the middleware lets through."""
    from app.main import app

    found = []
    for path, operations in app.openapi()["paths"].items():
        for method in operations:
            method = method.upper()
            if method in SAFE_METHODS:
                continue
            for prefix, methods in SKIP_AUTH_PREFIXES.items():
                if path.startswith(prefix) and ("*" in methods or method in methods):
                    found.append((method, path))
    return found


def test_every_registered_exempt_write_route_is_allowlisted():
    stray = [
        (m, p)
        for m, p in _exempt_writes()
        if not any(p.startswith(prefix) for prefix in WRITE_EXEMPT_ALLOWLIST)
    ]
    assert stray == [], f"write routes reachable without authentication: {stray}"


@pytest.fixture
def prod_client(monkeypatch):
    monkeypatch.setenv("AUTH_DISABLED", "false")
    monkeypatch.setenv("AUTH_STRICT", "true")
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "internal-test")
    monkeypatch.setenv("NOTIFY_REQUIRE_INTERNAL_SECRET", "true")
    monkeypatch.delenv("KEYCLOAK_JWKS_URL", raising=False)
    monkeypatch.delenv("JWT_ISSUERS", raising=False)
    with patch.dict("sys.modules", {"ikerketa": MagicMock(__version__="0.1.0")}), \
         patch("app.core.dependencies.init_driver", AsyncMock()), \
         patch("app.core.dependencies.close_driver", AsyncMock()), \
         patch("app.core.dependencies.get_driver", return_value=MagicMock()):
        from app.main import app

        yield TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("path", ["/api/ngsi-ld/notify", "/api/graph/internal/phenology-update"])
def test_exempt_receivers_enforce_the_internal_secret(prod_client, path):
    """The prefixes that keep '*' must reject a request without the shared secret."""
    body = {"data": []}
    assert prod_client.post(path, json=body).status_code == 401
    assert prod_client.post(
        path, json=body, headers={"X-Internal-Service-Secret": "wrong"}
    ).status_code == 401
    ok = prod_client.post(
        path, json=body, headers={"X-Internal-Service-Secret": "internal-test"}
    )
    assert ok.status_code == 204


@pytest.mark.parametrize(
    "path",
    [
        "/api/graph/phenology-params/contribute",
        "/api/graph/species",
        "/api/graph/phenology-stages",
        "/api/graph/heat-tolerance",
        "/api/graph/nutrient-profile",
        "/api/graph/soil-suitability",
        "/api/graph/rotation-constraints",
        "/api/graph/recommendations/next-crop",
        "/api/graph/varieties",
        "/api/crop/catalog",
        "/api/crop/catalog/ingest",
        "/api/crop/catalog/contribute",
        "/api/crop/catalog/derive-thermal",
        "/api/v1/sources",
        "/api/v1/catalog",
        "/api/v1/capability",
        "/ngsi-ld/bioorchestrator-context.jsonld",
    ],
)
@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_public_reference_prefixes_reject_unauthenticated_writes(prod_client, method, path):
    resp = prod_client.request(method, path)
    assert resp.status_code == 401, f"{method} {path} -> {resp.status_code}"


@pytest.mark.parametrize(
    "path",
    [
        "/api/graph/species",
        "/api/graph/action-rules",
        "/api/crop/catalog",
        "/api/v1/sources",
        "/ngsi-ld/bioorchestrator-context.jsonld",
    ],
)
def test_public_reference_reads_stay_public(prod_client, path):
    assert prod_client.get(path).status_code != 401
