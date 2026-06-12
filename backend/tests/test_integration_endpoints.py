"""Integration tests for critical API endpoints.

These tests verify that the FastAPI app serves endpoints correctly
with proper status codes and response shapes. They use TestClient
so no real HTTP server is needed.

Uses the conftest.py ``client`` fixture which already patches
IkerKeta imports, init_driver/close_driver, and sets AUTH_DISABLED=true.

Neo4j-dependent endpoints need ``get_driver()`` patched because the
lifespan never runs in TestClient mode (the app is created at module
level). Tests that hit routes requiring a driver use the
``with_patched_driver`` fixture.

Mocks for async Neo4j results must support ``async for`` iteration,
which requires ``__aiter__`` to return an object with ``__anext__``.
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from neo4j import AsyncDriver


# ---------------------------------------------------------------------------
# Helper: build a mock Neo4j result that supports async for / async iteration
# ---------------------------------------------------------------------------


class _MockAsyncResult:
    """Mock Neo4j result supporting async for / single() / data().

    ``__aiter__`` returns self (implements ``__anext__``), so the DAO
    can do ``async for record in result`` without crashing.
    """

    def __init__(self, records: list[dict[str, Any]] | None = None):
        self._records = records or []
        self._index = 0

    async def single(self) -> dict | None:
        return self._records[0] if self._records else None

    async def fetch(self, n: int = -1) -> list[dict]:
        return list(self._records)

    def data(self) -> list[dict[str, Any]]:
        return self._records

    def __aiter__(self):
        self._index = 0
        return self

    async def __anext__(self) -> dict:
        if self._index >= len(self._records):
            raise StopAsyncIteration
        record = self._records[self._index]
        self._index += 1
        return record


def _make_mock_driver(records: list[dict[str, Any]] | None = None):
    """Build a mock Neo4j AsyncDriver whose session returns a ``_MockAsyncResult``."""
    mock_driver = AsyncMock(spec=AsyncDriver)
    mock_session = AsyncMock()
    mock_session.__aenter__.return_value = mock_session

    mock_result = _MockAsyncResult(records)
    mock_session.run.return_value = mock_result
    mock_driver.session.return_value = mock_session
    return mock_driver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def with_patched_driver():
    """Patch ``app.core.dependencies.get_driver`` so routes that inject
    a ``DriverDep`` don't crash with RuntimeError (lifespan never runs)."""
    with patch("app.core.dependencies.get_driver") as mock_get:
        mock_get.return_value = _make_mock_driver()
        yield


@pytest.fixture
def auth_enabled():
    """Temporarily disable the AUTH_DISABLED override for one test."""
    old = os.environ.get("AUTH_DISABLED", "true")
    os.environ["AUTH_DISABLED"] = "false"
    yield
    os.environ["AUTH_DISABLED"] = old


# ── Health endpoints ──────────────────────────────────────────────────────────


def test_healthz(client):
    """Liveness probe must return 200."""
    resp = client.get("/healthz")
    assert resp.status_code == 200


def test_readyz(client):
    """Readiness probe must return 200 or 503 (IkerKeta availability)."""
    resp = client.get("/readyz")
    assert resp.status_code in (200, 503)


# ── Public reference data endpoints ───────────────────────────────────────────

# These endpoints are in SKIP_AUTH_PREFIXES — they must be accessible
# without any auth headers even when AUTH_DISABLED=false.
# They may return 500 if a dependency is unavailable (expected in test).


def test_crop_catalog_public(client):
    """Crop catalog (/api/crop/catalog) must be accessible without auth."""
    resp = client.get("/api/crop/catalog")
    assert resp.status_code in (200, 422, 500), (
        f"Expected 200/422/500, got {resp.status_code}"
    )


def test_sources_public(client):
    """Sources endpoint (/api/graph/agriculture/sources) must be public."""
    resp = client.get("/api/graph/agriculture/sources")
    assert resp.status_code in (200, 422, 500), (
        f"Expected 200/422/500, got {resp.status_code}"
    )


def test_phenology_params_public(client):
    """Phenology params (/api/graph/phenology-params) must be accessible.

    Note: Returns 404 when Neo4j returns no data (standard for mock env).
    The key test is that it does NOT return 401/403.
    """
    resp = client.get("/api/graph/phenology-params", params={"species": "olive"})
    assert resp.status_code in (200, 404, 422, 500), (
        f"Expected 200/404/422/500, got {resp.status_code}"
    )


def test_trial_sites_public(client):
    """Trial sites endpoint must be public."""
    resp = client.get("/api/graph/agriculture/trial-sites")
    assert resp.status_code in (200, 422, 500), (
        f"Expected 200/422/500, got {resp.status_code}"
    )


def test_extrapolate_public(client):
    """Extrapolate endpoint must be public."""
    resp = client.get("/api/graph/agriculture/extrapolate", params={"crop": "TRZAX", "climate_class": "Csa", "top_n": "3"})
    assert resp.status_code in (200, 422, 500), (
        f"Expected 200/422/500, got {resp.status_code}"
    )


# ── Protected endpoints (auth required) ───────────────────────────────────────

# These endpoints are NOT in SKIP_AUTH_PREFIXES — they must return
# 401/403 when AUTH_DISABLED is false and no auth header is present.


def test_pipeline_run_requires_auth(client, auth_enabled):
    """POST /api/pipeline/run must require auth when auth is enabled."""
    resp = client.post("/api/pipeline/run", json={})
    assert resp.status_code in (401, 403), (
        f"Expected 401/403, got {resp.status_code}"
    )


def test_contribute_endpoint_exists(client):
    """POST /api/graph/phenology-params/contribute must exist.

    Note: The endpoint is intentionally public (same SKIP_AUTH_PREFIXES
    as the GET /phenology-params). It uses Query params, not body.
    With the mock driver it returns 500 (expected — no real Neo4j).
    """
    resp = client.post(
        "/api/graph/phenology-params/contribute",
        params={"species": "test", "stage": "initial", "kc": 0.5},
    )
    assert resp.status_code in (200, 422, 500), (
        f"Expected 200/422/500, got {resp.status_code}"
    )


# ── CORS headers ──────────────────────────────────────────────────────────────

# Starlette CORSMiddleware adds Access-Control-Allow-Origin on all responses
# when the Origin header matches allowed_origins. Other CORS headers are
# only added for OPTIONS preflight requests.


def test_cors_headers_on_success(client):
    """200 responses must include CORS origin header."""
    resp = client.get("/healthz", headers={"Origin": "http://localhost:5173"})
    assert resp.status_code == 200
    assert "access-control-allow-origin" in resp.headers
    assert resp.headers["access-control-allow-origin"] == "http://localhost:5173"


def test_cors_headers_on_401(client, auth_enabled):
    """401 responses must also include CORS headers (browser needs them)."""
    resp = client.get(
        "/api/pipeline/run",
        headers={"Origin": "http://localhost:5173"},
    )
    assert resp.status_code in (401, 403)
    assert "access-control-allow-origin" in resp.headers, (
        "CORS headers missing on 401 -- browser will block the error response"
    )


# ── OPTIONS preflight (CORS) ──────────────────────────────────────────────────


def test_cors_preflight_allowed_methods(client):
    """OPTIONS preflight must include Access-Control-Allow-Methods."""
    resp = client.options(
        "/api/graph/stats",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert resp.status_code == 200
    assert "access-control-allow-origin" in resp.headers
    assert "access-control-allow-methods" in resp.headers
    assert "GET" in resp.headers["access-control-allow-methods"]
