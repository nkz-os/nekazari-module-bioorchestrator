"""/api/graph/soil-data must emit Sunset header and proxy to Orion via NGSI-LD."""
from __future__ import annotations

import httpx
import pytest
import respx
from fastapi import FastAPI
from httpx import ASGITransport

from app.api.v1.graph import router as graph_router


def _make_app() -> FastAPI:
    """Minimal FastAPI app without middleware — avoids starlette/anyio loop issues."""
    app = FastAPI()
    app.include_router(graph_router, prefix="/api/graph")
    return app


@pytest.mark.anyio
@respx.mock
async def test_soil_data_returns_sunset_header_and_proxies(monkeypatch):
    """Verify /api/graph/soil-data emits Sunset header and proxies to Orion-LD."""
    monkeypatch.setenv("ORION_BASE_URL", "http://orion-mock:1026")
    monkeypatch.setenv("CONTEXT_URL", "http://ctx/context.jsonld")

    # Mock Orion response for AgriSoilExtended query
    respx.get("http://orion-mock:1026/ngsi-ld/v1/entities").mock(
        return_value=httpx.Response(200, json=[
            {"id": "urn:ngsi-ld:AgriSoilExtended:point", "horizons": {"value": []}}
        ])
    )

    app = _make_app()
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.get("/api/graph/soil-data?lat=42.81&lon=-1.64")

    assert r.status_code == 200, f"Expected 200 but got {r.status_code}: {r.text}"

    # Check for deprecation headers (case-insensitive)
    headers_lower = {h.lower(): v for h, v in r.headers.items()}
    assert "sunset" in headers_lower, f"Missing Sunset header. Headers: {dict(r.headers)}"
    assert "deprecation" in headers_lower, f"Missing Deprecation header. Headers: {dict(r.headers)}"

    # Verify body contains proxied Orion response
    body = r.json()
    assert isinstance(body, dict), f"Expected dict response, got {type(body)}"
    # The key could be agriSoilExtended or agri_soil_extended or similar variants
    assert "agriSoilExtended" in body or "agri_soil_extended" in body, \
        f"Expected agriSoilExtended key in response body. Got: {body.keys()}"
