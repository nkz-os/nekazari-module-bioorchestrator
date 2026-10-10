"""fetch_crop_cycles: entity-manager internal route contract and fail-soft behaviour."""
from __future__ import annotations

import logging

import httpx
import pytest
import respx

from app.services.crop_cycles_client import fetch_crop_cycles

BASE = "http://entity-manager.test:5000"
PARCEL = "urn:ngsi-ld:AgriParcel:p-1"
URL = f"{BASE}/api/internal/parcels/{PARCEL}/crop-cycles"


@pytest.fixture(autouse=True)
def _config(monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "entity_manager_url", BASE + "/")
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s3cret")


@respx.mock
async def test_200_returns_payload_and_sends_secret_and_tenant():
    payload = {"current": {"crop_id": "c1"}, "previous": None, "next": None, "cycles": []}
    route = respx.get(URL).mock(return_value=httpx.Response(200, json=payload))

    out = await fetch_crop_cycles(PARCEL, "tenant-a")

    assert out == payload
    request = route.calls[0].request
    assert request.headers["x-internal-service-secret"] == "s3cret"
    assert request.url.params["tenant_id"] == "tenant-a"


@respx.mock
async def test_non_200_returns_none():
    respx.get(URL).mock(return_value=httpx.Response(503))

    assert await fetch_crop_cycles(PARCEL, "tenant-a") is None


@respx.mock
async def test_timeout_returns_none():
    respx.get(URL).mock(side_effect=httpx.ConnectTimeout("slow"))

    assert await fetch_crop_cycles(PARCEL, "tenant-a") is None


@pytest.mark.parametrize(
    ("response", "expected_fragment"),
    [
        (httpx.Response(503), "status=503"),
        (httpx.Response(200, content=b"<html>gateway</html>"), "not JSON"),
    ],
    ids=["non-200", "invalid-json"],
)
@respx.mock
async def test_fallback_is_logged_with_parcel_and_tenant(caplog, monkeypatch, response, expected_fragment):
    # Importing pcse runs a dictConfig that disables already-created loggers, so caplog
    # would see nothing depending on import order; re-enable this one.
    monkeypatch.setattr(logging.getLogger("app.services.crop_cycles_client"), "disabled", False)
    respx.get(URL).mock(return_value=response)

    with caplog.at_level(logging.WARNING, logger="app.services.crop_cycles_client"):
        out = await fetch_crop_cycles(PARCEL, "tenant-a")

    assert out is None
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert expected_fragment in warnings[0]
    assert PARCEL in warnings[0]
    assert "tenant-a" in warnings[0]
