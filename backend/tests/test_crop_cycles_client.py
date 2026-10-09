"""fetch_crop_cycles: entity-manager internal route contract and fail-soft behaviour."""
from __future__ import annotations

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
