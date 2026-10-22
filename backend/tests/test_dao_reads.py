"""TDD: weather-map HTTP contract for fetch_parcel_weather_stats (Site A)
and get_yield_potential CropHealthAssessment lookup (Site B)."""
from __future__ import annotations

import asyncio

import httpx
import pytest
import respx
from nkz_platform_sdk.crypto import verify_hmac_signature

from app.graph.dao import GraphDAO

WEATHER_MAP_URL = "http://weather-map-backend:8080"


@pytest.fixture(autouse=True)
def _weather_map_url(monkeypatch):
    from app.core.config import settings
    monkeypatch.setattr(settings, "weather_map_url", WEATHER_MAP_URL)


# ── Site A tests (weather-map HTTP contract) ──────────────────────────────────

@respx.mock
@pytest.mark.usefixtures("mock_driver")
def test_fetch_parcel_weather_stats_calls_weather_map_and_returns_metrics(monkeypatch):
    """weather-map path: GET /stats/{id}?metrics=... returns the metrics sub-dict."""
    monkeypatch.setenv("HMAC_SECRET", "s3cret")
    parcel = "urn:ngsi-ld:AgriParcel:P1"
    route = respx.get(f"{WEATHER_MAP_URL}/api/weather-map/stats/{parcel}").mock(
        return_value=httpx.Response(200, json={
            "parcel_geojson": {},
            "date": "2026-06-30",
            "metrics": {
                "temperature_avg": {"heat_stress_pct": 5.0, "mean": 22.0},
                "water_balance": {"deficit_area_pct": 40.0, "mean": -10.0},
                "frost_risk": {"high_risk_pct": 0.0, "mean": 5.0},
            },
        })
    )
    result = asyncio.run(
        GraphDAO.fetch_parcel_weather_stats(parcel, "demo-coop")
    )
    assert result == {
        "temperature_avg": {"heat_stress_pct": 5.0, "mean": 22.0},
        "water_balance": {"deficit_area_pct": 40.0, "mean": -10.0},
        "frost_risk": {"high_risk_pct": 0.0, "mean": 5.0},
    }, f"Got: {result!r}"
    assert route.called, "weather-map GET was not called"
    request = respx.calls[0].request
    assert request.url.params["metrics"] == "temperature_avg,water_balance,frost_risk"
    assert request.headers["x-tenant-id"] == "demo-coop"
    assert request.headers["x-user-id"] == "bioorchestrator-worker"
    # module signature: empty token, bound to the tenant
    assert verify_hmac_signature(
        "s3cret", request.headers["x-auth-signature"], "", "demo-coop"
    )


@respx.mock
@pytest.mark.usefixtures("mock_driver")
def test_fetch_parcel_weather_stats_returns_none_on_404():
    """Returns None when weather-map has no parcel/COG (404 or empty metrics)."""
    parcel = "urn:ngsi-ld:AgriParcel:P2"
    respx.get(f"{WEATHER_MAP_URL}/api/weather-map/stats/{parcel}").mock(
        return_value=httpx.Response(404, json={"detail": "Parcel not found"})
    )
    result = asyncio.run(
        GraphDAO.fetch_parcel_weather_stats(parcel, "test-tenant")
    )
    assert result is None, f"Expected None, got {result!r}"


@respx.mock
@pytest.mark.usefixtures("mock_driver")
def test_fetch_parcel_weather_stats_returns_none_when_unreachable():
    """Returns None (no exception) when weather-map is unreachable."""
    parcel = "urn:ngsi-ld:AgriParcel:P3"
    respx.get(f"{WEATHER_MAP_URL}/api/weather-map/stats/{parcel}").mock(
        side_effect=httpx.ConnectError("unreachable")
    )
    result = asyncio.run(
        GraphDAO.fetch_parcel_weather_stats(parcel, "tenant-x")
    )
    assert result is None, f"Expected None, got {result!r}"
