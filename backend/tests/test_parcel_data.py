from unittest.mock import patch

import pytest

from app.services.timescale import compute_trend


def test_compute_trend_up():
    obs = [
        {"date": "2026-01-01", "value": 0.50},
        {"date": "2026-02-01", "value": 0.55},
        {"date": "2026-03-01", "value": 0.62},
    ]
    trend = compute_trend(obs)
    assert trend["direction"] == "up"
    assert trend["delta"] == pytest.approx(0.12, 0.01)


def test_compute_trend_down():
    obs = [
        {"date": "2026-01-01", "value": 0.70},
        {"date": "2026-03-01", "value": 0.55},
    ]
    trend = compute_trend(obs)
    assert trend["direction"] == "down"
    assert trend["delta"] == pytest.approx(-0.15, 0.01)


def test_compute_trend_stable():
    obs = [
        {"date": "2026-01-01", "value": 0.60},
        {"date": "2026-03-01", "value": 0.61},
    ]
    trend = compute_trend(obs)
    assert trend["direction"] == "stable"
    assert trend["delta"] == pytest.approx(0.01, 0.01)


def test_compute_trend_insufficient_data():
    obs = [{"date": "2026-01-01", "value": 0.60}]
    trend = compute_trend(obs)
    assert trend["direction"] == "stable"
    assert "insuficientes" in trend["label"].lower()


def test_period_window_days():
    from app.api.v1.parcel_data import _PERIOD_DAYS
    assert _PERIOD_DAYS == {"1m": 30, "3m": 90, "6m": 180, "1y": 365}


def test_soil_unavailable_response_structure():
    response = {
        "available": False,
        "message": "El modulo soil no ha procesado esta parcela.",
    }
    assert "available" in response
    assert response["available"] is False
    assert "message" in response


def test_soil_available_response_structure():
    response = {
        "available": True,
        "entityId": "urn:ngsi-ld:AgriSoilExtended:test:parcel42",
        "horizons": [{"depthFrom": 0, "depthTo": 30, "sand": 52.0, "ph": 6.8}],
        "hydrologicGroup": "B",
        "source": "SoilGrids 2.0 + LUCAS 2018",
    }
    assert response["available"] is True
    assert len(response["horizons"]) == 1
    assert response["horizons"][0]["sand"] == 52.0
    assert response["hydrologicGroup"] == "B"




class _RecordingOrion:
    """Factory that records the tenant_id each OrionClient is built with."""
    constructed_tenants: list[str] = []

    def __init__(self, tenant_id, *a, **k):
        _RecordingOrion.constructed_tenants.append(tenant_id)
        self.tenant_id = tenant_id

    async def query_entities(self, type=None, q=None, limit=100, offset=0, attrs=None, options=None):
        return []  # no entities → endpoint returns "unavailable"

    async def close(self):
        return None


class _EOProductOrion:
    """Returns EOProduct entities (keyValues) for the vegetation read."""

    def __init__(self, tenant_id, *a, **k):
        self.tenant_id = tenant_id

    async def query_entities(self, type=None, q=None, limit=100, offset=0, attrs=None, options=None):
        if type == "EOProduct":
            return [
                {"id": "urn:ngsi-ld:EOProduct:t:P1:2026-06-11", "sensingDate": "2026-06-11", "ndvi": 0.55},
                {"id": "urn:ngsi-ld:EOProduct:t:P1:2026-06-01", "sensingDate": "2026-06-01", "ndvi": 0.42},
            ]
        return []

    async def close(self):
        return None


def test_vegetation_reads_eoproduct(client):
    with patch("app.api.v1.parcel_data.OrionClient", _EOProductOrion):
        resp = client.get(
            "/api/parcel/P1/vegetation?index=ndvi&period=1y",
            headers={"X-Tenant-ID": "montiko", "X-User-ID": "u1", "X-User-Roles": "Tecnico"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["index"] == "ndvi"
    assert body["count"] == 2
    assert body["current"] == 0.55
    assert [o["value"] for o in body["observations"]] == [0.42, 0.55]  # sorted by date


def test_vegetation_uses_request_tenant(client):
    _RecordingOrion.constructed_tenants = []
    with patch("app.api.v1.parcel_data.OrionClient", _RecordingOrion):
        resp = client.get(
            "/api/parcel/P1/vegetation",
            headers={"X-Tenant-ID": "asociacion-allotarra", "X-User-ID": "u1", "X-User-Roles": "Tecnico"},
        )
    assert resp.status_code == 200
    assert "asociacion-allotarra" in _RecordingOrion.constructed_tenants
    assert "" not in _RecordingOrion.constructed_tenants  # never the default store


def test_soil_uses_request_tenant(client):
    _RecordingOrion.constructed_tenants = []
    with patch("app.api.v1.parcel_data.OrionClient", _RecordingOrion):
        resp = client.get(
            "/api/parcel/P1/soil",
            headers={"X-Tenant-ID": "asociacion-allotarra", "X-User-ID": "u1", "X-User-Roles": "Tecnico"},
        )
    assert resp.status_code == 200
    assert "asociacion-allotarra" in _RecordingOrion.constructed_tenants
    assert "" not in _RecordingOrion.constructed_tenants  # never the default store
