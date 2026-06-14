import pytest

from app.services.timescale import PERIOD_MAP, compute_trend


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


def test_period_map_values():
    assert PERIOD_MAP["1m"].days == 30
    assert PERIOD_MAP["3m"].days == 90
    assert PERIOD_MAP["6m"].days == 180
    assert PERIOD_MAP["1y"].days == 365


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

    async def query_entities(self, type=None, q=None, limit=100, offset=0, attrs=None):
        return []  # no entities → endpoint returns "unavailable"

    async def close(self):
        return None


def _get_route_globals(client, path):
    """Return the __globals__ dict of the actual registered route endpoint.

    The conftest uses patch.dict('sys.modules', ...) which is cleaned up after
    fixture creation, leaving the route endpoint's __globals__ pointing to an
    orphaned module dict that is different from what 'import app.api.v1.parcel_data'
    returns after fixture setup. We must patch the dict the live route uses.
    """
    for route in client.app.routes:
        if hasattr(route, "path") and route.path == path:
            return route.endpoint.__globals__
    raise RuntimeError(f"Route not found: {path}")


def test_vegetation_uses_request_tenant(client):
    _RecordingOrion.constructed_tenants = []
    route_globals = _get_route_globals(client, "/api/parcel/{parcel_id}/vegetation")
    original = route_globals["OrionClient"]
    route_globals["OrionClient"] = _RecordingOrion
    try:
        resp = client.get(
            "/api/parcel/P1/vegetation",
            headers={"X-Tenant-ID": "asociacion-allotarra", "X-User-ID": "u1", "X-User-Roles": "Tecnico"},
        )
    finally:
        route_globals["OrionClient"] = original
    assert resp.status_code == 200
    assert "asociacion-allotarra" in _RecordingOrion.constructed_tenants
    assert "" not in _RecordingOrion.constructed_tenants  # never the default store


def test_soil_uses_request_tenant(client):
    _RecordingOrion.constructed_tenants = []
    route_globals = _get_route_globals(client, "/api/parcel/{parcel_id}/soil")
    original = route_globals["OrionClient"]
    route_globals["OrionClient"] = _RecordingOrion
    try:
        resp = client.get(
            "/api/parcel/P1/soil",
            headers={"X-Tenant-ID": "asociacion-allotarra", "X-User-ID": "u1", "X-User-Roles": "Tecnico"},
        )
    finally:
        route_globals["OrionClient"] = original
    assert resp.status_code == 200
    assert "asociacion-allotarra" in _RecordingOrion.constructed_tenants
    assert "" not in _RecordingOrion.constructed_tenants  # never the default store
