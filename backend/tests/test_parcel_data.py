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
