"""Tests for GET /api/graph/agriculture/crop-name (EPPO→common name resolver)."""

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_crop_name_known_eppo():
    r = client.get("/api/graph/agriculture/crop-name", params={"eppo": "TRZAX"})
    assert r.status_code == 200
    body = r.json()
    assert body["slug"] == "wheat"
    assert body["name"] == "trigo"


def test_crop_name_unknown_eppo():
    r = client.get("/api/graph/agriculture/crop-name", params={"eppo": "ZZZZZ"})
    assert r.status_code == 404


def test_old_pesticides_endpoint_removed():
    r = client.get("/api/graph/agriculture/pesticides", params={"crop_eppo": "TRZAX"})
    assert r.status_code == 404


def test_crop_name_lowercase_eppo():
    r = client.get("/api/graph/agriculture/crop-name", params={"eppo": "trzax"})
    assert r.status_code == 200
    assert r.json()["slug"] == "wheat"
