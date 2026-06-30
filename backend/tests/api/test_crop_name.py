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
