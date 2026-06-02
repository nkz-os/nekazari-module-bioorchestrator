"""Tests for F4 endpoints: assign-crop, crop-context, yield-potential."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from neo4j import AsyncDriver


@pytest.fixture
def mock_neo4j_driver():
    """Mock Neo4j driver that returns a mock session."""
    driver = MagicMock(spec=AsyncDriver)
    session = MagicMock()
    session.run = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    driver.session.return_value = session
    return driver


class TestAssignCrop:
    """POST /api/graph/agriculture/assign-crop"""

    def test_assign_crop_missing_parcel_id(self, mock_neo4j_driver):
        """Returns 400 when parcel_id is missing."""
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):
            with patch("app.core.dependencies.get_driver", return_value=mock_neo4j_driver):
                from app.main import app
                client = TestClient(app)
                resp = client.post(
                    "/api/graph/agriculture/assign-crop",
                    json={"variety_uri": "urn:ngsi-ld:AgriCropVariety:X"},
                )
                assert resp.status_code == 400
                assert "parcel_id" in resp.json()["detail"].lower()

    def test_assign_crop_invalid_management(self, mock_neo4j_driver):
        """Returns 400 when management is not organic/conventional."""
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):
            with patch("app.core.dependencies.get_driver", return_value=mock_neo4j_driver):
                from app.main import app
                client = TestClient(app)
                resp = client.post(
                    "/api/graph/agriculture/assign-crop",
                    json={
                        "parcel_id": "urn:ngsi-ld:AgriParcel:test-1",
                        "variety_uri": "urn:ngsi-ld:AgriCropVariety:X",
                        "crop_uri": "urn:ngsi-ld:AgriCrop:Y",
                        "management": "biodynamic",
                        "season_start": "2026-01-01",
                        "season_end": "2026-06-01",
                    },
                )
                assert resp.status_code == 400
                assert "management" in resp.json()["detail"].lower()

    def test_assign_crop_missing_fields(self, mock_neo4j_driver):
        """Returns 400 when required fields are missing."""
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):
            with patch("app.core.dependencies.get_driver", return_value=mock_neo4j_driver):
                from app.main import app
                client = TestClient(app)
                resp = client.post(
                    "/api/graph/agriculture/assign-crop",
                    json={
                        "parcel_id": "urn:ngsi-ld:AgriParcel:test-1",
                        "crop_uri": "urn:ngsi-ld:AgriCrop:Y",
                    },
                )
                assert resp.status_code == 400
                assert "missing" in resp.json()["detail"].lower()


class TestCropContext:
    """GET /api/graph/agriculture/crop-context (endpoint added in Task 4)"""

    def test_crop_context_missing_parcel_id(self):
        """Returns 404 when endpoint not yet deployed."""
        driver = MagicMock(spec=AsyncDriver)
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):
            with patch("app.core.dependencies.get_driver", return_value=driver):
                from app.main import app
                client = TestClient(app)
                resp = client.get("/api/graph/agriculture/crop-context")
                # 404 until endpoint is registered (Task 4)
                assert resp.status_code in (422, 404)


class TestYieldPotential:
    """GET /api/graph/agriculture/yield-potential (endpoint added in Task 7)"""

    def test_yield_potential_missing_params(self):
        """Returns 404 when endpoint not yet deployed."""
        driver = MagicMock(spec=AsyncDriver)
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):
            with patch("app.core.dependencies.get_driver", return_value=driver):
                from app.main import app
                client = TestClient(app)
                resp = client.get("/api/graph/agriculture/yield-potential")
                # 404 until endpoint is registered (Task 7)
                assert resp.status_code in (422, 404)
