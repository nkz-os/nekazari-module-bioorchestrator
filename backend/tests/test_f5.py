"""Tests for F5: Water Budget endpoint."""
from unittest.mock import MagicMock, patch
from neo4j import AsyncDriver


class TestWaterBudget:
    """GET /api/graph/agriculture/water-budget"""

    def test_water_budget_missing_parcel_id(self):
        """Returns 422 when parcel_id missing."""
        driver = MagicMock(spec=AsyncDriver)
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):
            with patch("app.core.dependencies.get_driver", return_value=driver):
                from fastapi.testclient import TestClient
                from app.main import app
                client = TestClient(app)
                resp = client.get("/api/graph/agriculture/water-budget")
                assert resp.status_code == 422
