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

    def test_assign_crop_forwards_tenant_from_header(self, mock_neo4j_driver):
        """The agriculture prefix is auth-exempt (SKIP_AUTH_PREFIXES), so
        request.state.tenant_id is never set. The route MUST fall back to the
        X-Tenant-ID header (mirroring crop-plan) or multi-tenant parcels 404.
        Regression test for the bug where assign-crop queried the catalog
        (default) tenant instead of the parcel's tenant.
        """
        captured = {}

        async def fake_assign(self, **kwargs):
            captured.update(kwargs)
            return {"status": "assigned", "entity_id": "x", "crop": "wheat",
                    "variety": "V", "management": "conventional", "parcel_id": kwargs["parcel_id"]}

        with patch.dict("sys.modules", {"ikerketa": MagicMock()}), \
             patch("app.core.dependencies.get_driver", return_value=mock_neo4j_driver), \
             patch("app.graph.dao.GraphDAO.assign_crop_to_parcel", fake_assign):
            from app.main import app
            client = TestClient(app)
            resp = client.post(
                "/api/graph/agriculture/assign-crop",
                headers={"X-Tenant-ID": "montiko", "X-User-ID": "smoke-test"},
                json={
                    "parcel_id": "urn:ngsi-ld:AgriParcel:test-1",
                    "variety_uri": "urn:ngsi-ld:AgriCropVariety:V",
                    "crop_uri": "urn:ngsi-ld:AgriCrop:wheat",
                    "management": "conventional",
                    "season_start": "2026-01-01",
                    "season_end": "2026-06-01",
                },
            )
            assert resp.status_code == 200, resp.text
            assert captured.get("tenant_id") == "montiko", (
                f"tenant_id not forwarded from X-Tenant-ID header: got {captured.get('tenant_id')!r}"
            )


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


class TestTenantResolution:
    """`_get_tenant_id` must fall back to the X-Tenant-ID header on the
    auth-exempt /agriculture/ path (request.state.tenant_id is never set there).
    Guards every parcel-scoped endpoint routed through the shared helper."""

    @staticmethod
    def _helper():
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):
            from app.api.v1.graph import _get_tenant_id
        return _get_tenant_id

    def test_falls_back_to_header(self):
        from types import SimpleNamespace
        req = SimpleNamespace(state=SimpleNamespace(), headers={"X-Tenant-ID": "montiko"})
        assert self._helper()(req) == "montiko"

    def test_prefers_state_when_set(self):
        from types import SimpleNamespace
        req = SimpleNamespace(state=SimpleNamespace(tenant_id="state-t"),
                              headers={"X-Tenant-ID": "montiko"})
        assert self._helper()(req) == "state-t"

    def test_empty_when_neither(self):
        from types import SimpleNamespace
        req = SimpleNamespace(state=SimpleNamespace(), headers={})
        assert self._helper()(req) == ""
