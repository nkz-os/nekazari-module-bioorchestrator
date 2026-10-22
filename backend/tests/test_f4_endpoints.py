"""Tests for F4 endpoints: assign-crop, crop-context, yield-potential."""
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from neo4j import AsyncDriver


class TestCropContext:
    """GET /api/graph/agriculture/crop-context (endpoint added in Task 4)"""

    def test_crop_context_missing_parcel_id(self):
        """Returns 404 when endpoint not yet deployed."""
        driver = MagicMock(spec=AsyncDriver)
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):  # noqa: SIM117
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
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):  # noqa: SIM117
            with patch("app.core.dependencies.get_driver", return_value=driver):
                from app.main import app
                client = TestClient(app)
                resp = client.get("/api/graph/agriculture/yield-potential")
                # 404 until endpoint is registered (Task 7)
                assert resp.status_code in (422, 404)


class TestTenantResolution:
    """`_get_tenant_id` resolves the tenant only from the verified identity
    (request.state.tenant_id). Headers, query params and the parcel URN are
    caller-controlled and must be ignored."""

    @staticmethod
    def _helper():
        with patch.dict("sys.modules", {"ikerketa": MagicMock()}):
            from app.api.v1.graph import _get_tenant_id
        return _get_tenant_id

    @staticmethod
    def _qp(defaults=None):
        """ImmutableMultiDict-like query_params for test mocks."""
        from types import SimpleNamespace
        d = defaults or {}
        return SimpleNamespace(get=lambda k, default="": d.get(k, default))

    def test_ignores_header(self):
        from types import SimpleNamespace
        req = SimpleNamespace(
            state=SimpleNamespace(),
            headers={"X-Tenant-ID": "tenant-b"},
            query_params=self._qp(),
        )
        assert self._helper()(req) == ""

    def test_prefers_state_when_set(self):
        from types import SimpleNamespace
        req = SimpleNamespace(
            state=SimpleNamespace(tenant_id="state-t"),
            headers={"X-Tenant-ID": "tenant-b"},
            query_params=self._qp(),
        )
        assert self._helper()(req) == "state-t"

    def test_empty_when_neither(self):
        from types import SimpleNamespace
        req = SimpleNamespace(
            state=SimpleNamespace(),
            headers={},
            query_params=self._qp(),
        )
        assert self._helper()(req) == ""

    def test_ignores_tenant_id_query_param(self):
        from types import SimpleNamespace
        req = SimpleNamespace(
            state=SimpleNamespace(),
            headers={},
            query_params=self._qp({"tenant_id": "tenant-b"}),
        )
        assert self._helper()(req) == ""

    def test_ignores_parcel_urn(self):
        from types import SimpleNamespace
        req = SimpleNamespace(
            state=SimpleNamespace(),
            headers={},
            query_params=self._qp({"parcel_id": "urn:ngsi-ld:AgriParcel:tenant-b:parcela-42"}),
        )
        assert self._helper()(req) == ""
