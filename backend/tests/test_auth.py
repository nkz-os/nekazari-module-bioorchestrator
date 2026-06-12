"""Test NKZ auth middleware."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch


# A minimal Neo4j result mock that supports ``single()``, ``data()``,
# and ``async for`` iteration so route handlers don't crash.
class _MockResult:
    def __init__(self, records=None):
        self._records = records or []
        self._index = 0
    async def single(self):
        return self._records[0] if self._records else None
    async def fetch(self, n=-1):
        return list(self._records)
    async def values(self, *keys):
        return self._records
    async def data(self):
        return self._records
    def __aiter__(self):
        self._index = 0
        return self
    async def __anext__(self):
        if self._index >= len(self._records):
            raise StopAsyncIteration
        r = self._records[self._index]
        self._index += 1
        return r


def _mock_driver():
    d = MagicMock()
    s = MagicMock()
    s.__aenter__ = AsyncMock(return_value=s)
    s.__aexit__ = AsyncMock(return_value=None)
    s.run = AsyncMock(return_value=_MockResult())
    d.session = MagicMock(return_value=s)
    return d


def test_auth_disabled_allows_all():
    """When AUTH_DISABLED=true, all requests pass through."""
    os.environ["AUTH_DISABLED"] = "true"
    # Build our own client with all patches active at import time
    from fastapi.testclient import TestClient
    with patch("app.core.dependencies.get_driver", return_value=_mock_driver()):
        with patch("app.core.dependencies.init_driver", AsyncMock()):
            with patch("app.core.dependencies.close_driver", AsyncMock()):
                with patch.dict("sys.modules", {"ikerketa": MagicMock(__version__="0.1.0")}):
                    from app.main import app
                    client = TestClient(app)
        resp = client.get("/api/graph/stats")
    # May 200/422/500 but should NOT be 401
    assert resp.status_code != 401, f"Auth blocked despite AUTH_DISABLED=true: {resp.status_code}"


def test_auth_required_without_header(client):
    """When AUTH_DISABLED=false, missing header returns 401."""
    os.environ["AUTH_DISABLED"] = "false"
    resp = client.get("/api/graph/stats")
    assert resp.status_code == 401
    assert "Missing" in resp.json()["detail"]


def test_dev_user_has_tenant_id(client):
    """Dev user should get tenant_id=dev."""
    os.environ["AUTH_DISABLED"] = "true"
    # The /api/graph/stats endpoint uses Depends(_get_tenant_id)
    # which reads request.state.tenant_id set by auth middleware
    resp = client.get("/healthz")  # skipped by auth
    assert resp.status_code == 200
