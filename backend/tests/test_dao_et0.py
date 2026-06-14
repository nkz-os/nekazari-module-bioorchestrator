"""TDD: ET0 WeatherObserved lookup must use the request tenant, not the default store."""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from app.graph.dao import GraphDAO


class _FakeOrionClient:
    """Stub for OrionClient that records the tenant_id it was constructed with."""

    _constructed_with: list[str] = []

    def __init__(self, tenant_id: str) -> None:
        _FakeOrionClient._constructed_with.append(tenant_id)

    async def query_entities(self, *, type: str, limit: int = 1):  # noqa: A002
        return []

    async def close(self) -> None:
        pass


@pytest.fixture(autouse=True)
def _reset_stub():
    _FakeOrionClient._constructed_with = []
    yield


def test_fetch_weekly_eto_uses_request_tenant(mock_driver):
    """_fetch_weekly_eto must construct OrionClient(tenant_id) and return None when
    no WeatherObserved entities exist for that tenant."""

    with patch("app.graph.dao.OrionClient", _FakeOrionClient):
        dao = GraphDAO(mock_driver)
        result = asyncio.run(
            dao._fetch_weekly_eto(
                tenant_id="asociacion-allotarra",
                ws="2026-06-01T00:00:00Z",
                we="2026-06-08T00:00:00Z",
            )
        )

    assert result is None, f"Expected None, got {result!r}"
    assert _FakeOrionClient._constructed_with == ["asociacion-allotarra"], (
        f"OrionClient was constructed with wrong tenants: {_FakeOrionClient._constructed_with}"
    )
