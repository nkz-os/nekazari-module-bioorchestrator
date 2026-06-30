"""Tests for _fetch_ropo_products — CUE ROPO safer-alternatives helper."""

from __future__ import annotations

import pytest
from unittest.mock import patch
from app.graph import dao as dao_mod


@pytest.mark.asyncio
async def test_fetch_ropo_products_returns_list(monkeypatch):
    monkeypatch.setenv("INTERNAL_SERVICE_SECRET", "s3cr3t")

    class _Resp:
        status_code = 200

        def json(self):
            return [{"nombre_comercial": "Prod A"}]

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, *a, **k):
            return _Resp()

    with patch("httpx.AsyncClient", return_value=_Client()):
        out = await dao_mod._fetch_ropo_products("trigo", "montiko")
    assert out == [{"nombre_comercial": "Prod A"}]


@pytest.mark.asyncio
async def test_fetch_ropo_products_failopen():
    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, *a, **k):
            raise RuntimeError("down")

    with patch("httpx.AsyncClient", return_value=_Client()):
        out = await dao_mod._fetch_ropo_products("trigo", "montiko")
    assert out == []
