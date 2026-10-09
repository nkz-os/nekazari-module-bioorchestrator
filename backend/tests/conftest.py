"""Shared fixtures for bioorchestrator tests."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.graph.dao import GraphDAO
from neo4j import AsyncDriver


@pytest.fixture(autouse=True)
def _env():
    """Ensure tests never touch real services."""
    os.environ.setdefault("AUTH_DISABLED", "true")
    os.environ.setdefault("AUTH_STRICT", "false")
    os.environ.setdefault("NEO4J_URI", "bolt://localhost:7687")
    os.environ.setdefault("NEO4J_USER", "neo4j")
    os.environ.setdefault("NEO4J_PASSWORD", "test")
    os.environ.setdefault("CORS_ORIGINS", "http://localhost:5173")
    os.environ.setdefault("DADIS_API_TOKEN", "")
    yield


@pytest.fixture(autouse=True)
def _clean_similarity_env(monkeypatch):
    """Isolate tests from the developer shell; tests that need them set them."""
    monkeypatch.delenv("AGROCLIMATIC_VECTOR", raising=False)
    monkeypatch.delenv("CHELSA_PARCEL_CLIMATE_ENABLED", raising=False)


@pytest.fixture(autouse=True)
def _no_chelsa_network(request, monkeypatch):
    """Tests must never reach CHELSA unless marked `network`."""
    if request.node.get_closest_marker("network"):
        return

    def _blocked(*args, **kwargs):
        raise RuntimeError("network disabled in tests")

    monkeypatch.setattr("app.services.chelsa_climate._rasterio_sampler", _blocked)


@pytest.fixture(autouse=True)
def _no_crop_cycles_network(request, monkeypatch):
    """Tests never call entity-manager; those that need cycles patch ``app.graph.dao.fetch_crop_cycles``."""
    if request.node.get_closest_marker("network"):
        return
    monkeypatch.setattr("app.graph.dao.fetch_crop_cycles", AsyncMock(return_value=None))


@pytest.fixture
def mock_driver() -> AsyncDriver:
    """Return a mock Neo4j AsyncDriver."""
    driver = MagicMock(spec=AsyncDriver)
    driver.session.return_value.__aenter__ = AsyncMock()
    driver.session.return_value.__aexit__ = AsyncMock()
    return driver


@pytest.fixture
def client() -> TestClient:
    """FastAPI TestClient with IkerKeta import skipped."""
    with patch.dict("sys.modules", {"ikerketa": MagicMock(__version__="0.1.0")}), \
         patch("app.core.dependencies.init_driver", AsyncMock()), \
         patch("app.core.dependencies.close_driver", AsyncMock()):
        from app.main import app

        yield TestClient(app)


async def batch_via_per_crop(self, crops, similar_sites, irrigation_regime=None, top_n=10, **kw):
    """Stand-in for extrapolate_varieties_batch over the (mocked) per-crop method.

    The batch is defined as per-crop extrapolate_varieties; that equivalence is
    proven on real Neo4j in tests/graph/test_extrapolate_batch.py.
    """
    return {c: (await self.extrapolate_varieties(
        crop=c, irrigation_regime=irrigation_regime, top_n=top_n,
        similar_sites_override=similar_sites, **kw))["ranked_varieties"]
        for c in dict.fromkeys(crops)}


@pytest.fixture(autouse=True)
def _batch_via_per_crop(request):
    """Mock-driven recommend tests exercise extrapolate_varieties_batch via this stand-in.

    Tests marked `real_batch` (the real-Neo4j equivalence suite) run the real method.
    """
    if request.node.get_closest_marker("real_batch"):
        yield
        return
    with patch.object(GraphDAO, "extrapolate_varieties_batch", batch_via_per_crop):
        yield
