"""Shared fixtures for bioorchestrator tests."""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
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
    with patch.dict("sys.modules", {"ikerketa": MagicMock(__version__="0.1.0")}):
        with patch("app.core.dependencies.init_driver", AsyncMock()):
            with patch("app.core.dependencies.close_driver", AsyncMock()):
                from app.main import app

                return TestClient(app)
