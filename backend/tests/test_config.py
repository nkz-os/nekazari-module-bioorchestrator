"""Test application configuration loading."""

from __future__ import annotations

import os


def test_settings_defaults():
    """Verify sensible defaults for local development."""
    from app.core.config import settings

    assert settings.neo4j_uri == "bolt://localhost:7687"
    assert settings.neo4j_user == "neo4j"
    assert settings.auth_disabled is False
    assert "robotika.cloud" in settings.keycloak_jwks_url
    assert settings.keycloak_client_id == "nekazari-frontend"


def test_allowed_origins_split():
    """CORS origins string is split correctly into a list."""
    from app.core.config import settings

    origins = settings.allowed_origins
    assert isinstance(origins, list)
    assert len(origins) >= 1


def test_dadis_api_url_configured():
    """DAD-IS API URL should use the FAO cloud function."""
    from app.core.config import settings

    assert "fao" in settings.dadis_api_url or "dadis" in settings.dadis_api_url.lower()
