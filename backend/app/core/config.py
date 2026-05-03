"""Application configuration — Pydantic v2 Settings.

All values can be overridden via environment variables or a .env file.
"""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Neo4j ─────────────────────────────────────────────────────────────────
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "bioorchestrator"

    # ── Auth ──────────────────────────────────────────────────────────────────
    auth_disabled: bool = False
    auth_strict: bool = True
    keycloak_jwks_url: str = (
        "https://auth.robotika.cloud/realms/nekazari/protocol/openid-connect/certs"
    )
    keycloak_client_id: str = "nekazari-frontend"

    # ── CORS ──────────────────────────────────────────────────────────────────
    cors_origins: str = "https://nekazari.robotika.cloud,http://localhost:5173"

    @property
    def allowed_origins(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    # ── IkerKeta data paths ───────────────────────────────────────────────────
    ikerketa_data_dir: Path = Path("./data/processed")

    # ── DAD-IS API ────────────────────────────────────────────────────────────
    dadis_api_url: str = "https://us-central1-fao-dadis-dev.cloudfunctions.net/api/v1"
    dadis_api_token: str = ""

    # ── IUCN Red List API ─────────────────────────────────────────────────────


settings = Settings()
