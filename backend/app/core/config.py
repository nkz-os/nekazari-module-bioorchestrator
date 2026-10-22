"""Application configuration — Pydantic v2 Settings.

All values can be overridden via environment variables or a .env file.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

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
    keycloak_jwks_url: str = ""
    keycloak_client_id: str = "nekazari-frontend"

    # ── CORS ──────────────────────────────────────────────────────────────────
    cors_origins: str = "http://localhost:5173"

    @property
    def allowed_origins(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    # ── Redis ─────────────────────────────────────────────────────────────────
    # Best-effort stores: pipeline history and progress streams, crop events.
    # The background queue reads REDIS_URL on its own (app/workers/queue.py).
    redis_host: str = "redis-service:6379"
    redis_password: str = ""

    @property
    def redis_cache_url(self) -> str:
        if self.redis_password:
            return f"redis://:{quote(self.redis_password, safe='')}@{self.redis_host}/0"
        return f"redis://{self.redis_host}/0"

    # ── Orion-LD ──────────────────────────────────────────────────────────────
    orion_ld_url: str = "http://orion-ld-service:1026"
    context_url: str = "http://api-gateway-service:5000/ngsi-ld-context.json"
    # Canonical Orion-LD tenant owning the global AgriCrop reference catalog.
    # bioorch is the SOLE writer; other modules consume via /api/crop/catalog.
    catalog_tenant: str = "shared"

    # ── Entity-manager (parcel crop cycles: season start, campaign) ───────────
    entity_manager_url: str = "http://entity-manager-service:5000"

    # ── Weather-Map (per-parcel meteo stats: temperature_avg, water_balance, frost_risk) ─
    weather_map_url: str = "http://weather-map-backend:8080"

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = "INFO"  # level for this module's own loggers; see app.logging_setup

    # ── IkerKeta data paths ───────────────────────────────────────────────────
    ikerketa_data_dir: Path = Path("./data/processed")

    # ── DAD-IS API ────────────────────────────────────────────────────────────
    dadis_api_url: str = "https://us-central1-fao-dadis-dev.cloudfunctions.net/api/v1"
    dadis_api_token: str = ""

    # ── IUCN Red List API ─────────────────────────────────────────────────────


settings = Settings()
