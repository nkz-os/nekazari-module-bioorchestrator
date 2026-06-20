"""NKZ Authentication Middleware — JWT validation from platform.

Validates the Authorization header against the NKZ Keycloak instance.
Skips auth for health check endpoints.
"""

from __future__ import annotations

import os
from app.common.tenant_utils import normalize_tenant_id
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


# Endpoints that don't require auth — health probes, docs, and public reference data
SKIP_AUTH_PATHS = {"/healthz", "/readyz", "/docs", "/openapi.json"}

# Public reference data endpoints — global knowledge graph, no tenant-specific data.
# These are scientific reference data (EPPO codes, phenology params, crop catalog)
# available to all users regardless of auth state.
SKIP_AUTH_PREFIXES = [
    "/api/graph/agriculture/",
    "/api/graph/species",
    "/api/graph/phenology-params",
    "/api/graph/phenology-stages",
    "/api/graph/action-rules",
    "/api/graph/heat-tolerance",
    "/api/graph/nutrient-profile",
    "/api/graph/soil-suitability",
    "/api/graph/rotation-constraints",
    "/api/graph/recommendations/",
    "/api/graph/varieties",
    "/api/crop/catalog",
    "/api/v1/sources",
    "/api/v1/catalog",
    "/api/v1/capability",
    "/ngsi-ld/",
    # Orion-LD subscription notifications POST here directly (in-cluster, no
    # api-gateway, no JWT). NetworkPolicy gates ingress. Without this the
    # production auth branch 401s every notification and the catalog sync dies.
    "/api/ngsi-ld/",
]


class NKZAuthMiddleware(BaseHTTPMiddleware):
    """Validate JWT tokens from the NKZ platform.

    In development (AUTH_DISABLED=true), all requests pass through.
    In production, validates Bearer token against Keycloak JWKS.
    """

    async def dispatch(self, request: Request, call_next: Callable):
        # Skip auth for health checks and public reference data
        if request.url.path in SKIP_AUTH_PATHS:
            return await call_next(request)
        for prefix in SKIP_AUTH_PREFIXES:
            if request.url.path.startswith(prefix):
                return await call_next(request)

        # Trust gateway-injected headers (request already passed api-gateway auth)
        gateway_tenant = request.headers.get("X-Tenant-ID", "")
        gateway_user = request.headers.get("X-User-ID", "")
        gateway_roles = request.headers.get("X-User-Roles", "")
        if gateway_tenant and gateway_user:
            request.state.user = {
                "sub": gateway_user,
                "tenant_id": gateway_tenant,
                "roles": gateway_roles.split(",") if gateway_roles else [],
            }
            request.state.tenant_id = normalize_tenant_id(gateway_tenant)
            return await call_next(request)

        # Development mode: skip auth
        if os.getenv("AUTH_DISABLED", "false").lower() == "true":
            request.state.user = {
                "sub": "dev-user",
                "roles": ["PlatformAdmin"],
                "tenant_id": "dev",
            }
            request.state.tenant_id = "dev"
            return await call_next(request)

        # Production: validate JWT
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid Authorization header"},
            )

        token = auth_header.split(" ", 1)[1]

        try:
            payload = await self._validate_token(token)
            request.state.user = payload
            # Extract tenant_id following platform convention:
            # canonical attribute is 'tenant_id' (underscore), fallback 'tenant'
            raw_tenant = payload.get("tenant_id") or payload.get("tenant", "")
            request.state.tenant_id = normalize_tenant_id(raw_tenant) if raw_tenant else ""
        except Exception as e:
            return JSONResponse(
                status_code=401,
                content={"detail": f"Token validation failed: {e}"},
            )

        return await call_next(request)

    async def _validate_token(self, token: str) -> dict:
        """Validate JWT against Keycloak JWKS endpoint.

        In production, uses python-jose or PyJWT with Keycloak's
        JWKS endpoint for RS256 signature verification.
        """
        import jwt

        keycloak_url = os.getenv(
            "KEYCLOAK_JWKS_URL",
            "https://auth.robotika.cloud/auth/realms/nekazari/protocol/openid-connect/certs",
        )

        # Fetch JWKS and validate
        # For now, decode without verification in non-prod
        if os.getenv("AUTH_STRICT", "true").lower() != "true":
            return jwt.decode(token, options={"verify_signature": False})

        # Production: full verification
        from jwt import PyJWKClient

        jwks_client = PyJWKClient(keycloak_url)
        signing_key = jwks_client.get_signing_key_from_jwt(token)

        return jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            options={"verify_aud": False},
        )
