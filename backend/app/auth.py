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


# Endpoints that don't require auth
SKIP_AUTH_PATHS = {"/healthz", "/readyz", "/docs", "/openapi.json"}


class NKZAuthMiddleware(BaseHTTPMiddleware):
    """Validate JWT tokens from the NKZ platform.

    In development (AUTH_DISABLED=true), all requests pass through.
    In production, validates Bearer token against Keycloak JWKS.
    """

    async def dispatch(self, request: Request, call_next: Callable):
        # Skip auth for health checks
        if request.url.path in SKIP_AUTH_PATHS:
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
            audience=os.getenv("KEYCLOAK_CLIENT_ID", "nekazari-frontend"),
        )
