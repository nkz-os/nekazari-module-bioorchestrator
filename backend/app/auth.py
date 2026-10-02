"""NKZ Authentication Middleware — JWT validation from platform.

Validates the Authorization header against the NKZ Keycloak instance.
Skips auth for health check endpoints.
"""

from __future__ import annotations

import functools
import hmac
import logging
import os
from collections.abc import Callable

import jwt
from jwt import PyJWKClient
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from nkz_platform_sdk.crypto import verify_hmac_signature

from app.auth_policy import requires_identity
from app.common.tenant_utils import normalize_tenant_id

logger = logging.getLogger(__name__)

# Endpoints that don't require auth — health probes, docs, and public reference data
SKIP_AUTH_PATHS = {"/healthz", "/readyz", "/docs", "/openapi.json"}

# Public reference data endpoints — global knowledge graph, no tenant-specific data.
# These are scientific reference data (EPPO codes, phenology params, crop catalog)
# available to all users regardless of auth state.
#
# Each prefix maps to the set of HTTP methods exempted from auth for that prefix.
# "*" means all methods are exempt — this is the legacy default, preserved here
# for every pre-existing entry so today's behavior is unchanged (the broader
# audit of which of these should also be method-restricted is a separate,
# deliberate follow-up — NOT part of this fix).
#
# "/api/graph/action-rules" is the one entry that is intentionally NOT "*":
# it carries the life-critical agronomic action-rule catalog. GET (read) stays
# public for the crop-health worker, but POST/PUT (agronomist edits) must
# require auth — an unauthenticated in-cluster pod must not be able to mutate
# the rule catalog.
SKIP_AUTH_PREFIXES: dict[str, set[str]] = {
    "/api/graph/agriculture/": {"*"},
    "/api/graph/species": {"*"},
    "/api/graph/phenology-params": {"*"},
    "/api/graph/phenology-stages": {"*"},
    "/api/graph/action-rules": {"GET"},
    "/api/graph/heat-tolerance": {"*"},
    "/api/graph/nutrient-profile": {"*"},
    "/api/graph/soil-suitability": {"*"},
    "/api/graph/rotation-constraints": {"*"},
    "/api/graph/recommendations/": {"*"},
    "/api/graph/varieties": {"*"},
    "/api/crop/catalog": {"*"},
    "/api/v1/sources": {"*"},
    "/api/v1/catalog": {"*"},
    "/api/v1/capability": {"*"},
    "/ngsi-ld/": {"*"},
    # Orion-LD subscription notifications POST here directly (in-cluster, no
    # api-gateway, no JWT). NetworkPolicy gates ingress. Without this the
    # production auth branch 401s every notification and the catalog sync dies.
    "/api/ngsi-ld/": {"*"},
    # Internal in-cluster receivers (e.g. phenology-update from crop-health Orion sub).
    # No JWT in sub notifications; NetworkPolicy gates ingress.
    "/api/graph/internal/": {"*"},
}



@functools.lru_cache(maxsize=4)
def _jwks_client(url: str) -> PyJWKClient:
    return PyJWKClient(url, cache_keys=True)


def _allowed_issuers() -> list[str]:
    return [i.strip() for i in os.getenv("JWT_ISSUERS", "").split(",") if i.strip()]


class NKZAuthMiddleware(BaseHTTPMiddleware):
    """Validate JWT tokens from the NKZ platform.

    In development (AUTH_DISABLED=true), all requests pass through.
    In production, validates Bearer token against Keycloak JWKS.
    """

    @staticmethod
    def _set_identity(request: Request, tenant: str, sub: str, roles: list[str]) -> None:
        tenant_id = normalize_tenant_id(tenant)
        request.state.tenant_id = tenant_id
        request.state.user = {"sub": sub, "tenant_id": tenant_id, "roles": roles}

    def _internal_identity(self, request: Request) -> bool:
        secret = os.getenv("INTERNAL_SERVICE_SECRET", "")
        provided = request.headers.get("X-Internal-Service-Secret", "")
        tenant = request.headers.get("X-Tenant-ID", "").strip()
        if not secret or not provided or not tenant:
            return False
        # The secret is shared org-wide, so it must never be honoured from the
        # internet. The ingress always sets X-Forwarded-For / X-Real-Ip, while
        # in-cluster callers reach this service by its service DNS name without
        # them: their presence means the request came through the ingress.
        if "x-forwarded-for" in request.headers or "x-real-ip" in request.headers:
            return False
        try:
            if not hmac.compare_digest(provided, secret):
                return False
        except TypeError:  # non-ASCII header value
            return False
        caller = request.headers.get("X-User-ID", "").strip() or "internal"
        self._set_identity(request, tenant, f"service:{caller}", [])
        return True

    def _gateway_identity(self, request: Request) -> bool:
        tenant = request.headers.get("X-Tenant-ID", "").strip()
        user = request.headers.get("X-User-ID", "").strip()
        signature = request.headers.get("X-Auth-Signature", "")
        if not tenant or not user or not signature:
            return False
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if not token:
            return False
        try:
            valid = verify_hmac_signature(
                os.getenv("HMAC_SECRET", ""), signature, token, tenant, fail_open=False
            )
        except TypeError:  # non-ASCII header value
            return False
        if not valid:
            return False
        roles_header = request.headers.get("X-User-Roles", "")
        self._set_identity(
            request, tenant, user, roles_header.split(",") if roles_header else []
        )
        return True

    async def dispatch(self, request: Request, call_next: Callable):
        # Skip auth for health checks and public reference data
        if request.url.path in SKIP_AUTH_PATHS:
            return await call_next(request)

        needs_identity = requires_identity(
            request.url.path, request.method, request.query_params
        )
        for prefix, methods in SKIP_AUTH_PREFIXES.items():
            if request.url.path.startswith(prefix) and (
                "*" in methods or request.method in methods
            ):
                if not needs_identity:
                    # Public reference route: resolve the tenant from a valid
                    # token best-effort, never reject.
                    await self._soft_set_tenant_from_token(request)
                    return await call_next(request)
                break  # public prefix, but tenant data: identity chain below

        if self._internal_identity(request) or self._gateway_identity(request):
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
            if needs_identity and not request.state.tenant_id:
                return JSONResponse(
                    status_code=401, content={"detail": "Token carries no tenant"}
                )
        except Exception as e:  # noqa: BLE001
            logger.warning("JWT validation failed: %s", type(e).__name__)
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing credentials"},
            )

        return await call_next(request)

    async def _soft_set_tenant_from_token(self, request: Request) -> None:
        """Best-effort tenant resolution for public, tenant-scoped routes.

        Never rejects the request: on any failure it proceeds unauthenticated
        and the tenant stays empty. Routes never read the tenant from headers,
        query params or the parcel URN; tenant-scoped handlers reject an empty one.
        """
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return
        try:
            payload = await self._validate_token(auth_header.split(" ", 1)[1])
        except Exception:  # noqa: BLE001 — public route: never reject on token errors
            return
        raw_tenant = payload.get("tenant_id") or payload.get("tenant", "")
        if raw_tenant:
            request.state.tenant_id = normalize_tenant_id(raw_tenant)

    async def _validate_token(self, token: str) -> dict:
        """Validate a Keycloak RS256 JWT: signature via JWKS, exact issuer whitelist, expiry."""
        if os.getenv("AUTH_STRICT", "true").strip().lower() == "false":
            logger.critical(
                "AUTH_STRICT=false: accepting a JWT WITHOUT signature verification"
            )
            return jwt.decode(token, options={"verify_signature": False})

        jwks_url = os.getenv("KEYCLOAK_JWKS_URL", "").strip()
        issuers = _allowed_issuers()
        if not jwks_url or not issuers:
            raise RuntimeError("JWT validation not configured (KEYCLOAK_JWKS_URL / JWT_ISSUERS)")

        signing_key = _jwks_client(jwks_url).get_signing_key_from_jwt(token)
        return jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=issuers,
            options={"verify_aud": False, "require": ["exp", "iss"]},
        )
