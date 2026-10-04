"""FastAPI dependency injection — Neo4j AsyncDriver factory."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Coroutine
from typing import Any

from fastapi import Depends, HTTPException, Request

from app.core.config import settings
from neo4j import AsyncDriver, AsyncGraphDatabase

# Module-level driver instance (created during lifespan, shared across requests)
_driver: AsyncDriver | None = None


def get_driver() -> AsyncDriver:
    """Return the active Neo4j AsyncDriver.

    Raises RuntimeError if called before the lifespan initialises the driver.
    """
    if _driver is None:
        raise RuntimeError("Neo4j driver not initialised — lifespan not started")
    return _driver


async def init_driver() -> AsyncDriver:
    """Create and verify the Neo4j AsyncDriver. Called from lifespan."""
    global _driver
    _driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    await _driver.verify_connectivity()
    return _driver


async def close_driver() -> None:
    """Close the Neo4j AsyncDriver. Called from lifespan on shutdown."""
    global _driver
    if _driver is not None:
        await _driver.close()
        _driver = None


async def get_neo4j_driver() -> AsyncGenerator[AsyncDriver, None]:
    """FastAPI dependency: yields the Neo4j AsyncDriver per request."""
    yield get_driver()


def get_dao() -> GraphDAO:  # noqa: F821 — GraphDAO imported inside body to avoid circular import
    """FastAPI dependency: returns GraphDAO wrapping the active Neo4j driver."""
    from app.graph.dao import GraphDAO
    return GraphDAO(get_driver())


# Platform role vocabulary (matches Keycloak realm roles and X-User-Roles).
ROLE_TECHNICAL_CONSULTANT = "TechnicalConsultant"
ROLE_TENANT_ADMIN = "TenantAdmin"
ROLE_PLATFORM_ADMIN = "PlatformAdmin"


async def get_current_user(request: Request) -> dict:
    """Return the identity NKZAuthMiddleware verified for this request.

    The middleware sets ``request.state.user`` only from a signed gateway
    request, the internal service secret, or a validated JWT. Nothing here
    reads headers, so an unauthenticated request (including one that reached
    a public prefix) has no identity and is rejected.
    """
    user = getattr(request.state, "user", None)
    if not isinstance(user, dict) or not user.get("sub"):
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def require_roles(*roles: str) -> Callable[..., Coroutine[Any, Any, dict]]:
    """Dependency factory: the verified user must hold at least one of ``roles``.

    401 if unauthenticated, 403 if authenticated without a listed role.
    Role names are matched exactly.
    """
    if not roles:
        raise ValueError("require_roles needs at least one role")
    allowed = frozenset(roles)

    async def _require_roles(user: dict = Depends(get_current_user)) -> dict:  # noqa: B008
        held = user.get("roles") or ()
        if not allowed.intersection(held):
            raise HTTPException(status_code=403, detail="Insufficient role")
        return user

    return _require_roles


# Writes to global (cross-tenant) data are PlatformAdmin-only; contributions for
# review are open to consultants and admins.
require_platform_admin = require_roles(ROLE_PLATFORM_ADMIN)
require_contributor = require_roles(
    ROLE_TECHNICAL_CONSULTANT, ROLE_TENANT_ADMIN, ROLE_PLATFORM_ADMIN
)
