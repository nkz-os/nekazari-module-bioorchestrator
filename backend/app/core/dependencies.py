"""FastAPI dependency injection — Neo4j AsyncDriver factory."""

from __future__ import annotations

from typing import AsyncGenerator

from neo4j import AsyncDriver, AsyncGraphDatabase

from app.core.config import settings

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
