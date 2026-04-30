"""DAD-IS FAO Service for BioOrchestrator.

Handles HTTP requests to DAD-IS Interoperability API.
Uses httpx.AsyncClient — non-blocking for FastAPI event loop.
Token is pending FAO approval.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from app.core.config import settings

logger = logging.getLogger("bioorchestrator.dadis")

MAX_RETRIES = 3
RETRY_BACKOFF = 1.0  # seconds
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
REQUEST_TIMEOUT = 15.0


class DadisAPIError(Exception):
    """Exception raised for errors in DAD-IS API communication."""


class DadisClient:
    """Async client for DAD-IS Interoperability API.

    Creates a new httpx.AsyncClient per request for isolation.
    For high-throughput scenarios, inject a shared client via lifespan.
    """

    def __init__(self) -> None:
        self.base_url = settings.dadis_api_url.rstrip("/")
        self.token = settings.dadis_api_token

    def _headers(self) -> dict[str, str]:
        if not self.token:
            logger.warning("DADIS_API_TOKEN is not set — requests may fail")
        return {
            "Authorization": self.token,
            "Content-Type": "application/json",
        }

    async def _request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> Any:
        """Make an async HTTP request to DAD-IS with retry logic."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        last_exc: Exception | None = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                    logger.debug("DAD-IS %s %s (attempt %d)", method, url, attempt)
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=self._headers(),
                        **kwargs,
                    )

                if response.status_code in RETRYABLE_STATUSES and attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF * attempt
                    logger.warning(
                        "DAD-IS %d from %s — retrying in %.1fs (attempt %d/%d)",
                        response.status_code, url, wait, attempt, MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                logger.error("DAD-IS HTTP %d for %s: %s", status, url, e)
                last_exc = DadisAPIError(f"HTTP {status}: {e}")

            except httpx.TimeoutException:
                logger.error("DAD-IS timeout for %s (attempt %d)", url, attempt)
                last_exc = DadisAPIError(f"Timeout after {REQUEST_TIMEOUT}s")

            except httpx.ConnectError as e:
                logger.error("DAD-IS connection error for %s: %s", url, e)
                last_exc = DadisAPIError(f"Connection error: {e}")

            except Exception as e:
                logger.error("DAD-IS unexpected error: %s", e)
                last_exc = DadisAPIError(f"Unexpected error: {e}")

            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF * attempt)

        assert last_exc is not None
        raise last_exc

    async def get_breeds(
        self,
        classification: str = "all",
        country_ids: list[str] | None = None,
        species_ids: list[int] | None = None,
    ) -> Any:
        """Get breeds using the POST endpoint for filtering."""
        body: dict[str, Any] = {
            "classification": classification,
            "countryIds": country_ids or [],
            "speciesIds": species_ids or [],
        }
        return await self._request("POST", "breeds", json=body)

    async def get_breed_by_id(self, breed_id: str, lang: str = "en") -> Any:
        """Get breed information by ID."""
        return await self._request(
            "GET", f"breeds/{breed_id}", params={"lang": lang}
        )

    async def get_countries(self) -> Any:
        """Get all countries."""
        return await self._request("GET", "countries")

    async def get_species(self) -> Any:
        """Get all species."""
        return await self._request("GET", "species")


# Module-level instance — created per-request in API handlers.
# In production, inject a shared client via lifespan for connection pooling.
def get_dadis_client() -> DadisClient:
    return DadisClient()
