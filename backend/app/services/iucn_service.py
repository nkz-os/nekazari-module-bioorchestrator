"""IUCN Red List Service for BioOrchestrator.

Handles HTTP requests to IUCN Red List API v4.
Uses httpx.AsyncClient — non-blocking for FastAPI event loop.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from app.core.config import settings

logger = logging.getLogger("bioorchestrator.iucn")

MAX_RETRIES = 3
RETRY_BACKOFF = 1.0
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
REQUEST_TIMEOUT = 15.0


class IucnAPIError(Exception):
    """Exception raised for errors in IUCN API communication."""


class IucnClient:
    """Async client for IUCN Red List API v4."""

    def __init__(self) -> None:
        self.base_url = settings.iucn_api_url.rstrip("/")
        self.token = settings.iucn_api_token

    def _headers(self) -> dict[str, str]:
        if not self.token:
            logger.warning("IUCN_API_TOKEN is not set — requests may fail")
        return {
            "Authorization": self.token,
            "Accept": "application/json",
        }

    async def _request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> Any:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        last_exc: Exception | None = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                    logger.debug("IUCN %s %s (attempt %d)", method, url, attempt)
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=self._headers(),
                        **kwargs,
                    )

                if response.status_code in RETRYABLE_STATUSES and attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF * attempt
                    logger.warning(
                        "IUCN %d from %s — retrying in %.1fs (attempt %d/%d)",
                        response.status_code, url, wait, attempt, MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                logger.error("IUCN HTTP %d for %s: %s", status, url, e)
                last_exc = IucnAPIError(f"HTTP {status}: {e}")

            except httpx.TimeoutException:
                logger.error("IUCN timeout for %s (attempt %d)", url, attempt)
                last_exc = IucnAPIError(f"Timeout after {REQUEST_TIMEOUT}s")

            except httpx.ConnectError as e:
                logger.error("IUCN connection error for %s: %s", url, e)
                last_exc = IucnAPIError(f"Connection error: {e}")

            except Exception as e:
                logger.error("IUCN unexpected error: %s", e)
                last_exc = IucnAPIError(f"Unexpected error: {e}")

            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BACKOFF * attempt)

        assert last_exc is not None
        raise last_exc

    async def get_species_page(self, page: int = 0) -> Any:
        """Get a paginated list of all assessed species."""
        return await self._request("GET", f"species/page/{page}")

    async def get_species_by_name(self, species_name: str) -> Any:
        """Get assessment information for a species by scientific name."""
        return await self._request("GET", f"species/{species_name}")

    async def get_species_by_id(self, species_id: int) -> Any:
        """Get assessment information for a species by its internal IUCN ID."""
        return await self._request("GET", f"species/id/{species_id}")

    async def get_species_by_region(self, region_code: str, page: int = 0) -> Any:
        """Get species assessments for a given marine region."""
        return await self._request(
            "GET", f"species/region/{region_code}/page/{page}"
        )

    async def get_species_by_category(self, category_code: str, page: int = 0) -> Any:
        """Get species assessments by IUCN Red List category (CR, EN, VU, NT, LC, DD)."""
        return await self._request(
            "GET", f"species/category/{category_code}"
        )

    async def get_assessment(self, assessment_id: int) -> Any:
        """Get detailed information for a specific assessment."""
        return await self._request("GET", f"assessment/{assessment_id}")

    async def get_country_species(self, country_code: str) -> Any:
        """Get the list of species that occur in a country (ISO alpha-2 code)."""
        return await self._request(
            "GET", f"country/getspecies/{country_code}"
        )

    async def get_threats(self) -> Any:
        """Get the full list of IUCN threat types."""
        return await self._request("GET", "threats")

    async def get_habitats(self) -> Any:
        """Get the full list of IUCN habitat types."""
        return await self._request("GET", "habitats")

    async def get_conservation_measures(self) -> Any:
        """Get the full list of IUCN conservation measures."""
        return await self._request("GET", "conservation_measures")

    async def get_measures_for_species_id(self, species_id: int) -> Any:
        """Get conservation measures for a species by its internal IUCN ID."""
        return await self._request(
            "GET", f"measures/species/id/{species_id}"
        )

    async def get_measures_for_species_name(self, species_name: str) -> Any:
        """Get conservation measures for a species by its scientific name."""
        return await self._request(
            "GET", f"measures/species/name/{species_name}"
        )


def get_iucn_client() -> IucnClient:
    return IucnClient()
