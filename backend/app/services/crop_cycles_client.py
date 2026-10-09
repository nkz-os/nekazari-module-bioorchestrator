"""Parcel crop cycles as resolved by the platform (entity-manager)."""
import logging
import os

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


async def fetch_crop_cycles(parcel_urn: str, tenant_id: str) -> dict | None:
    url = f"{settings.entity_manager_url.rstrip('/')}/api/internal/parcels/{parcel_urn}/crop-cycles"
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(url, params={"tenant_id": tenant_id}, headers={
                "X-Internal-Service-Secret": os.getenv("INTERNAL_SERVICE_SECRET", "")})
        return resp.json() if resp.status_code == 200 else None
    except httpx.HTTPError as e:
        logger.warning("crop-cycles unavailable for %s: %s", parcel_urn, e)
        return None
