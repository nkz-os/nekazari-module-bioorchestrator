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
    except httpx.HTTPError as e:
        logger.warning("crop-cycles unavailable for %s tenant=%s: %s; falling back to local resolution",
                       parcel_urn, tenant_id, e)
        return None
    # Every fallback is logged: a silent one hides a broken platform contract.
    if resp.status_code != 200:
        logger.warning("crop-cycles endpoint status=%s for %s tenant=%s; falling back to local resolution",
                       resp.status_code, parcel_urn, tenant_id)
        return None
    try:
        return resp.json()
    except ValueError as e:
        logger.warning("crop-cycles response not JSON for %s tenant=%s: %s; falling back to local resolution",
                       parcel_urn, tenant_id, e)
        return None
