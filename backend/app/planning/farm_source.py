"""I/O of the farm matrix: tenant parcels, farms and crops from Orion, cycles from the core."""
from __future__ import annotations

import asyncio
import logging

from nkz_platform_sdk.orion import OrionClient

from app.services.crop_cycles_client import fetch_crop_cycles

logger = logging.getLogger(__name__)
PAGE = 1000
MAX_PAGES = 20
CYCLES_CONCURRENCY = 8


async def _all(client, entity_type: str, q: str | None = None) -> list[dict]:
    out: list[dict] = []
    for page in range(MAX_PAGES):
        rows = await client.query_entities(type=entity_type, q=q, limit=PAGE, offset=page * PAGE, options="keyValues")
        out.extend(rows)
        if len(rows) < PAGE:
            return out
    logger.warning("planning farm: %s listing truncated at %d entities", entity_type, len(out))
    return out


async def load_farm(tenant_id: str, parcel_id: str | None = None):
    client = OrionClient(tenant_id=tenant_id)
    try:
        if parcel_id:
            parcels = [await client.get_entity(parcel_id, options="keyValues")]
            crops = await _all(client, "AgriCrop", q=f'hasAgriParcel=="{parcel_id}"')
        else:
            parcels = await _all(client, "AgriParcel")
            crops = await _all(client, "AgriCrop")
        farms = await _all(client, "AgriFarm")
    finally:
        await client.close()
    gate = asyncio.Semaphore(CYCLES_CONCURRENCY)

    async def cycles(urn: str):
        async with gate:
            return await fetch_crop_cycles(urn, tenant_id)

    results = await asyncio.gather(*(cycles(p["id"]) for p in parcels))
    return parcels, farms, {c["id"]: c for c in crops}, dict(zip([p["id"] for p in parcels], results))
