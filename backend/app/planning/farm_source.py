"""I/O of the farm matrix: tenant parcels, farms and crops from Orion, cycles from the core."""
from __future__ import annotations

import asyncio
import logging

from nkz_platform_sdk.orion import OrionClient

from app.planning.service import PARCEL_URN_RE, InvalidParcelId
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
    # The id goes into an Orion path and a ``q`` filter: never trust the caller to have checked it.
    if parcel_id and not (isinstance(parcel_id, str) and PARCEL_URN_RE.fullmatch(parcel_id)):
        raise InvalidParcelId("invalid parcel id")
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
        # One parcel with a broken answer must not take the whole farm down: it becomes "cycles unavailable".
        async with gate:
            try:
                answer = await fetch_crop_cycles(urn, tenant_id)
            except Exception:
                logger.warning("planning farm: crop-cycles failed tenant=%s parcel=%s", tenant_id, urn, exc_info=True)
                return None
        if answer is not None and not isinstance(answer, dict):
            logger.warning("planning farm: crop-cycles answer is %s, not an object tenant=%s parcel=%s",
                           type(answer).__name__, tenant_id, urn)
            return None
        return answer

    results = await asyncio.gather(*(cycles(p["id"]) for p in parcels))
    return parcels, farms, {c["id"]: c for c in crops}, dict(zip([p["id"] for p in parcels], results))
