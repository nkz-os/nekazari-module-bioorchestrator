"""Parcel crop cycles as the platform resolves them, read from the broker by the SDK.

Same payload the core's former ``/crop-cycles`` endpoint served (``CropCycleTimeline.to_dict()``),
``None`` when it cannot be read: every caller reads that as "cycles unavailable", never as "no crop".
"""
import logging

from nkz_platform_sdk import fetch_crop_timeline

logger = logging.getLogger(__name__)


async def fetch_crop_cycles(parcel_urn: str, tenant_id: str) -> dict | None:
    try:
        return await fetch_crop_timeline(tenant_id, parcel_urn)
    except Exception:
        # The SDK already answers None on broker failures; anything else (e.g. an id it cannot
        # normalise) is also "unavailable", logged here so it is never silent.
        logger.exception("crop timeline failed parcel=%s tenant=%s", parcel_urn, tenant_id)
        return None
