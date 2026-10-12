"""Warm the trial evidence of the crops a parcel's fit list shows first (owner decision 2026-10-12).

Choosing a crop loads its varieties and estimates, and the first recommender call for a crop at a
parcel is cold (seconds on a cold graph). While the farmer reads the fit list, the first crops that
fit and have trials are computed in the background, so picking one of them answers from the
recommender's cache. One crop at a time and one prefetch at a time in the process: the farmer's own
request always finds a free slot of the recommender's cold guard. A prefetch never fails a request.
"""
from __future__ import annotations

import asyncio
import logging
import weakref

from app.planning.evidence import evidence_conditions

logger = logging.getLogger(__name__)

PREFETCH_CROPS = 3
# The detail page opens with conventional management (the user may switch to organic, a cache miss).
PREFETCH_MANAGEMENT = "conventional"

_TASKS: set[asyncio.Task] = set()
# One slot per event loop: asyncio primitives are bound to the loop they run on.
_SLOTS: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def _slot() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    slot = _SLOTS.get(loop)
    if slot is None:
        slot = _SLOTS[loop] = asyncio.Semaphore(1)
    return slot


def prefetch_crops(species: list[dict]) -> list[str]:
    """EPPO codes to warm: the first crops of the fit list that fit (with or without caveats) and have trials."""
    return [s["eppo"] for s in species if s.get("verdict") != "no_fit" and s.get("has_variety_trials")][:PREFETCH_CROPS]


async def prefetch_evidence(dao, *, parcel_id: str, tenant_id: str, regime: str, crops: list[str]) -> None:
    try:
        async with _slot():
            env = await dao.get_parcel_environment(parcel_id, tenant_id)
            if "error" in env:
                return
            for eppo in crops:
                conditions = evidence_conditions(env, crop_eppo=eppo, regime=regime, management=PREFETCH_MANAGEMENT)
                if not conditions["climate_class"]:
                    return  # no climate: the evidence answers needs_climate without the recommender
                await dao.recommend_for_conditions(conditions)
    except Exception:
        logger.warning("planning prefetch failed tenant=%s parcel=%s crops=%s", tenant_id, parcel_id, crops,
                       exc_info=True)


def schedule_evidence_prefetch(dao, *, parcel_id: str, tenant_id: str, regime: str | None,
                               species: list[dict]) -> asyncio.Task | None:
    """Start warming in the background; nothing without a regime (varieties and estimates need one)."""
    crops = prefetch_crops(species)
    if regime is None or not crops:
        return None
    task = asyncio.create_task(prefetch_evidence(dao, parcel_id=parcel_id, tenant_id=tenant_id, regime=regime,
                                                 crops=crops))
    _TASKS.add(task)  # keep a reference until it ends
    task.add_done_callback(_TASKS.discard)
    return task
