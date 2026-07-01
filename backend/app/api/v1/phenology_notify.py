"""Receiver for Orion-LD notifications on CropHealthAssessment.phenologyStage.

Auth-exempt (/api/graph/internal/ in SKIP_AUTH_PREFIXES). Responds 200 fast and
dispatches rule evaluation on the serving event loop. bioorch reads the state
crop-health wrote; it never recomputes it. In-process dedup collapses repeated
same-stage notifications.
"""
import asyncio
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.workers.rule_worker import handle_evaluate_action_rules

router = APIRouter(tags=["internal"])
logger = logging.getLogger(__name__)

_LAST_STAGE: dict[tuple[str, str], str] = {}
# Keep strong refs to in-flight tasks so the loop doesn't GC them mid-run.
_BG_TASKS: set = set()


def _kv(prop):
    """Value of a normalized Property, or a Relationship object, or a scalar."""
    if isinstance(prop, dict):
        return prop.get("value", prop.get("object"))
    return prop


def _dispatch(tenant_id: str, parcel_id: str, observed: dict) -> None:
    """Fire-and-forget rule evaluation on the current serving loop.

    Uses create_task (not the shared background_queue) so the Neo4j/Orion
    clients run on the same loop that owns them, and failures surface in the
    handler's own logging instead of being swallowed by the queue loop.
    """
    task = asyncio.create_task(handle_evaluate_action_rules(tenant_id, parcel_id, observed))
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)


@router.post("/phenology-update")
async def phenology_update(request: Request):
    payload = await request.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        return JSONResponse(status_code=400, content={"error": "invalid payload"})

    tenant_id = request.headers.get("NGSILD-Tenant", "")
    queued = 0
    for entity in payload["data"]:
        if entity.get("type") != "CropHealthAssessment":
            continue
        parcel_id = _kv(entity.get("hasAgriParcel"))
        stage = _kv(entity.get("phenologyStage"))
        if not parcel_id or stage is None:
            continue
        key = (tenant_id, parcel_id)
        if _LAST_STAGE.get(key) == stage:
            continue  # dedup: stage unchanged since last notification
        observed = {"phenology.current_stage": stage}
        for extra in ("waterDeficitMm", "nRequirementKgHa"):
            if extra in entity:
                observed[_SNAKE[extra]] = _kv(entity[extra])
        _dispatch(tenant_id, parcel_id, observed)
        _LAST_STAGE[key] = stage
        queued += 1
    return {"status": "accepted", "queued": queued}


_SNAKE = {"waterDeficitMm": "water_deficit_mm", "nRequirementKgHa": "n_requirement_kg_ha"}
