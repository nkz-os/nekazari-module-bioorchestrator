"""Receiver for Orion-LD notifications on CropHealthAssessment.phenologyStage.

Auth-exempt (/api/graph/internal/ in SKIP_AUTH_PREFIXES). Responds 200 fast and
enqueues rule evaluation. bioorch reads the state crop-health wrote; it never
recomputes it. In-process dedup collapses repeated same-stage notifications.
"""
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.workers.queue import background_queue

router = APIRouter(tags=["internal"])
logger = logging.getLogger(__name__)

_LAST_STAGE: dict[tuple[str, str], str] = {}


def _kv(prop):
    """Value of a normalized Property, or a Relationship object, or a scalar."""
    if isinstance(prop, dict):
        return prop.get("value", prop.get("object"))
    return prop


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
        _LAST_STAGE[key] = stage
        observed = {"phenology.current_stage": stage}
        for extra in ("waterDeficitMm", "nRequirementKgHa"):
            if extra in entity:
                observed[_SNAKE[extra]] = _kv(entity[extra])
        await background_queue.enqueue("evaluate_action_rules", tenant_id, parcel_id, observed)
        queued += 1
    return {"status": "accepted", "queued": queued}


_SNAKE = {"waterDeficitMm": "water_deficit_mm", "nRequirementKgHa": "n_requirement_kg_ha"}
