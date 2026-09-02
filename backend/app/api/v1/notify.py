"""NGSI-LD subscription notification handler."""
from fastapi import APIRouter, HTTPException, Request

from app.workers.queue import background_queue

router = APIRouter(tags=["ngsi-ld"])


def _is_valid_ngsi_ld_subscription(payload: dict) -> bool:
    """Basic validation of NGSI-LD subscription notification payload."""
    if not isinstance(payload, dict):
        return False
    data = payload.get("data")
    return isinstance(data, list)


@router.post("/notify", status_code=204)
async def ngsi_ld_notify(request: Request):
    """Receive NGSI-LD subscription notifications from Orion-LD.

    Validates the payload, responds 204 (No Content) immediately, and enqueues
    Neo4j sync for background processing.

    Returns 204 with no body on success (contract requirement for Orion-LD).
    Malformed payloads return 400 — if Orion cannot produce it, fail loudly.
    """
    payload = await request.json()

    if not _is_valid_ngsi_ld_subscription(payload):
        raise HTTPException(status_code=400, detail="invalid payload")

    entities = payload.get("data", [])
    for entity in entities:
        if entity.get("type") == "AgriCrop":
            await background_queue.enqueue("sync_agri_crop", entity)
