"""NGSI-LD subscription notification handler."""
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from app.workers.queue import background_queue

router = APIRouter(tags=["ngsi-ld"])


def _is_valid_ngsi_ld_subscription(payload: dict) -> bool:
    """Basic validation of NGSI-LD subscription notification payload."""
    if not isinstance(payload, dict):
        return False
    data = payload.get("data")
    if not isinstance(data, list):
        return False
    return True


@router.post("/notify")
async def ngsi_ld_notify(request: Request):
    """Receive NGSI-LD subscription notifications from Orion-LD.

    Validates the payload, responds 200 immediately, and enqueues
    Neo4j sync for background processing.
    """
    payload = await request.json()

    if not _is_valid_ngsi_ld_subscription(payload):
        return JSONResponse(status_code=400, content={"error": "invalid payload"})

    entities = payload.get("data", [])
    queued = 0
    for entity in entities:
        if entity.get("type") == "AgriCrop":
            await background_queue.enqueue("sync_agri_crop", entity)
            queued += 1

    return {"status": "accepted", "queued": queued}
