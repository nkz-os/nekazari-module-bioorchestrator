"""NGSI-LD subscription notification handler."""
import hmac
import os

from fastapi import APIRouter, Header, HTTPException, Request

from app.workers.queue import background_queue

router = APIRouter(tags=["ngsi-ld"])


def _reject_unauthenticated_notify(x_internal_secret: str | None) -> HTTPException | None:
    """Flag-gated auth for the Orion notification receiver (two-phase rollout)."""
    require = os.getenv("NOTIFY_REQUIRE_INTERNAL_SECRET", "").lower() in (
        "1", "true", "yes", "on"
    )
    if not require:
        return None
    secret = os.getenv("INTERNAL_SERVICE_SECRET", "")
    if not secret or not hmac.compare_digest(x_internal_secret or "", secret):
        return HTTPException(status_code=401, detail="missing or invalid internal secret")
    return None


def _is_valid_ngsi_ld_subscription(payload: dict) -> bool:
    """Basic validation of NGSI-LD subscription notification payload."""
    if not isinstance(payload, dict):
        return False
    data = payload.get("data")
    return isinstance(data, list)


@router.post("/notify", status_code=204)
async def ngsi_ld_notify(
    request: Request,
    x_internal_secret: str | None = Header(None, alias="X-Internal-Service-Secret"),
):
    """Receive NGSI-LD subscription notifications from Orion-LD.

    Validates the payload, responds 204 (No Content) immediately, and enqueues
    Neo4j sync for background processing.

    Returns 204 with no body on success (contract requirement for Orion-LD).
    Malformed payloads return 400 — if Orion cannot produce it, fail loudly.
    """
    reject = _reject_unauthenticated_notify(x_internal_secret)
    if reject:
        raise reject
    payload = await request.json()

    if not _is_valid_ngsi_ld_subscription(payload):
        raise HTTPException(status_code=400, detail="invalid payload")

    entities = payload.get("data", [])
    for entity in entities:
        if entity.get("type") == "AgriCrop":
            await background_queue.enqueue("sync_agri_crop", entity)
