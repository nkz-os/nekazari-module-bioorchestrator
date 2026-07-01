"""Background handler: evaluate action rules for a phenologyStage change."""
import logging

from nkz_platform_sdk.orion import OrionClient

from app.core.dependencies import get_driver
from app.graph.dao import GraphDAO
from app.graph.rule_engine import evaluate

logger = logging.getLogger(__name__)


async def handle_evaluate_action_rules(tenant_id: str, parcel_id: str, observed: dict) -> None:
    orion = OrionClient(tenant_id)
    try:
        dao = GraphDAO(get_driver())
        advisories = await evaluate(dao, orion, tenant_id, parcel_id, observed)
        logger.info("evaluate_action_rules done: tenant=%s parcel=%s advisories=%d",
                    tenant_id, parcel_id, len(advisories))
    except Exception as exc:
        logger.warning("evaluate_action_rules failed: tenant=%s parcel=%s err=%s",
                       tenant_id, parcel_id, exc)
    finally:
        await orion.close()
