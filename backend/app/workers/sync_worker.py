"""Background worker that syncs Orion-LD entities to Neo4j."""
import logging

from app.core.dependencies import get_driver
from app.graph.dao import GraphDAO
from app.ingestion.sync import sync_single_agri_crop

logger = logging.getLogger("bioorchestrator.sync")


async def handle_sync_agri_crop(entity: dict):
    """Background handler: sync a single AgriCrop from Orion-LD to Neo4j."""
    driver = get_driver()
    dao = GraphDAO(driver)
    try:
        await sync_single_agri_crop(dao, entity)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Sync failed for {entity.get('id')}: {e}")
