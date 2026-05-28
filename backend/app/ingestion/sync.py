"""Sync Orion-LD AgriCrop entities to Neo4j.

Batch mode: for seeding / mass ingestion.
Subscription mode: for runtime mutations (see workers/sync_worker.py).
"""
from app.graph.dao import GraphDAO


async def sync_all_agri_crops(dao: GraphDAO, orion_client) -> int:
    """Sync all AgriCrop entities from Orion-LD to Neo4j (batch seeding).

    Returns count of synced entities.
    """
    crops = await orion_client.list_by_type("AgriCrop")
    count = 0
    for crop in crops:
        await dao.merge_agri_crop(crop)
        count += 1
    return count


async def sync_single_agri_crop(dao: GraphDAO, entity: dict) -> None:
    """Sync a single AgriCrop entity to Neo4j (runtime subscription)."""
    await dao.merge_agri_crop(entity)
