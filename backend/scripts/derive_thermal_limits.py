"""Derive heat/frost tolerance thresholds from EcoCrop temperature data.

Reads all AgriCrop entities from Orion-LD that have tempMinAbs/tempMaxAbs
(from EcoCrop ingestion), derives conservative damage thresholds, and
patches them back to Orion-LD.

Run: python scripts/derive_thermal_limits.py
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.ingestion.orion import OrionIngestionClient
from app.ingestion.sync import sync_all_agri_crops
from app.graph.dao import GraphDAO
from app.core.dependencies import get_driver

HEAT_MARGIN_C = 2.0   # Conservative offset from absolute max to damage threshold
FROST_THRESHOLD = 0.0  # Only derive frost threshold if tempMinAbs < 0 deg C


async def main():
    orion = OrionIngestionClient()
    driver = await get_driver()
    dao = GraphDAO(driver)

    crops = await orion.list_by_type("AgriCrop")
    derived = 0

    for crop in crops:
        attrs_to_patch = {}

        temp_min = _extract_property(crop, "tempMinAbs")
        temp_max = _extract_property(crop, "tempMaxAbs")

        if temp_max is not None:
            heat_threshold = temp_max - HEAT_MARGIN_C
            attrs_to_patch["heatDamageThresholdC"] = {
                "type": "Property", "value": heat_threshold,
            }

        if temp_min is not None and temp_min < FROST_THRESHOLD:
            attrs_to_patch["frostDamageThresholdC"] = {
                "type": "Property", "value": temp_min,
            }

        if attrs_to_patch:
            attrs_to_patch["thermalSource"] = {
                "type": "Property",
                "value": "EcoCrop GAEZ v4 (derived, conserv. margin +/-2 deg C)",
            }
            try:
                await orion.patch_entity(crop["id"], attrs_to_patch)
                derived += 1
            except Exception as e:
                print(f"  ERROR patching {crop.get('id')}: {e}")

    synced = await sync_all_agri_crops(dao, orion)
    print(f"Done. {derived} crops with derived thermal limits, {synced} synced to Neo4j.")
    await driver.close()


def _extract_property(entity: dict, attr_name: str) -> float | None:
    """Safely extract a numeric NGSI-LD Property value."""
    attr = entity.get(attr_name, {})
    if isinstance(attr, dict):
        value = attr.get("value")
        if isinstance(value, (int, float)):
            return float(value)
    return None


if __name__ == "__main__":
    asyncio.run(main())
