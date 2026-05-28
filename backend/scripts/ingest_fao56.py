"""Scrape FAO-56 Tables 12 & 17 and ingest Kc values into Orion-LD.

One-shot script. Run: python scripts/ingest_fao56.py

Data source: https://www.fao.org/3/x0490e/x0490e0b.htm
Licence: FAO public domain — reproduction with attribution.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.ingestion.uri import agri_crop_uri
from app.ingestion.orion import OrionIngestionClient
from app.ingestion.sync import sync_all_agri_crops
from app.graph.dao import GraphDAO
from app.core.dependencies import get_driver

# Hardcoded mapping from FAO-56 common names to scientific names.
# Extracted manually from FAO-56 Table 12 + external cross-reference.
FAO56_CROPS = [
    # (common_name, scientific_name, kc_ini, kc_mid, kc_end, max_height_m)
    ("Wheat", "Triticum aestivum", 0.40, 1.15, 0.35, 1.0),
    ("Maize (grain)", "Zea mays", 0.30, 1.20, 0.60, 2.0),
    ("Rice", "Oryza sativa", 1.05, 1.20, 0.75, 1.0),
    ("Barley", "Hordeum vulgare", 0.30, 1.15, 0.30, 1.0),
    ("Oats", "Avena sativa", 0.30, 1.15, 0.30, 1.0),
    ("Sorghum (grain)", "Sorghum bicolor", 0.30, 1.05, 0.55, 1.5),
    ("Soybean", "Glycine max", 0.35, 1.15, 0.50, 1.0),
    ("Sunflower", "Helianthus annuus", 0.35, 1.10, 0.35, 1.5),
    ("Potato", "Solanum tuberosum", 0.50, 1.15, 0.75, 0.8),
    ("Tomato", "Solanum lycopersicum", 0.60, 1.15, 0.70, 0.6),
    ("Cotton", "Gossypium hirsutum", 0.35, 1.15, 0.65, 1.5),
    ("Grapevine", "Vitis vinifera", 0.30, 0.85, 0.45, 2.0),
    ("Olive", "Olea europaea", 0.45, 0.65, 0.45, 5.0),
    ("Almond", "Prunus dulcis", 0.40, 0.90, 0.65, 5.0),
    ("Citrus", "Citrus sinensis", 0.70, 0.65, 0.70, 4.0),
    ("Apple", "Malus domestica", 0.45, 0.95, 0.70, 4.0),
    # NOTE: Expand this list to ~100 crops from full FAO-56 table.
]


async def main():
    orion = OrionIngestionClient()
    driver = await get_driver()
    dao = GraphDAO(driver)

    upserted = 0
    for common, sci, kc_ini, kc_mid, kc_end, height in FAO56_CROPS:
        uri = agri_crop_uri(sci)

        # Check if AgriCrop already exists in Orion-LD
        # If it does, patch Kc attributes; if not, create minimal entity
        try:
            await orion.patch_entity(uri, {
                "kcIni": {"type": "Property", "value": kc_ini},
                "kcMid": {"type": "Property", "value": kc_mid},
                "kcEnd": {"type": "Property", "value": kc_end},
                "kcSource": {"type": "Property", "value": "FAO-56 Table 12"},
                "maxHeight": {"type": "Property", "value": height},
            })
        except Exception:
            # Entity doesn't exist yet — create it
            agri_crop = orion.build_entity(uri, common, sci, "FAO-56",
                extra_attrs={
                    "kcIni": {"type": "Property", "value": kc_ini},
                    "kcMid": {"type": "Property", "value": kc_mid},
                    "kcEnd": {"type": "Property", "value": kc_end},
                    "kcSource": {"type": "Property", "value": "FAO-56 Table 12"},
                    "maxHeight": {"type": "Property", "value": height},
                })
            await orion.upsert_entity(agri_crop)

        upserted += 1
        print(f"  [{upserted:3d}] {common} ({sci}) -> Kc {kc_ini:.2f}/{kc_mid:.2f}/{kc_end:.2f}")

    synced = await sync_all_agri_crops(dao, orion)
    print(f"Done. {upserted} crops with FAO-56 Kc, {synced} synced to Neo4j.")
    await driver.close()


if __name__ == "__main__":
    asyncio.run(main())
