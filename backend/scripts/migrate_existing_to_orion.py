"""One-shot migration: seed the 4 manually-curated species into Orion-LD.

Reads from phenology_sources.yaml + Neo4j PhenologyParams,
creates AgriCrop entities in Orion-LD with Kc values and provenance.
Uses agri_crop_uri() to match the format EcoCrop will use (prevents duplicates).

Run ONCE before running EcoCrop/FAO-56 ingestion.
"""
import asyncio
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.ingestion.uri import agri_crop_uri
from app.ingestion.orion import OrionIngestionClient
from app.core.dependencies import get_driver
from app.graph.dao import GraphDAO


async def main():
    data_dir = Path(__file__).parent.parent.parent / "data"
    yaml_path = data_dir / "phenology_sources.yaml"

    if not yaml_path.exists():
        print(f"ERROR: {yaml_path} not found. Run from bioorchestrator root.")
        return

    with open(yaml_path) as f:
        phen_data = yaml.safe_load(f)

    orion = OrionIngestionClient()
    driver = await get_driver()
    dao = GraphDAO(driver)
    migrated = 0

    for species_entry in phen_data.get("species", []):
        sci_name = species_entry.get("scientificName")
        common_name = species_entry.get("name", sci_name)
        if not sci_name:
            continue

        uri = agri_crop_uri(sci_name)
        attrs = {}

        # Collect Kc values from curated stages
        for stage in species_entry.get("stages", []):
            for param in stage.get("parameters", []):
                kc = param.get("kc")
                if kc is not None and "kcIni" not in attrs:
                    attrs["kcIni"] = {"type": "Property", "value": float(kc)}
                    doi = param.get("sourceDoi", "")
                    attrs["kcSource"] = {"type": "Property",
                                         "value": f"Scientific literature ({doi})" if doi else "Scientific literature"}

        # Heat tolerance
        for ht in species_entry.get("heatTolerance", []):
            if "heatDamageThresholdC" in ht:
                attrs["heatDamageThresholdC"] = {"type": "Property",
                                                  "value": float(ht["heatDamageThresholdC"])}
            if "frostDamageThresholdC" in ht:
                attrs["frostDamageThresholdC"] = {"type": "Property",
                                                   "value": float(ht["frostDamageThresholdC"])}

        agri_crop = orion.build_entity(
            uri, common_name, sci_name, "Manual curation (phenology_sources.yaml)",
            extra_attrs=attrs)
        await orion.upsert_entity(agri_crop)
        migrated += 1
        print(f"  [{migrated}] {common_name} ({sci_name}) -> {uri}")

    print(f"Done. {migrated} species migrated to Orion-LD.")
    print("NOTE: Detailed provenance (per-stage, per-cultivar) remains in Neo4j.")
    await driver.close()


if __name__ == "__main__":
    asyncio.run(main())
