"""Ingest crop varieties from CPVO into Orion-LD as AgriCrop entities."""
from app.ingestion.uri import agri_crop_uri
from app.ingestion.orion import OrionIngestionClient
from app.ingestion.sync import sync_all_agri_crops
from app.graph.dao import GraphDAO


class VarietyIngester:
    """Transform CPVO connector output -> NGSI-LD AgriCrop varieties."""

    def __init__(self, orion: OrionIngestionClient):
        self.orion = orion

    async def ingest(self, dao: GraphDAO, limit: int | None = None) -> dict:
        """Run CPVO variety ingestion.

        1. Fetch varieties from CPVO connector
        2. Create AgriCrop entities with hierarchical URIs
        3. Add variety URIs to parent species hasSubCrop
        4. Upsert to Orion-LD
        5. Sync to Neo4j
        """
        from ikerketa.connectors.cpvo_varieties import CPVOVarietiesConnector

        connector = CPVOVarietiesConnector()
        result = connector.fetch(limit=limit)

        entities = result.entities if hasattr(result, 'entities') else []

        # Group varieties by parent species for hasSubCrop updates
        parent_updates: dict[str, list[str]] = {}  # parent_uri -> [variety_uris]
        agri_crops = []

        for var in entities:
            sci_name = var.get("scientificName", "")
            common_name = var.get("name", "")
            parent_species = var.get("species", "")

            if not sci_name or not parent_species:
                continue

            variety_uri = agri_crop_uri(sci_name, variety_of=parent_species)
            parent_uri = agri_crop_uri(parent_species)

            attrs = {}
            if var.get("registrationYear"):
                attrs["registrationYear"] = {"type": "Property",
                                              "value": var["registrationYear"]}
            if var.get("maintainer"):
                attrs["maintainer"] = {"type": "Property",
                                       "value": var["maintainer"]}

            agri_crop = self.orion.build_entity(
                variety_uri, common_name, sci_name, "CPVO",
                extra_attrs=attrs)
            agri_crops.append(agri_crop)

            if parent_uri not in parent_updates:
                parent_updates[parent_uri] = []
            parent_updates[parent_uri].append(variety_uri)

        # Upsert varieties to Orion-LD
        chunk_size = 100
        for i in range(0, len(agri_crops), chunk_size):
            chunk = agri_crops[i:i + chunk_size]
            await self.orion.upsert_batch(chunk)

        # Update parent species with hasSubCrop relationships
        for parent_uri, variety_uris in parent_updates.items():
            await self.orion.patch_entity(parent_uri, {
                "hasSubCrop": {
                    "type": "Relationship",
                    "object": variety_uris,
                }
            })

        synced = await sync_all_agri_crops(dao, self.orion)

        return {
            "source": "cpvo",
            "entities_found": len(entities),
            "varieties_upserted": len(agri_crops),
            "parents_updated": len(parent_updates),
            "synced_to_neo4j": synced,
        }
