"""Ingest species from EcoCrop GAEZ v4 into Orion-LD as AgriCrop entities."""
from app.ingestion.uri import agri_crop_uri
from app.ingestion.builders import build_agri_crop_entity
from app.ingestion.sync import sync_all_agri_crops
from app.graph.dao import GraphDAO


class EcoCropIngester:
    """Transform EcoCrop connector output -> NGSI-LD AgriCrop entities."""

    # Map EcoCrop property names to NGSI-LD attribute names
    PROPERTY_MAP = {
        "tempMinAbs": "tempMinAbs",
        "tempMaxAbs": "tempMaxAbs",
        "tempMinOpt": "tempMinOpt",
        "tempMaxOpt": "tempMaxOpt",
        "rainMin": "rainMin",
        "rainMax": "rainMax",
        "phMin": "phMin",
        "phMax": "phMax",
        "cycleMinDays": "cycleMinDays",
        "cycleMaxDays": "cycleMaxDays",
        "altitudeMin": "altitudeMin",
        "altitudeMax": "altitudeMax",
        "soilTexture": "soilTexture",
        "soilFertility": "soilFertility",
        "soilDrainage": "soilDrainage",
        "agroVocUri": "agroVocConcept",
    }

    def __init__(self, orion):
        self.orion = orion

    def _to_ngsi_ld_attrs(self, eco_entity: dict) -> dict:
        """Convert EcoCrop properties to NGSI-LD Property format."""
        attrs = {}
        for eco_key, ngsi_key in self.PROPERTY_MAP.items():
            value = eco_entity.get(eco_key)
            if value is not None:
                attrs[ngsi_key] = {"type": "Property", "value": value}
        return attrs

    async def ingest(self, dao: GraphDAO,
                     limit: int | None = None,
                     species_filter: list[str] | None = None) -> dict:
        """Run EcoCrop ingestion pipeline.

        1. Read EcoCrop entities from IkerKeta connector
        2. Transform to AgriCrop NGSI-LD
        3. Upsert batch to Orion-LD
        4. Sync to Neo4j
        """
        from ikerketa.connectors.ecocrop import EcoCropConnector

        connector = EcoCropConnector()
        result = connector.fetch()

        entities = result.entities if hasattr(result, 'entities') else []
        if species_filter:
            entities = [e for e in entities
                        if e.get("scientificName") in species_filter]
        if limit:
            entities = entities[:limit]

        agri_crops = []
        for eco in entities:
            sci_name = eco.get("scientificName", "")
            if not sci_name:
                continue
            uri = agri_crop_uri(sci_name)
            name = eco.get("name") or sci_name
            attrs = self._to_ngsi_ld_attrs(eco)
            agri_crop = build_agri_crop_entity(
                uri, name, sci_name, "EcoCrop GAEZ v4", extra_attrs=attrs)
            agri_crops.append(agri_crop)

        # Batch upsert to Orion-LD (chunked if needed)
        chunk_size = 100
        for i in range(0, len(agri_crops), chunk_size):
            chunk = agri_crops[i:i + chunk_size]
            await self.orion.upsert_entities_batch(chunk)

        # Sync to Neo4j
        synced = await sync_all_agri_crops(dao, self.orion)

        return {
            "source": "ecocrop",
            "entities_found": len(entities),
            "entities_upserted": len(agri_crops),
            "synced_to_neo4j": synced,
        }
