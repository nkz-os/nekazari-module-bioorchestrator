"""Pure NGSI-LD entity builders for the AgriCrop catalog (no Orion I/O)."""
from app.core.config import settings


def build_agri_crop_entity(
    uri: str,
    name: str,
    scientific_name: str,
    provider: str,
    extra_attrs: dict | None = None,
) -> dict:
    """Build a minimal AgriCrop NGSI-LD entity (ld+json, @context inline)."""
    entity = {
        "id": uri,
        "type": "AgriCrop",
        "@context": [settings.context_url],
        "name": {"type": "Property", "value": name},
        "scientificName": {"type": "Property", "value": scientific_name},
        "dataProvider": {"type": "Property", "value": provider},
    }
    if extra_attrs:
        entity.update(extra_attrs)
    return entity
