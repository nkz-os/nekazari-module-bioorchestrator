"""Pure NGSI-LD entity builders for the AgriCrop catalog (no Orion I/O)."""
from app.core.config import settings


def build_agri_crop_entity(
    uri: str,
    name: str,
    scientific_name: str,
    provider: str,
    extra_attrs: dict | None = None,
) -> dict:
    """Build a minimal AgriCrop NGSI-LD entity (ld+json, @context inline).

    Provenance is mandatory (life-critical data fabric): a sourceless agronomic
    entity must never be constructed, so an empty/whitespace ``provider`` raises.
    """
    if not provider or not str(provider).strip():
        raise ValueError(
            "build_agri_crop_entity: provenance (provider) is mandatory — "
            f"refusing to build sourceless AgriCrop {uri!r}"
        )
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
