"""Canonical URI generator for AgriCrop entities.

All ingestion flows MUST use this function to generate entity IDs.
Orion-LD upsert matches on id — shared URI format guarantees no duplicates.
"""


def agri_crop_uri(scientific_name: str, variety_of: str | None = None) -> str:
    """Generate canonical NGSI-LD URI for an AgriCrop entity.

    Species:  urn:ngsi-ld:AgriCrop:Olea_europaea
    Variety:  urn:ngsi-ld:AgriCrop:Olea_europaea:Picual
    """
    formatted = scientific_name.strip().replace(" ", "_").replace(".", "")
    if variety_of:
        parent = agri_crop_uri(variety_of)
        return f"{parent}:{formatted}"
    return f"urn:ngsi-ld:AgriCrop:{formatted}"
