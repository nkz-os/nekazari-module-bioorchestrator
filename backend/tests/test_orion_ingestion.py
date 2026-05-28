import pytest
from app.ingestion.uri import agri_crop_uri


def test_build_entity_structure():
    """Skip httpx — test entity construction in isolation."""
    from app.ingestion.orion import OrionIngestionClient

    client = OrionIngestionClient()
    uri = agri_crop_uri("Zea mays")
    entity = client.build_entity(uri, "Maize", "Zea mays", "EcoCrop",
        extra_attrs={
            "phMin": {"type": "Property", "value": 5.5},
            "phMax": {"type": "Property", "value": 7.5},
        })

    assert entity["id"] == "urn:ngsi-ld:AgriCrop:Zea_mays"
    assert entity["type"] == "AgriCrop"
    assert entity["@context"]  # populated
    assert entity["phMin"]["value"] == 5.5
    assert entity["name"]["value"] == "Maize"


def test_entity_with_kc_attributes():
    from app.ingestion.orion import OrionIngestionClient

    client = OrionIngestionClient()
    uri = agri_crop_uri("Triticum aestivum")
    entity = client.build_entity(uri, "Wheat", "Triticum aestivum", "FAO-56",
        extra_attrs={
            "kcIni": {"type": "Property", "value": 0.40},
            "kcMid": {"type": "Property", "value": 1.15},
            "kcEnd": {"type": "Property", "value": 0.35},
            "kcSource": {"type": "Property", "value": "FAO-56 Table 12"},
        })

    assert entity["kcIni"]["value"] == 0.40
    assert entity["kcMid"]["value"] == 1.15
