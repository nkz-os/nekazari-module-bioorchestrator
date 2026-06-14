from app.ingestion.builders import build_agri_crop_entity


def test_build_minimal_agri_crop():
    e = build_agri_crop_entity(
        "urn:ngsi-ld:AgriCrop:wheat", "Wheat", "Triticum aestivum", "EcoCrop GAEZ v4")
    assert e["id"] == "urn:ngsi-ld:AgriCrop:wheat"
    assert e["type"] == "AgriCrop"
    assert e["@context"]
    assert e["name"] == {"type": "Property", "value": "Wheat"}
    assert e["scientificName"]["value"] == "Triticum aestivum"
    assert e["dataProvider"]["value"] == "EcoCrop GAEZ v4"


def test_build_merges_extra_attrs():
    e = build_agri_crop_entity(
        "urn:ngsi-ld:AgriCrop:wheat", "Wheat", "Triticum aestivum", "x",
        extra_attrs={"phMin": {"type": "Property", "value": 5.5}})
    assert e["phMin"] == {"type": "Property", "value": 5.5}
