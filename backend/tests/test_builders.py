import pytest

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


@pytest.mark.parametrize("bad_provider", [None, "", "   "])
def test_provenance_is_mandatory(bad_provider):
    """Life-critical invariant: no AgriCrop catalog entity without a source.

    A provenance-less agronomic entity must never be constructed — the builder
    refuses an empty/whitespace provider so a future ingester change cannot
    silently ship sourceless crop data into Orion/Neo4j.
    """
    with pytest.raises(ValueError):
        build_agri_crop_entity(
            "urn:ngsi-ld:AgriCrop:wheat", "Wheat", "Triticum aestivum", bad_provider)
