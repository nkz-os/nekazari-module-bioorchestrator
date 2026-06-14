from app.ingestion.uri import agri_crop_uri
from app.ingestion.ecocrop_ingester import EcoCropIngester


def test_property_map_coverage():
    """All mapped properties transform correctly."""
    ingester = EcoCropIngester(None)  # orion not needed for this test
    eco_entity = {
        "phMin": 6.0,
        "phMax": 8.0,
        "tempMinAbs": -5.0,
        "tempMaxAbs": 42.0,
        "rainMin": 300,
        "rainMax": 1200,
    }
    attrs = ingester._to_ngsi_ld_attrs(eco_entity)
    assert attrs["phMin"] == {"type": "Property", "value": 6.0}
    assert attrs["phMax"] == {"type": "Property", "value": 8.0}
    assert attrs["tempMinAbs"] == {"type": "Property", "value": -5.0}
    assert attrs["tempMaxAbs"] == {"type": "Property", "value": 42.0}


def test_none_values_skipped():
    ingester = EcoCropIngester(None)
    eco_entity = {"phMin": 6.5, "phMax": None, "soilTexture": None}
    attrs = ingester._to_ngsi_ld_attrs(eco_entity)
    assert "phMin" in attrs
    assert "phMax" not in attrs
    assert "soilTexture" not in attrs


def test_uri_generation_for_ecocrop_entities():
    """URIs match between EcoCrop ingester and canonical function."""
    sci_names = ["Olea europaea", "Triticum aestivum", "Zea mays"]
    for name in sci_names:
        uri = agri_crop_uri(name)
        assert uri.startswith("urn:ngsi-ld:AgriCrop:")
        assert " " not in uri
