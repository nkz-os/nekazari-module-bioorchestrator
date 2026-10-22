from app.ingestion.uri import agri_crop_uri


def test_variety_uri_hierarchy():
    uri = agri_crop_uri("Picual", variety_of="Olea europaea")
    assert uri == "urn:ngsi-ld:AgriCrop:Olea_europaea:Picual"
    # Parent is prefix
    parent = agri_crop_uri("Olea europaea")
    assert uri.startswith(parent)


def test_multiple_varieties_same_species():
    parent = agri_crop_uri("Olea europaea")
    vars_ = [
        agri_crop_uri(v, variety_of="Olea europaea")
        for v in ("Picual", "Arbequina", "Hojiblanca")
    ]
    for var_uri in vars_:
        assert var_uri.startswith(parent)
