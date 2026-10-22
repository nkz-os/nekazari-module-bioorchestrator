from app.ingestion.uri import agri_crop_uri


def test_species_uri():
    uri = agri_crop_uri("Olea europaea")
    assert uri == "urn:ngsi-ld:AgriCrop:Olea_europaea"


def test_species_uri_removes_dots():
    uri = agri_crop_uri("Olea europaea var. sylvestris")
    assert uri == "urn:ngsi-ld:AgriCrop:Olea_europaea_var_sylvestris"


def test_variety_uri():
    uri = agri_crop_uri("Picual", variety_of="Olea europaea")
    assert uri == "urn:ngsi-ld:AgriCrop:Olea_europaea:Picual"


def test_variety_uri_with_subspecies_parent():
    uri = agri_crop_uri("Raf", variety_of="Solanum lycopersicum")
    assert uri == "urn:ngsi-ld:AgriCrop:Solanum_lycopersicum:Raf"


def test_uri_idempotent():
    uri1 = agri_crop_uri("Triticum aestivum")
    uri2 = agri_crop_uri("Triticum aestivum")
    assert uri1 == uri2
