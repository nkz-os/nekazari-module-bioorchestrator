from app.ingestion.uri import agri_crop_uri


def test_all_fao56_crops_have_valid_uris():
    """Every crop in FAO56_CROPS produces a valid URI."""
    from scripts.ingest_fao56 import FAO56_CROPS

    for common, sci, kc_ini, kc_mid, kc_end, height in FAO56_CROPS:
        uri = agri_crop_uri(sci)
        assert uri.startswith("urn:ngsi-ld:AgriCrop:")
        assert 0 < kc_ini <= 1.5, f"{common}: kc_ini out of range"
        assert 0 < kc_mid <= 1.5, f"{common}: kc_mid out of range"
        assert 0 < kc_end <= 1.5, f"{common}: kc_end out of range"


def test_fao56_kc_values_in_reasonable_range():
    from scripts.ingest_fao56 import FAO56_CROPS

    for common, sci, kc_ini, kc_mid, kc_end, height in FAO56_CROPS:
        # FAO-56 Kc values are always between 0 and ~1.5
        assert kc_ini > 0, f"{common} kc_ini must be > 0"
        assert kc_mid > 0, f"{common} kc_mid must be > 0"
        assert kc_mid >= kc_ini * 0.5, f"{common}: kc_mid suspiciously low"
        assert height > 0, f"{common}: height must be > 0"
