"""Soil nodata sanitization for crop-context soil.actual."""

from app.services.soil_nodata import is_soilgrids_nodata, sanitize_soil_properties


def test_is_soilgrids_nodata_sentinels():
    assert is_soilgrids_nodata(-3276.8)
    assert is_soilgrids_nodata(-32.77) is False
    assert not is_soilgrids_nodata(6.5)


def test_sanitize_soil_properties_drops_sentinels_and_oob_bulk_density():
    raw = {
        "ph": 6.2,
        "bulk_density_g_cm3": -3276.8,
        "organic_matter_pct": 2.1,
        "data_available": True,
    }
    cleaned = sanitize_soil_properties(raw)
    assert cleaned["ph"] == 6.2
    assert cleaned["bulk_density_g_cm3"] is None
    assert cleaned["organic_matter_pct"] == 2.1


def test_sanitize_soil_properties_drops_invalid_ph():
    cleaned = sanitize_soil_properties({"ph": -32.77, "data_available": True})
    assert cleaned["ph"] is None
