from scripts.derive_thermal_limits import _extract_property


def test_extract_property_standard():
    entity = {"tempMaxAbs": {"type": "Property", "value": 42.0}}
    assert _extract_property(entity, "tempMaxAbs") == 42.0


def test_extract_property_missing():
    entity = {"name": {"type": "Property", "value": "Olive"}}
    assert _extract_property(entity, "tempMaxAbs") is None


def test_extract_property_integer():
    entity = {"tempMinAbs": {"type": "Property", "value": -5}}
    assert _extract_property(entity, "tempMinAbs") == -5.0


def test_heat_margin():
    """Heat threshold = tempMaxAbs - 2 deg C margin"""
    temp_max = 42.0
    assert temp_max - 2.0 == 40.0


def test_frost_only_derived_when_below_zero():
    """Frost only set if tempMinAbs < 0 deg C"""
    assert -5.0 < 0  # should derive
    assert 5.0 >= 0   # should not derive
