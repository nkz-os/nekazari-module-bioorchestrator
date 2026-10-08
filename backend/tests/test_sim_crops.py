"""Species slug -> AquaCrop built-in crop mapping."""
from __future__ import annotations

import pytest

from app.services.engines.aquacrop_engine import supported_crops
from app.services.sim_crops import AQUACROP_CROPS, resolve_aquacrop_crop
from app.services.sim_errors import SimulationError


def test_table_matches_spec_and_names_exist_in_aquacrop():
    assert AQUACROP_CROPS == {
        "wheat": "WheatGDD", "durum_wheat": "WheatGDD", "barley": "BarleyGDD",
        "maize": "MaizeGDD", "rice": "PaddyRiceGDD", "soybean": "SoybeanGDD",
        "sunflower": "SunflowerGDD", "sugar_beet": "SugarBeetGDD", "potato": "PotatoGDD",
        "tomato": "TomatoGDD", "bean": "DryBeanGDD",
    }
    assert set(AQUACROP_CROPS.values()) <= set(supported_crops())


@pytest.mark.parametrize("ident,slug,crop", [
    ("wheat", "wheat", "WheatGDD"),
    ("TRZAX", "wheat", "WheatGDD"),
    ("HORVX", "barley", "BarleyGDD"),
    ("Zea mays", "maize", "MaizeGDD"),
])
def test_resolves_via_species_registry(ident, slug, crop):
    c = resolve_aquacrop_crop(ident)
    assert (c.slug, c.aquacrop_crop) == (slug, crop)
    assert c.note is None


def test_durum_wheat_declares_bread_wheat_calibration():
    c = resolve_aquacrop_crop("durum_wheat")
    assert c.aquacrop_crop == "WheatGDD"
    assert "durum" in c.note.lower() and "bread" in c.note.lower()


@pytest.mark.parametrize("ident", ["almond", "alfalfa", "olive"])
def test_unsupported_species_is_422_never_wheat(ident):
    with pytest.raises(SimulationError) as ei:
        resolve_aquacrop_crop(ident)
    assert ei.value.code == "unsupported_crop" and ei.value.status_code == 422


def test_unknown_identifier_is_unsupported_crop():
    with pytest.raises(SimulationError) as ei:
        resolve_aquacrop_crop("not-a-crop-xyz")
    assert ei.value.code == "unsupported_crop"
