"""Tests for species_registry crop_group resolution."""

from __future__ import annotations

from app.species_registry import get_crop_group, resolve_species


def test_crop_group_resolution():
    """EPPO codes resolve to the expected crop_group."""
    assert get_crop_group(resolve_species("HORVX")) == "cereal"        # barley
    assert get_crop_group(resolve_species("VITVI")) == "grapevine"     # grapevine
    assert get_crop_group(resolve_species("MABSD")) == "pome_fruit"    # apple
    assert get_crop_group(resolve_species("LYPES")) == "solanaceous"   # tomato
    assert get_crop_group("nonexistent_slug") is None


def test_crop_group_absent_for_ungrouped_species():
    """Species without a clear crop_group resolve to None."""
    assert get_crop_group(resolve_species("GLXMA")) is None  # soybean (legume)
    assert get_crop_group(resolve_species("OLVEU")) is None  # olive
