"""Tests for species_registry crop_group resolution."""

from __future__ import annotations

from app.species_registry import (
    CATALOG_SIBLING_CODES,
    catalog_code,
    get_crop_group,
    get_species_info,
    list_species,
    resolve_species,
)


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


# Alias codes of the SAME species (``eppo_aliases``): code -> (slug, scientific name).
_ALIASES = {
    "ZEAMX": ("maize", "Zea mays"),
    "BRSNW": ("rapeseed", "Brassica napus"),
    "PIBAR": ("pea", "Pisum sativum"),
    "BRSOX": ("broccoli", "Brassica oleracea var. italica"),
    "BROOL": ("broccoli", "Brassica oleracea var. italica"),
    "CIEAS": ("chickpea", "Cicer arietinum"),
    "LINUS": ("flax", "Linum usitatissimum"),
    "CYNSC": ("artichoke", "Cynara cardunculus var. scolymus"),
    "CYNCA": ("cardoon", "Cynara cardunculus"),
    "CNUSA": ("hemp", "Cannabis sativa"),
}


def test_maize_resolves_from_both_codes():
    assert resolve_species("ZEAMX") == resolve_species("ZEAMA") == "maize"
    assert resolve_species("zeamx") == "maize"
    assert get_crop_group(resolve_species("ZEAMX")) == "cereal"


def test_alias_codes_resolve_to_the_same_species_with_the_same_scientific_name():
    for code, (slug, scientific) in _ALIASES.items():
        assert resolve_species(code) == slug, code
        assert get_species_info(slug)["scientific_name"] == scientific, code


def test_aliases_are_exactly_the_documented_ones_and_never_collide():
    declared = {}
    for slug in list_species():
        info = get_species_info(slug)
        for code in info.get("eppo_aliases") or []:
            assert code not in declared, f"{code} aliased twice"
            declared[code] = slug
            # an alias is never another species' primary code
            assert all(get_species_info(s)["eppo_code"] != code for s in list_species())
    assert declared == {code: slug for code, (slug, _) in _ALIASES.items()}


def test_codes_of_other_species_or_missing_from_the_registry_stay_unresolved():
    # spelt (not wheat), triticale, sorghum and lupin have no entry here: no alias is invented
    for code in ("TRZAW", "TRZSP", "TTLSS", "SORVU", "LUPAL"):
        assert resolve_species(code) is None, code


def test_catalog_siblings_are_the_same_species_and_only_maize_is_merged():
    assert CATALOG_SIBLING_CODES == {"ZEAMA": "ZEAMX"}
    for sibling, listed in CATALOG_SIBLING_CODES.items():
        assert resolve_species(sibling) == resolve_species(listed) is not None
    assert catalog_code("ZEAMA") == "ZEAMX" and catalog_code("ZEAMX") == "ZEAMX"
    for code in ("BRSNW", "PIBAR", "CIEAS", "TRZAX", "unknown"):
        assert catalog_code(code) == code
