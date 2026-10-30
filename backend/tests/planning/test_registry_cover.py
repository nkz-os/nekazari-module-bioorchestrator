from app.species_registry import (
    get_lifecycle,
    get_species_info,
    is_cover_only,
    list_species,
    resolve_species,
)


def test_cover_species_resolve_from_their_codes():
    assert resolve_species("VICVI") == "hairy_vetch"
    assert resolve_species("TRFIN") == "crimson_clover"
    for slug in ("hairy_vetch", "crimson_clover"):
        info = get_species_info(slug)
        assert info["crop_group"] == "legume" and get_lifecycle(slug) == "annual"
        assert set(info["common_names"]) == {"es", "en", "eu", "fr", "ca", "pt"}
        assert slug in list_species()


def test_cover_only_flag():
    assert is_cover_only("hairy_vetch") and is_cover_only("crimson_clover")
    # A crop that is also sown as a cover (vetch, oat) stays a main crop.
    assert not is_cover_only("vetch") and not is_cover_only("oat") and not is_cover_only("wheat")
    assert not is_cover_only("no_such_species")


def test_reference_cover_species_are_cover_only():
    expected = {
        "italian_ryegrass": ("Lolium multiflorum", "LOLMU"),
        "white_mustard": ("Sinapis alba", "SINAL"),
        "forage_radish": ("Raphanus sativus", "RAPSR"),
        "phacelia": ("Phacelia tanacetifolia", "PHCTA"),
    }
    for slug, (sci, eppo) in expected.items():
        info = get_species_info(slug)
        assert info["scientific_name"] == sci and info["eppo_code"] == eppo
        assert resolve_species(sci) == slug and resolve_species(eppo) == slug
        assert is_cover_only(slug) and get_lifecycle(slug) == "annual"
        assert set(info["common_names"]) == {"es", "en", "eu", "fr", "ca", "pt"}
        assert slug in list_species()
    # The oil radish subspecies is NOT the forage radish: no alias merges them.
    assert resolve_species("Raphanus sativus oleiformis") is None
