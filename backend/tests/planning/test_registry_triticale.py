from app.species_registry import get_lifecycle, get_species_info, resolve_species


def test_triticale_resolves_from_its_trial_code():
    assert resolve_species("TTLSS") == "triticale"
    info = get_species_info("triticale")
    assert info["crop_group"] == "cereal" and get_lifecycle("triticale") == "annual"
    assert set(info["common_names"]) == {"es", "en", "eu", "fr", "ca", "pt"}
