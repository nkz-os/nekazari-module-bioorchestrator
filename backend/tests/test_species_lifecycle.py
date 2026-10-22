from app.species_registry import get_lifecycle, list_species


def test_every_species_has_a_lifecycle():
    assert all(get_lifecycle(s) in ("annual", "perennial") for s in list_species())


def test_examples():
    assert get_lifecycle("wheat") == "annual"
    assert get_lifecycle("grapevine") == "perennial"
    assert get_lifecycle("nonexistent") is None


def test_perennial_set_is_the_curated_one():
    from app.species_registry import get_species_info

    perennial_eppo = {
        get_species_info(s)["eppo_code"] for s in list_species() if get_lifecycle(s) == "perennial"
    }
    assert perennial_eppo == {
        "MEDSA", "ASPOF", "CYUCA", "CYUCD", "VITVI", "OLVEU", "PRNDU", "PIAVE",
        "MABSD", "PYUCO", "PRNPS", "PRNAR", "PRNDO", "PRNAV", "FRAAN",
    }
