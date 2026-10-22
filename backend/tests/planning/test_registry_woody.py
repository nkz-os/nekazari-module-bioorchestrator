from app.species_registry import is_woody


def test_trees_and_vines_are_woody_herbaceous_perennials_are_not():
    assert is_woody("olive") and is_woody("grapevine") and is_woody("almond") and is_woody("cherry")
    assert not is_woody("alfalfa") and not is_woody("asparagus") and not is_woody("wheat")
    assert not is_woody("no-such-species")
