"""The phenology seed writes Species.agrovocUri into the graph: it must use the registry's verified concepts."""
from pathlib import Path

import yaml

DATA = Path(__file__).resolve().parent.parent / "data"


def test_phenology_species_use_the_registry_agrovoc_concept():
    registry = yaml.safe_load((DATA / "species_registry.yaml").read_text(encoding="utf-8"))["species"]
    by_eppo = {s.get("eppo_code"): s.get("agrovoc_uri") for s in registry.values()}
    by_name = {s["scientific_name"].lower(): s.get("agrovoc_uri") for s in registry.values()}
    phenology = yaml.safe_load((DATA / "phenology_sources.yaml").read_text(encoding="utf-8"))["species"]
    expected = {sp["name"]: by_eppo.get(sp.get("eppo_code")) or by_name.get(sp["scientific_name"].lower())
                for sp in phenology}
    assert all(expected.values()), {n: u for n, u in expected.items() if not u}
    mismatched = {sp["name"]: (sp.get("agrovoc_uri"), expected[sp["name"]])
                  for sp in phenology if sp.get("agrovoc_uri") != expected[sp["name"]]}
    assert mismatched == {}
