"""Static checks on the AGROVOC concept URIs of the species registry (no network)."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import yaml

_REGISTRY = Path(__file__).resolve().parents[1] / "data" / "species_registry.yaml"
_AGROVOC_URI = re.compile(r"^http://aims\.fao\.org/aos/agrovoc/c_\d+$")


def _species() -> dict:
    with _REGISTRY.open("r") as f:
        return yaml.safe_load(f)["species"]


def test_agrovoc_uris_are_well_formed():
    bad = {
        slug: sp["agrovoc_uri"]
        for slug, sp in _species().items()
        if "agrovoc_uri" in sp and not _AGROVOC_URI.match(str(sp["agrovoc_uri"]))
    }
    assert not bad, f"malformed agrovoc_uri: {bad}"


def test_agrovoc_uris_are_unique_across_species():
    by_uri: dict[str, list[str]] = defaultdict(list)
    for slug, sp in _species().items():
        if sp.get("agrovoc_uri"):
            by_uri[sp["agrovoc_uri"]].append(slug)
    shared = {uri: slugs for uri, slugs in by_uri.items() if len(slugs) > 1}
    assert not shared, f"agrovoc_uri shared by several species: {shared}"
