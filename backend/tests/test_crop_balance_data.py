"""The committed crop-balance coefficients: valid, redistributable, attributed, registered crops."""
import json
from pathlib import Path

import yaml

from app.kg.registries import DEFAULT_REGISTRIES_PATH, load_registries
from app.services.crop_balance import validate
from app.species_registry import resolve_species

DATA = Path(__file__).resolve().parents[1] / "data"
DOC = yaml.safe_load((DATA / "crop_balance_coefficients.yaml").read_text(encoding="utf-8"))


def test_file_validates_against_loadable_sources():
    allowed = {s.source_id for s in load_registries(DEFAULT_REGISTRIES_PATH).loadable_sources()}
    assert [str(e) for e in validate(DOC, allowed)] == []


def test_every_used_source_has_a_structured_attribution():
    registry = json.loads((DATA / "sources_registry.json").read_text(encoding="utf-8"))
    attributed = {e["source_id"] for e in registry
                  if e.get("attribution_text") and e.get("licence_url")}
    used = {v["source_id"] for c in DOC["crops"].values() for v in c["values"].values()}
    assert used <= attributed


def test_crops_are_registered_species_and_core_crops_present():
    # resolve_species honours eppo_aliases (ZEAMX resolves to maize).
    unresolved = sorted(c for c in DOC["crops"] if resolve_species(c) is None)
    assert unresolved == []
    assert {"TRZAX", "HORVX"} <= set(DOC["crops"])
