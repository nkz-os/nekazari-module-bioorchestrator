"""The 84 localities of the GENVCE results map as field sites of the registry (scripts/genvce_trials_sites.py)."""
from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest

from app.kg.model import SITE_ID_PATTERN
from app.kg.registries import DEFAULT_REGISTRIES_PATH, load_registries

BACKEND = Path(__file__).resolve().parents[2]
SCRIPT = BACKEND / "scripts" / "genvce_trials_sites.py"
INDEX = Path(__file__).parent / "fixtures" / "genvce_trials" / "data" / "results_map" / "index.json"


def _script():
    spec = importlib.util.spec_from_file_location("genvce_trials_sites", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def reg():
    return load_registries(DEFAULT_REGISTRIES_PATH)


@pytest.fixture(scope="module")
def index() -> dict:
    return json.loads(INDEX.read_text(encoding="utf-8"))


def test_slug_ids_match_the_site_id_pattern(index):
    script = _script()
    assert script.slug_id("Cadreita") == "ES-GENVCE-TRIALS-CADREITA"
    assert script.slug_id("Clariana de Cardener") == "ES-GENVCE-TRIALS-CLARIANA-DE-CARDENER"
    assert script.slug_id("La Tallada d'Empordà - Verges") == "ES-GENVCE-TRIALS-LA-TALLADA-D-EMPORDA-VERGES"
    assert script.slug_id("Vilobí d'Onyar") == "ES-GENVCE-TRIALS-VILOBI-D-ONYAR"
    for locality in index["localities"]:
        assert re.fullmatch(SITE_ID_PATTERN, script.slug_id(locality["name"]))


def test_new_entries_are_field_sites_with_sourced_coordinates(index):
    script = _script()
    empty = load_registries(DEFAULT_REGISTRIES_PATH)
    new, _ = script.plan_entries(index, empty, download_date="2026-10-19", skip_registered=False)
    by_name = {entry["name"]: entry for entry in new}
    clariana = by_name["Clariana de Cardener"]
    assert clariana["site_kind"] == "field" and clariana["country"] == "ES"
    assert clariana["longitude"] == pytest.approx(1.5896391809) and clariana["longitude"] > 0
    assert clariana["coordinate_source"].startswith("GENVCE results map, localidad record id ")
    assert "2026-10-19" in clariana["coordinate_source"]
    assert clariana["sources"] == ["GENVCE-TRIALS"] and clariana["status"] == "assumption"
    assert clariana["note"].startswith("ASSUMPTION:")
    assert clariana["aliases"] == []


def test_a_locality_already_registered_is_a_collision_not_a_new_entry(index):
    script = _script()
    new, collisions = script.plan_entries(index, load_registries(DEFAULT_REGISTRIES_PATH), download_date="2026-10-19")
    assert "Zamadueñas" not in {entry["name"] for entry in new}
    (zamaduenas,) = [c for c in collisions if c["locality"] == "Zamadueñas"]
    assert zamaduenas["existing_id"] == "ES-ZAMADUENAS"
    assert zamaduenas["latitude"] == pytest.approx(41.71306)  # the map's locality record, rounded


def test_rendered_yaml_round_trips_into_the_registry_schema(index, tmp_path):
    import yaml

    script = _script()
    new, _ = script.plan_entries(index, load_registries(DEFAULT_REGISTRIES_PATH), download_date="2026-10-19",
                                 skip_registered=False)
    text = script.render(new)
    assert text.startswith("  - id: ES-GENVCE-TRIALS-")
    parsed = yaml.safe_load("sites:\n" + text)["sites"]
    assert [p["id"] for p in parsed] == [e["id"] for e in new]


def test_generation_is_idempotent_against_the_registry(index, reg):
    new, _ = _script().plan_entries(index, reg, download_date="2026-10-19")
    assert new == []


def test_every_index_locality_resolves_to_a_field_site_with_coordinates(index, reg):
    for locality in index["localities"]:
        site = reg.site(locality["name"])
        assert site is not None, locality["name"]
        assert site.site_kind == "field", locality["name"]
        # the registry keeps five decimals (about 1 m), the map publishes up to fifteen
        assert site.latitude == pytest.approx(locality["latitude"], abs=5.1e-6), locality["name"]
        assert site.longitude == pytest.approx(locality["longitude"], abs=5.1e-6), locality["name"]
