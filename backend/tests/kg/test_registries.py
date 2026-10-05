"""Registry data and loader: schema validation of every file, uniqueness, lookups."""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from app.kg.registries import (
    DEFAULT_REGISTRIES_PATH,
    REGISTRY_FILES,
    RegistryError,
    load_registries,
)


@pytest.fixture(scope="module")
def reg():
    return load_registries(DEFAULT_REGISTRIES_PATH)


@pytest.fixture
def copy_dir(tmp_path: Path):
    """A writable copy of the registry directory, plus a helper that edits one file."""
    dest = tmp_path / "registries"
    shutil.copytree(DEFAULT_REGISTRIES_PATH, dest)

    def edit(name: str, fn) -> Path:
        file = dest / name
        data = yaml.safe_load(file.read_text(encoding="utf-8"))
        fn(data)
        file.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return dest

    return dest, edit


# ── loader ───────────────────────────────────────────────────────────────────

def test_every_registry_file_exists_and_loads(reg):
    for name in REGISTRY_FILES:
        assert (DEFAULT_REGISTRIES_PATH / name).is_file(), name
    assert len(reg.registries_hash) == 64


def test_registries_hash_is_stable_and_changes_with_content(copy_dir):
    dest, edit = copy_dir
    first = load_registries(dest).registries_hash
    assert load_registries(dest).registries_hash == first
    edit("crops.yaml", lambda d: d["crops"][0].update(notes="changed"))
    assert load_registries(dest).registries_hash != first


def test_missing_file_is_an_error(copy_dir):
    dest, _ = copy_dir
    (dest / "crops.yaml").unlink()
    with pytest.raises(RegistryError, match="missing registry file"):
        load_registries(dest)


def test_unknown_field_is_rejected(copy_dir):
    dest, edit = copy_dir
    edit("crops.yaml", lambda d: d["crops"][0].update(surprise=1))
    with pytest.raises(RegistryError, match="schema validation failed"):
        load_registries(dest)


# ── crops ────────────────────────────────────────────────────────────────────

def test_crops_cover_the_raw_data_codes(reg):
    codes = {c.eppo for c in reg.crops}
    assert {"ZEAMX", "HORVX", "TRZAX", "BRSNN"} <= codes
    seen = {c.eppo for c in reg.crops if c.seen_in}
    assert seen == {"ZEAMX", "HORVX", "TRZAX", "BRSNN"}


@pytest.mark.parametrize(
    ("raw", "eppo"),
    [
        ("ZEAMA", "ZEAMX"),
        ("eppo:ZEAMX", "ZEAMX"),
        ("zeamx", "ZEAMX"),
        ("CIEAS", "CIEAR"),
        ("BRSNW", "BRSNN"),
        ("Cebada de ciclo largo", "HORVX"),
        ("  cebada   DE invierno ", "HORVX"),
        ("Trigo blando ecológico de invierno", "TRZAX"),
        ("Mais", "ZEAMX"),
        ("Colza de otoño", "BRSNN"),
    ],
)
def test_crop_resolves_codes_aliases_and_raw_labels(reg, raw, eppo):
    assert reg.crop(raw).eppo == eppo


@pytest.mark.parametrize("raw", [None, "", "TRZAW", "Grano duro", "Grano tenero", "unknown crop"])
def test_crop_unresolved_is_none(reg, raw):
    assert reg.crop(raw) is None


def test_every_alias_resolves_to_exactly_one_crop(reg):
    owners: dict[str, set[str]] = {}
    for crop in reg.crops:
        for name in (crop.eppo, *crop.aliases):
            owners.setdefault(" ".join(name.casefold().split()), set()).add(crop.eppo)
    assert {k: v for k, v in owners.items() if len(v) > 1} == {}
    for crop in reg.crops:
        for name in (crop.eppo, *crop.aliases):
            assert reg.crop(name) is crop


def test_duplicate_crop_eppo_is_an_error(copy_dir):
    dest, edit = copy_dir

    def dup(d):
        d["crops"].append({**d["crops"][0], "aliases": []})

    edit("crops.yaml", dup)
    with pytest.raises(RegistryError, match="duplicate crop eppo"):
        load_registries(dest)


def test_alias_shared_by_two_crops_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("crops.yaml", lambda d: d["crops"][1]["aliases"].append("zeama"))
    with pytest.raises(RegistryError, match="crop alias"):
        load_registries(dest)
