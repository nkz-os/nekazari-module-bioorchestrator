"""Irrigation regime derived from a per-crop yield cutoff where the source states none.

Sections: the registry of cutoffs; the derivation in the contract engine (source wins, canonical field
only, two bands, derivation recorded, thresholds are data); the calibration on the rows whose regime
the source states; the real bundles (only where the private raw-data repository is available).
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from app.kg.registries import (
    DEFAULT_REGISTRIES_PATH,
    IRRIGATION_DERIVATION_V1,
    RegistryError,
    load_registries,
)

REGISTRIES = load_registries()


@pytest.fixture
def copy_dir(tmp_path: Path):
    dest = tmp_path / "registries"
    shutil.copytree(DEFAULT_REGISTRIES_PATH, dest)

    def edit(fn) -> Path:
        file = dest / "irrigation_thresholds.yaml"
        data = yaml.safe_load(file.read_text(encoding="utf-8"))
        fn(data)
        file.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return dest

    return dest, edit


def calibrated(crop="HORVX", low=4000.0, high=7000.0, **over):
    entry = {"crop": crop, "status": "assumption", "low_kg_ha": low, "high_kg_ha": high, "owner_approval": None,
             "evidence": {"method": "test", "epsilon": 0.05, "n_rainfed": 40, "n_irrigated": 30,
                          "labelled_sources": ["GENVCE"]}}
    entry.update(over)
    return entry


# ═════════════════════════════════════════════════════════════════════════════
# 1. the registry of cutoffs
# ═════════════════════════════════════════════════════════════════════════════

def test_the_file_is_part_of_the_registries_and_its_method_is_the_one_the_units_record():
    assert IRRIGATION_DERIVATION_V1 == "yield_threshold_v1"
    assert (DEFAULT_REGISTRIES_PATH / "irrigation_thresholds.yaml").is_file()
    assert {t.crop for t in REGISTRIES.irrigation_thresholds} == {"ZEAMX", "HORVX", "TRZAX", "BRSNN"}


def test_every_calibrated_value_is_an_assumption_awaiting_the_owner_and_every_gap_says_why():
    for threshold in REGISTRIES.irrigation_thresholds:
        assert not (threshold.owner_approval or "").strip(), "the owner has not approved anything yet"
        if threshold.status == "not_calibrated":
            assert threshold.evidence.reason.strip()
        else:
            assert threshold.status == "assumption" and threshold.low_kg_ha < threshold.high_kg_ha


def test_the_cutoffs_are_data_a_changed_file_changes_the_registries_hash(copy_dir):
    dest, edit = copy_dir
    before = load_registries(dest).registries_hash
    edit(lambda d: d["thresholds"].__setitem__(0, calibrated("ZEAMX")))
    assert load_registries(dest).registries_hash != before


def test_lookup_returns_only_calibrated_crops_and_accepts_aliases(copy_dir):
    dest, edit = copy_dir
    edit(lambda d: d["thresholds"].__setitem__(1, calibrated("HORVX")))
    reg = load_registries(dest)
    assert reg.irrigation_threshold("HORVX").low_kg_ha == 4000.0
    assert reg.irrigation_threshold("Cebada").high_kg_ha == 7000.0
    assert reg.irrigation_threshold("ZEAMX") is None  # not calibrated: no cutoff, never a default
    assert reg.irrigation_threshold("Narnia") is None and reg.irrigation_threshold(None) is None


@pytest.mark.parametrize(("entry", "message"), [
    (calibrated(low=7000.0, high=4000.0), "low must be below high"),
    (calibrated(low=5000.0, high=5000.0), "low must be below high"),
    (calibrated(low=None), "both thresholds"),
    (calibrated(evidence={"method": "t", "n_rainfed": 0, "n_irrigated": 30}), "both regimes"),
    ({"crop": "HORVX", "status": "not_calibrated", "low_kg_ha": 4000.0,
      "evidence": {"method": "t", "n_rainfed": 3, "n_irrigated": 1, "reason": "too few"}}, "no threshold values"),
    ({"crop": "HORVX", "status": "not_calibrated", "evidence": {"method": "t", "n_rainfed": 3, "n_irrigated": 1}},
     "says why"),
    ({"crop": "HORVX", "status": "not_calibrated", "owner_approval": "owner",
      "evidence": {"method": "t", "n_rainfed": 3, "n_irrigated": 1, "reason": "too few"}}, "nothing to approve"),
    (calibrated(status="reviewed"), "status"),
])
def test_a_malformed_entry_is_refused(copy_dir, entry, message):
    dest, edit = copy_dir
    edit(lambda d: d["thresholds"].__setitem__(1, entry))
    with pytest.raises(RegistryError, match=message):
        load_registries(dest)


def test_an_unknown_crop_or_a_duplicate_is_refused(copy_dir):
    dest, edit = copy_dir
    edit(lambda d: d["thresholds"].append(calibrated("TRZDU")))
    with pytest.raises(RegistryError, match="not a canonical EPPO code"):
        load_registries(dest)
    edit(lambda d: (d["thresholds"].pop(), d["thresholds"].append(calibrated("HORVX"))))
    with pytest.raises(RegistryError, match="duplicate"):
        load_registries(dest)


def test_another_method_than_the_recorded_one_is_refused(copy_dir):
    dest, edit = copy_dir
    edit(lambda d: d.update(method="yield_threshold_v2"))
    with pytest.raises(RegistryError):
        load_registries(dest)
