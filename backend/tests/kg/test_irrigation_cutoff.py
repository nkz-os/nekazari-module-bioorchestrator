"""Irrigation regime derived from a per-crop yield cutoff where the source states none.

Sections: the registry of cutoffs; the derivation in the contract engine (source wins, canonical field
only, two bands, derivation recorded, thresholds are data); the calibration on the rows whose regime
the source states; the real bundles (only where the private raw-data repository is available).
"""
from __future__ import annotations

import copy
import shutil
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from app.kg import identity
from app.kg.contracts import Contract, ContractError, run_contract
from app.kg.model import UnitRow
from app.kg.registries import (
    DEFAULT_REGISTRIES_PATH,
    IRRIGATION_DERIVATION_V1,
    RegistryError,
    load_registries,
)

from .test_contracts import base_contract, raw_row

REGISTRIES = load_registries()
RAINFED = REGISTRIES.vocab("irrigation", "rainfed")
IRRIGATED = REGISTRIES.vocab("irrigation", "irrigated")


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


# ═════════════════════════════════════════════════════════════════════════════
# 2. the derivation in the contract engine
# ═════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def barley_registries(copy_dir):
    """The registries with barley calibrated at 4000 / 7000 kg/ha (a synthetic pair, not the real one)."""
    dest, edit = copy_dir
    edit(lambda d: d["thresholds"].__setitem__(1, calibrated("HORVX", 4000.0, 7000.0)))
    return load_registries(dest)


def contract(derive=True, **over) -> dict:
    data = base_contract(**over)
    if derive:
        data["unit"]["irrigation_derivation"] = {"method": "yield_threshold_v1"}
    return data


def build(rows, registries, data=None, observations=None):
    data = copy.deepcopy(data or contract())
    data["expected"] = {"units": len(rows), "observations": 2 * len(rows) if observations is None else observations,
                        "sites": 0}
    return run_contract(Contract.model_validate(data), registries, rows)


def silent(yield_value, **over):
    return raw_row(irrigation=None, **{"yield": yield_value}, **over)


def test_the_source_wins_a_stated_regime_is_used_whatever_the_yield(barley_registries):
    bundle = build([raw_row(irrigation="regadío", **{"yield": 3000}),
                    raw_row(irrigation="secano", variety="B", **{"yield": 9000})], barley_registries)
    by_variety = {u.raw_variety: u for u in bundle.units}
    assert by_variety["MAYA"].irrigation_regime == IRRIGATED and by_variety["B"].irrigation_regime == RAINFED
    for unit in bundle.units:  # an observed regime has no derivation record
        assert unit.irrigation_derivation is None and unit.irrigation_yield_low_kg_ha is None
    assert not bundle.report.irrigation_derived


def test_a_stated_regime_the_vocabulary_does_not_know_is_still_the_sources_word_and_is_not_overridden(
        barley_registries):
    (unit,) = build([raw_row(irrigation="a veces", **{"yield": 9000})], barley_registries).units
    assert unit.raw_irrigation == "a veces" and unit.irrigation_regime is None
    assert unit.irrigation_derivation is None


def test_a_silent_row_gets_the_regime_on_the_canonical_field_and_the_raw_field_stays_empty(barley_registries):
    low, high = build([silent(3500), silent(8000, variety="B")], barley_registries).units
    for unit in (low, high):
        assert unit.raw_irrigation is None
        assert any(g.field == "raw_irrigation" for g in unit.gaps)  # the source still says nothing
    assert low.irrigation_regime == RAINFED and high.irrigation_regime == IRRIGATED


def test_a_derived_regime_is_told_from_an_observed_one_by_its_recorded_method_and_thresholds(barley_registries):
    (unit,) = build([silent(3500)], barley_registries).units
    assert unit.irrigation_derivation == IRRIGATION_DERIVATION_V1 == "yield_threshold_v1"
    assert (unit.irrigation_yield_low_kg_ha, unit.irrigation_yield_high_kg_ha) == (4000.0, 7000.0)


def test_the_unit_key_does_not_change_when_a_regime_is_derived(barley_registries):
    rows = [silent(3500), silent(8000, variety="B"), silent(5500, variety="C"), raw_row(variety="D")]
    with_cutoff = build(rows, barley_registries)
    without = build(rows, barley_registries, contract(derive=False))
    assert [identity.unit_key(u) for u in with_cutoff.units] == [identity.unit_key(u) for u in without.units]
    assert all(u.irrigation_regime is None for u in without.units if u.raw_irrigation is None)


@pytest.mark.parametrize(("yield_kg_ha", "regime"), [
    (1000, RAINFED), (3999.9, RAINFED), (4000, RAINFED),          # yield <= low
    (4000.1, None), (5500, None), (6999.9, None),                  # in between
    (7000, IRRIGATED), (7000.1, IRRIGATED), (12000, IRRIGATED),    # yield >= high
])
def test_two_bands_per_crop_with_the_cutoffs_themselves_inside_the_bands(barley_registries, yield_kg_ha, regime):
    (unit,) = build([silent(yield_kg_ha)], barley_registries).units
    assert unit.irrigation_regime == regime


def test_a_yield_between_the_bands_has_no_regime_and_the_gap_names_why(barley_registries):
    (unit,) = build([silent(5500)], barley_registries).units
    assert unit.irrigation_regime is None
    (gap,) = [g for g in unit.gaps if g.field == "irrigation_regime"]
    assert gap.reason.startswith("irrigation_ambiguous_yield") and "4000" in gap.reason and "7000" in gap.reason
    # the rule was evaluated, so the cutoffs it used are recorded even though no regime came out
    assert unit.irrigation_derivation == IRRIGATION_DERIVATION_V1


def test_a_unit_without_a_yield_stays_null_and_records_no_derivation(barley_registries):
    (unit,) = build([raw_row(irrigation=None, **{"yield": None})], barley_registries, observations=1).units
    assert unit.irrigation_regime is None and unit.irrigation_derivation is None
    assert not any("irrigation_ambiguous_yield" in g.reason for g in unit.gaps)


def test_a_crop_without_a_cutoff_is_left_alone(barley_registries):
    (unit,) = build([silent(9000, crop="ZEAMX")], barley_registries).units
    assert barley_registries.irrigation_threshold("ZEAMX") is None
    assert unit.irrigation_regime is None and unit.irrigation_derivation is None


def test_the_report_counts_what_the_cutoff_judged_per_crop(barley_registries):
    bundle = build([silent(3000), silent(3100, variety="B"), silent(8000, variety="C"), silent(5500, variety="D"),
                    raw_row(variety="E")], barley_registries)
    assert bundle.report.irrigation_derived == {"HORVX:ambiguous": 1, "HORVX:irrigated": 1, "HORVX:rainfed": 2}


def test_the_thresholds_are_data_the_contract_names_the_method_only(copy_dir):
    dest, edit = copy_dir
    rows = [silent(5000)]
    edit(lambda d: d["thresholds"].__setitem__(1, calibrated("HORVX", 4000.0, 7000.0)))
    assert build(rows, load_registries(dest)).units[0].irrigation_regime is None
    edit(lambda d: d["thresholds"].__setitem__(1, calibrated("HORVX", 3000.0, 4500.0)))
    assert build(rows, load_registries(dest)).units[0].irrigation_regime == IRRIGATED  # same contract, new data
    with pytest.raises(ValidationError):  # nothing numeric fits in the contract
        Contract.model_validate({**contract(), "unit": {**contract()["unit"],
                                 "irrigation_derivation": {"method": "yield_threshold_v1", "low": 4000}}})
    with pytest.raises(ValidationError):
        Contract.model_validate({**contract(), "unit": {**contract()["unit"],
                                 "irrigation_derivation": {"method": "yield_threshold_v2"}}})


def test_a_derivation_needs_the_irrigation_field_and_a_yield(barley_registries):
    no_field = contract()
    del no_field["unit"]["fields"]["irrigation"]
    with pytest.raises(ContractError, match="reads no irrigation field"):
        build([silent(5000)], barley_registries, no_field)
    no_yield = contract()
    del no_yield["unit"]["yield"]
    del no_yield["unit"]["purpose"]
    with pytest.raises(ContractError, match="needs a unit yield"):
        build([silent(5000)], barley_registries, no_yield)


def test_a_row_cannot_carry_both_a_stated_regime_and_a_derivation():
    base = {"source_id": "GENVCE", "document_key": "a" * 64, "crop_eppo": "HORVX", "raw_irrigation": "secano",
            "yield_kg_ha": 3000.0, "yield_metric": "grain", "yield_basis": "standard_moisture",
            "yield_moisture_pct": 13.0, "yield_value_original": 3000.0, "yield_unit_original": "kg/ha",
            "purpose": "grain", "irrigation_derivation": "yield_threshold_v1",
            "irrigation_yield_low_kg_ha": 4000.0, "irrigation_yield_high_kg_ha": 7000.0}
    with pytest.raises(ValidationError, match="the source wins"):
        UnitRow(**base)
    stated_nothing = {**base, "raw_irrigation": None}
    UnitRow(**stated_nothing)
    with pytest.raises(ValidationError, match="both thresholds"):
        UnitRow(**{**stated_nothing, "irrigation_yield_high_kg_ha": None})
    with pytest.raises(ValidationError, match="without irrigation_derivation"):
        UnitRow(**{**stated_nothing, "irrigation_derivation": None})
    with pytest.raises(ValidationError, match="without a yield"):
        UnitRow(source_id="GENVCE", document_key="a" * 64, crop_eppo="HORVX", irrigation_derivation="yield_threshold_v1", irrigation_yield_low_kg_ha=4000.0, irrigation_yield_high_kg_ha=7000.0)
