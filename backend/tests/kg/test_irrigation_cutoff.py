"""Irrigation regime derived from a per-crop yield cutoff where the source states none.

Sections: the registry of cutoffs; the derivation in the contract engine (source wins, canonical field
only, two bands, derivation recorded, thresholds are data); the calibration on the rows whose regime
the source states; the real bundles (only where the private raw-data repository is available).
"""
from __future__ import annotations

import copy
import json
import math
import os
import shutil
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from app.kg import identity
from app.kg.adapters import crea, genvce
from app.kg.contracts import (
    Contract,
    ContractError,
    Expected,
    IgnoreSpec,
    load_contract,
    run_contract,
)
from app.kg.irrigation_cutoff import (
    EPSILON,
    STEP_KG_HA,
    calibrate,
    classify_yield,
    quantile,
    separation,
)
from app.kg.model import UnitRow
from app.kg.registries import (
    DEFAULT_REGISTRIES_PATH,
    IRRIGATION_DERIVATION_V1,
    RegistryError,
    load_registries,
)
from scripts.kg_calibrate_irrigation import labelled_yields, render_thresholds

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


# ═════════════════════════════════════════════════════════════════════════════
# 3. the calibration on the rows whose regime the source states
# ═════════════════════════════════════════════════════════════════════════════

RAINFED_YIELDS = [3000 + 100 * i for i in range(40)]      # 3000 .. 6900
IRRIGATED_YIELDS = [6500 + 150 * i for i in range(30)]    # 6500 .. 10850


def test_the_rule_gives_the_cutoffs_to_their_bands():
    assert classify_yield(4000, 4000, 7000) == "rainfed"
    assert classify_yield(7000, 4000, 7000) == "irrigated"
    assert classify_yield(4000.5, 4000, 7000) is None and classify_yield(6999.5, 4000, 7000) is None


def test_quantiles_are_observed_values_nearest_rank():
    assert quantile([5, 1, 3, 2, 4], 0.5) == 3 and quantile([5, 1, 3, 2, 4], 0.05) == 1
    assert quantile([1, 2], 0.95) == 2
    with pytest.raises(ValueError):
        quantile([], 0.5)


def test_separable_regimes_get_cutoffs_that_misjudge_at_most_epsilon_of_each_class():
    c = calibrate("HORVX", RAINFED_YIELDS, IRRIGATED_YIELDS)
    assert c.calibrated and c.reason is None and c.low_kg_ha < c.high_kg_ha
    assert c.low_kg_ha % STEP_KG_HA == 0 and c.high_kg_ha % STEP_KG_HA == 0
    assert c.irrigated_judged_rainfed == sum(1 for y in IRRIGATED_YIELDS if y <= c.low_kg_ha)
    assert c.rainfed_judged_irrigated == sum(1 for y in RAINFED_YIELDS if y >= c.high_kg_ha)
    assert c.irrigated_judged_rainfed <= EPSILON * len(IRRIGATED_YIELDS)
    assert c.rainfed_judged_irrigated <= EPSILON * len(RAINFED_YIELDS)
    assert c.n_wrong == c.irrigated_judged_rainfed + c.rainfed_judged_irrigated
    assert c.error_rate == c.n_wrong / c.n_labelled
    assert c.decided_error_rate == c.n_wrong / c.n_decided and c.n_decided == c.n_labelled - c.n_ambiguous


def test_the_cutoffs_are_the_loosest_that_keep_the_error_bound_neither_tighter_nor_looser():
    c = calibrate("HORVX", RAINFED_YIELDS, IRRIGATED_YIELDS)
    limit_irrigated = math.floor(EPSILON * len(IRRIGATED_YIELDS))
    limit_rainfed = math.floor(EPSILON * len(RAINFED_YIELDS))
    # one step more on either side would break the bound
    assert sum(1 for y in IRRIGATED_YIELDS if y <= c.low_kg_ha + STEP_KG_HA) > limit_irrigated
    assert sum(1 for y in RAINFED_YIELDS if y >= c.high_kg_ha - STEP_KG_HA) > limit_rainfed


def test_calibration_depends_on_the_yields_not_on_their_order():
    first = calibrate("HORVX", RAINFED_YIELDS, IRRIGATED_YIELDS)
    second = calibrate("HORVX", RAINFED_YIELDS[::-1], list(reversed(IRRIGATED_YIELDS)))
    assert first == second


@pytest.mark.parametrize(("rainfed", "irrigated", "why"), [
    (RAINFED_YIELDS, [], "irrigated 0"),
    ([], IRRIGATED_YIELDS, "rainfed 0"),
    ([], [], "rainfed 0, irrigated 0"),
    (RAINFED_YIELDS[:14], IRRIGATED_YIELDS, "rainfed 14"),
    (RAINFED_YIELDS, IRRIGATED_YIELDS[:14], "irrigated 14"),
])
def test_too_few_labelled_rows_in_a_regime_means_no_cutoff_is_invented(rainfed, irrigated, why):
    c = calibrate("ZEAMX", rainfed, irrigated)
    assert not c.calibrated and c.low_kg_ha is None and c.high_kg_ha is None
    assert "no cutoff is invented" in c.reason and why in c.reason
    assert c.n_wrong is None and c.error_rate is None


def test_fifteen_labelled_rows_per_regime_are_enough_and_fourteen_are_not():
    spread = lambda start, n: [start + 200 * i for i in range(n)]
    assert calibrate("X", spread(3000, 15), spread(9000, 15)).calibrated
    assert not calibrate("X", spread(3000, 14), spread(9000, 15)).calibrated
    assert not calibrate("X", spread(3000, 15), spread(9000, 14)).calibrated


def test_regimes_with_the_same_yields_get_no_cutoff_and_the_reason_says_so():
    same = [3000 + 100 * i for i in range(30)]
    c = calibrate("BRSNN", same, same)
    assert not c.calibrated and "do not separate" in c.reason
    assert c.separation == pytest.approx(0.5, abs=0.02)


def test_perfectly_separated_regimes_get_a_cut_in_the_middle_of_the_gap_with_a_one_step_undecided_band():
    rainfed = [3000.0 + 200 * i for i in range(15)]    # 3000 .. 5800
    irrigated = [9000.0 + 200 * i for i in range(15)]  # 9000 .. 11800
    c = calibrate("HORVX", rainfed, irrigated)
    assert c.calibrated and (c.low_kg_ha, c.high_kg_ha) == (7400.0, 7500.0)
    assert c.n_wrong == 0 and c.n_ambiguous == 0 and c.error_rate == 0 == c.decided_error_rate


def test_a_cutoff_that_decides_few_rows_and_decides_them_badly_is_refused():
    rainfed = [3000.0] * 19 + [4000.0]
    irrigated = [2800.0] + [3000.0] * 19
    c = calibrate("BRSNN", rainfed, irrigated)
    assert (c.n_rainfed, c.n_irrigated) == (20, 20)
    assert not c.calibrated and "do not separate" in c.reason


def test_separation_is_the_probability_the_irrigated_yield_is_higher():
    assert separation([1, 2, 3], [4, 5, 6]) == 1.0
    assert separation([4, 5, 6], [1, 2, 3]) == 0.0
    assert separation([1, 2], [1, 2]) == pytest.approx(0.5)
    assert separation([], [1]) is None


def test_derived_units_never_feed_a_calibration(barley_registries):
    """Labelled rows are the ones whose regime the source states; a derived regime is not a label."""
    bundle = build([silent(3500), silent(8000, variety="B")], barley_registries)
    assert {u.irrigation_regime for u in bundle.units} == {RAINFED, IRRIGATED}
    assert not labelled_yields([bundle], barley_registries)


def test_labelled_yields_are_the_stated_regimes_per_crop_with_their_sources(barley_registries):
    bundle = build([raw_row(irrigation="secano", **{"yield": 4100}), raw_row(irrigation="regadío", variety="B", **{"yield": 8200}),
                    raw_row(irrigation="regadío", variety="C", **{"yield": None}), silent(5000, variety="D")],
                   barley_registries, observations=7)
    labelled = labelled_yields([bundle], barley_registries)
    assert labelled["HORVX"]["rainfed"] == [4100.0] and labelled["HORVX"]["irrigated"] == [8200.0]
    assert labelled["HORVX"]["sources"] == {"GENVCE"}


def test_the_thresholds_the_script_renders_load_back_through_the_registry(copy_dir):
    dest, _ = copy_dir
    calibrations = [calibrate("HORVX", RAINFED_YIELDS, IRRIGATED_YIELDS),
                    calibrate("ZEAMX", [], IRRIGATED_YIELDS), calibrate("TRZAX", [], []),
                    calibrate("BRSNN", RAINFED_YIELDS, RAINFED_YIELDS)]
    text = render_thresholds(calibrations, "abc123", {"HORVX": {"GENVCE"}, "ZEAMX": {"CREA"}})
    file = dest / "irrigation_thresholds.yaml"
    head = file.read_text(encoding="utf-8").split("\nthresholds:\n", 1)[0]
    file.write_text(head + "\nthresholds:\n" + text, encoding="utf-8")
    reg = load_registries(dest)
    horvx = reg.irrigation_threshold("HORVX")
    assert (horvx.low_kg_ha, horvx.high_kg_ha) == (calibrations[0].low_kg_ha, calibrations[0].high_kg_ha)
    assert horvx.status == "assumption" and horvx.owner_approval is None
    assert horvx.evidence.n_rainfed == 40 and horvx.evidence.n_irrigated == 30
    assert horvx.evidence.raw_data_commit == "abc123" and horvx.evidence.labelled_sources == ("GENVCE",)
    assert horvx.evidence.error_rate == pytest.approx(calibrations[0].error_rate, abs=1e-4)
    for crop in ("ZEAMX", "TRZAX", "BRSNN"):
        assert reg.irrigation_threshold(crop) is None
        assert next(t for t in reg.irrigation_thresholds if t.crop == crop).evidence.reason


# ═════════════════════════════════════════════════════════════════════════════
# 4. the real contracts: GENVCE and CREA fixtures, then the whole raw data
# ═════════════════════════════════════════════════════════════════════════════

SOURCES = Path(__file__).resolve().parents[2] / "data" / "sources"
FIXTURES = Path(__file__).parent / "fixtures"


def real_bundle(source_id, adapter, rows, expected=None, derive=True):
    loaded = load_contract(SOURCES / f"{source_id}.yaml")
    if not derive:
        spec = loaded.unit.irrigation_derivation
        ignored = (IgnoreSpec(field=spec.not_derivable_when, reason="read only by the derivation")
                   if spec is not None and spec.not_derivable_when else None)
        loaded = loaded.model_copy(update={
            "unit": loaded.unit.model_copy(update={"irrigation_derivation": None}),
            "ignore": (*loaded.ignore, *([ignored] if ignored else []))})
    if expected is not None:
        loaded = loaded.model_copy(update={"expected": Expected(**expected)})
    return run_contract(loaded, REGISTRIES, rows)


@pytest.fixture(scope="module")
def fixture_bundles():
    out = {}
    for source_id, adapter, expected in (
        ("GENVCE", genvce, {"units": 32, "observations": 233, "sites": 14}),
        ("CREA", crea, {"units": 16, "observations": 132, "sites": 5}),
    ):
        rows = adapter.load(FIXTURES / source_id.lower()).rows
        out[source_id] = (real_bundle(source_id, adapter, rows, expected),
                          real_bundle(source_id, adapter, rows, expected, derive=False))
    return out


@pytest.mark.parametrize("source_id", ["GENVCE", "CREA"])
def test_both_contracts_ask_for_the_derivation_and_name_no_number(source_id):
    spec = load_contract(SOURCES / f"{source_id}.yaml").unit.irrigation_derivation
    assert spec is not None and spec.method == "yield_threshold_v1"
    text = (SOURCES / f"{source_id}.yaml").read_text(encoding="utf-8")
    assert "low_kg_ha" not in text and "high_kg_ha" not in text  # the cutoffs live in the registry only


@pytest.mark.parametrize("source_id", ["GENVCE", "CREA"])
def test_on_the_fixtures_every_unit_follows_the_rule_and_the_keys_do_not_move(fixture_bundles, source_id):
    with_cutoff, without = fixture_bundles[source_id]
    assert [identity.unit_key(u) for u in with_cutoff.units] == [identity.unit_key(u) for u in without.units]
    for unit in with_cutoff.units:
        if unit.raw_irrigation is not None:
            assert unit.irrigation_derivation is None  # the source wins
            continue
        threshold = REGISTRIES.irrigation_threshold(unit.crop_eppo)
        if threshold is None or unit.yield_kg_ha is None:
            assert unit.irrigation_regime is None and unit.irrigation_derivation is None
            continue
        assert unit.irrigation_derivation == "yield_threshold_v1"
        assert (unit.irrigation_yield_low_kg_ha, unit.irrigation_yield_high_kg_ha) == (
            threshold.low_kg_ha, threshold.high_kg_ha)
        band = classify_yield(unit.yield_kg_ha, threshold.low_kg_ha, threshold.high_kg_ha)
        assert unit.irrigation_regime == (REGISTRIES.vocab("irrigation", band) if band else None)
        assert (band is None) == any(g.reason.startswith("irrigation_ambiguous_yield") for g in unit.gaps)
    assert all(u.raw_irrigation is None for u in with_cutoff.units if u.irrigation_derivation)


def test_the_real_registry_has_no_cutoff_for_any_crop_so_no_regime_is_derived(fixture_bundles):
    assert all(t.status == "not_calibrated" and t.low_kg_ha is None for t in REGISTRIES.irrigation_thresholds)
    assert REGISTRIES.irrigation_threshold("HORVX") is None
    for with_cutoff, _ in fixture_bundles.values():
        assert not any(u.irrigation_derivation for u in with_cutoff.units)
        assert not with_cutoff.report.irrigation_derived or all(
            k.endswith(":not_derivable") for k in with_cutoff.report.irrigation_derived)
        assert all(u.irrigation_regime is None for u in with_cutoff.units if u.raw_irrigation is None)


def _genvce_with_barley_cutoffs(copy_dir_):
    dest, edit = copy_dir_
    edit(lambda d: d["thresholds"].__setitem__(1, calibrated("HORVX", 4000.0, 7000.0)))
    rows = genvce.load(FIXTURES / "genvce").rows
    loaded = load_contract(SOURCES / "GENVCE.yaml").model_copy(update={"expected": Expected(units=32, observations=233, sites=14)})
    return run_contract(loaded, load_registries(dest), rows)


def test_a_mixed_regime_or_yield_stratum_group_never_gets_a_regime_even_with_cutoffs(copy_dir):
    bundle = _genvce_with_barley_cutoffs(copy_dir)
    blocked = [u for u in bundle.units if u.raw_irrigation is None and u.raw_site and (
        "regad" in u.raw_site.casefold() and "secano" in u.raw_site.casefold()
        or u.raw_site.casefold().startswith(("rendimiento", "productividad")))]
    assert blocked, "fixture has no such group: the test would be vacuous"
    for unit in blocked:
        assert unit.irrigation_regime is None and unit.irrigation_derivation is None
        assert any(g.field == "irrigation_regime" and g.reason.startswith("irrigation_not_derivable") for g in unit.gaps)
    # the cutoffs do work on the other silent barley units
    assert any(u.irrigation_derivation for u in bundle.units if u not in blocked)
    assert not any(u.irrigation_derivation for u in bundle.units if u in blocked)
    assert any(k.endswith(":not_derivable") for k in bundle.report.irrigation_derived)


def test_the_yield_stratum_is_kept_as_its_own_field_and_changes_no_key(fixture_bundles):
    with_cutoff, without = fixture_bundles["GENVCE"]
    assert [identity.unit_key(u) for u in with_cutoff.units] == [identity.unit_key(u) for u in without.units]
    strata = {u.raw_site: u.productivity_class for u in with_cutoff.units if u.productivity_class}
    assert strata.get("Rendimiento bajo") == "yield_stratum_low"
    assert strata.get("Secanos áridos y semiáridos fríos") == "rainfed_arid_semiarid"
    assert all(u.productivity_class is None for u in with_cutoff.units if u.raw_site == "Secanos templados")


RAW_REPO = os.environ.get("NKZ_DATA_SOURCES_DIR", "")


@pytest.mark.skipif(not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to the raw-data repository to run")
def test_the_whole_raw_data_derives_no_regime_and_changes_no_key():
    result = genvce.load(Path(RAW_REPO) / "genvce")
    with_cutoff = real_bundle("GENVCE", genvce, result.rows)
    without = real_bundle("GENVCE", genvce, result.rows, derive=False)
    assert [identity.unit_key(u) for u in with_cutoff.units] == [identity.unit_key(u) for u in without.units]
    derived = with_cutoff.report.irrigation_derived
    assert not derived or all(k.endswith(":not_derivable") for k in derived), "no crop has a cutoff"
    assert not any(u.irrigation_regime for u in with_cutoff.units if u.raw_irrigation is None)
    assert not any(u.irrigation_derivation for u in with_cutoff.units)
    assert any(u.productivity_class for u in with_cutoff.units)
    stated = [u for u in with_cutoff.units if u.raw_irrigation is not None]
    assert stated and all(u.irrigation_derivation is None for u in stated)
    assert not any(u.irrigation_regime for u in with_cutoff.units if u.crop_eppo != "HORVX" and not u.raw_irrigation)
    print(f"\nGENVCE derived by band: {json.dumps(derived, sort_keys=True)}")


@pytest.mark.skipif(not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to the raw-data repository to run")
def test_maize_has_no_cutoff_so_no_crea_unit_is_derived_and_every_stated_regime_stays():
    result = crea.load(Path(RAW_REPO) / "crea")
    with_cutoff = real_bundle("CREA", crea, result.rows)
    assert with_cutoff.report.irrigation_derived == {}
    assert sum(1 for u in with_cutoff.units if u.raw_irrigation) == 149
    assert all(u.irrigation_derivation is None and u.irrigation_regime is None
               for u in with_cutoff.units if u.raw_irrigation is None)
