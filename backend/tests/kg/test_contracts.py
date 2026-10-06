"""Contract engine: schema (closed, justified, reasoned) and ``run_contract`` over synthetic rows.

Only synthetic contracts and raw rows are used here (the real GENVCE and CREA contracts are a later
task); the registries are the real ones, so a vocabulary, a unit or a variable is exactly what the
build will see. The five tests the plan pins come first, in each section: a synthetic contract gives
the expected bundle; an unmapped field raises; an ignore without a reason is invalid; unit conversion
keeps the original; a metric or basis default without a justification is rejected. Then the rulings
carried forward from review: per-crop yield moisture, a purpose on every yield, skipped range checks
counted, the site key taking an observed zone or stratum, and observation qualifiers.
"""
from __future__ import annotations

import copy
import random

import pytest
import yaml
from pydantic import ValidationError

from app.kg import identity
from app.kg.contracts import (
    Contract,
    ContractDataError,
    ContractError,
    ExpectedCountError,
    UnmappedFieldError,
    contract_hash,
    load_contract,
    run_contract,
)
from app.kg.registries import load_registries

REGISTRIES = load_registries()
RAINFED = REGISTRIES.vocab("irrigation", "rainfed")
IRRIGATED = REGISTRIES.vocab("irrigation", "irrigated")

JUSTIFIED = "synthetic: the report states it"


def base_contract(**over) -> dict:
    """A small valid contract (plain dict, as it would be parsed from YAML)."""
    contract = {
        "source_id": "GENVCE",
        "raw": {"repo": "synthetic-raw", "paths": ["rows/*.json"], "extraction_version": "t1"},
        "adapter": "app.kg.adapters.synthetic",
        "document": {
            "title": {"from": "doc.title"},
            "issue": {"from": "doc.issue"},
            "year": {"from": "doc.year"},
            "locator": [{"label": "page", "from": "doc.page"}, {"label": "table", "from": "doc.table"}],
        },
        "study": {"type": "variety", "group_by": ["network"]},
        "unit": {
            "fields": {
                "crop": {"from": "crop"},
                "variety": {"from": "variety"},
                "site": {"from": "zone"},
                "season": {"from": "year"},
                "irrigation": {"from": "irrigation", "vocab": "irrigation"},
            },
            "purpose": {"default": "grain", "justification": JUSTIFIED},
            "yield": {
                "from": "yield",
                "unit": "kg/ha",
                "metric": {"default": "grain", "justification": JUSTIFIED},
                "basis": {"default": "standard_moisture", "justification": JUSTIFIED},
                "moisture_pct": {"default": 13, "justification": JUSTIFIED},
            },
        },
        "observations": [{"from": "quality.protein", "variable": "grain_protein_content", "unit": "%"}],
        "ignore": [{"field": "_validation", "reason": "extraction metadata"}],
        "sites": {"aggregate_patterns": []},
        "expected": {"units": 1, "observations": 2, "sites": 0},
    }
    contract.update(over)
    return contract


# ═════════════════════════════════════════════════════════════════════════════
# 1. Schema
# ═════════════════════════════════════════════════════════════════════════════

def test_a_valid_contract_parses_and_hashes_stably():
    contract = Contract.model_validate(base_contract())
    assert contract.source_id == "GENVCE"
    assert contract.unit.yield_.metric.default == "grain"
    assert contract_hash(contract) == contract_hash(Contract.model_validate(base_contract()))
    changed = base_contract()
    changed["expected"]["units"] = 2
    assert contract_hash(Contract.model_validate(changed)) != contract_hash(contract)


@pytest.mark.parametrize("entry", [
    {"field": "_validation"},
    {"field": "_validation", "reason": ""},
    {"field": "_validation", "reason": "   "},
])
def test_an_ignore_entry_without_a_reason_is_invalid(entry):
    with pytest.raises(ValidationError, match="reason"):
        Contract.model_validate(base_contract(ignore=[entry]))


@pytest.mark.parametrize("section, key", [("metric", "justification"), ("basis", "justification"),
                                          ("moisture_pct", "justification")])
def test_a_yield_default_without_a_justification_is_rejected(section, key):
    contract = base_contract()
    del contract["unit"]["yield"][section][key]
    with pytest.raises(ValidationError, match="default without a justification"):
        Contract.model_validate(contract)


@pytest.mark.parametrize("blank", ["", "  "])
def test_a_blank_justification_is_rejected(blank):
    contract = base_contract()
    contract["unit"]["yield"]["basis"]["justification"] = blank
    with pytest.raises(ValidationError, match="default without a justification"):
        Contract.model_validate(contract)


def test_a_purpose_default_without_a_justification_is_rejected():
    contract = base_contract()
    del contract["unit"]["purpose"]["justification"]
    with pytest.raises(ValidationError, match="purpose: a default without a justification"):
        Contract.model_validate(contract)


def test_every_by_crop_value_needs_its_own_justification():
    contract = base_contract()
    contract["unit"]["yield"]["moisture_pct"] = {"by_crop": {
        "HORVX": {"default": 13, "justification": JUSTIFIED},
        "ZEAMX": {"default": 14},
    }}
    with pytest.raises(ValidationError, match="default without a justification"):
        Contract.model_validate(contract)


def test_a_yield_without_a_declared_purpose_is_rejected():
    contract = base_contract()
    del contract["unit"]["purpose"]
    with pytest.raises(ValidationError, match="every yield carries a purpose"):
        Contract.model_validate(contract)


def test_a_declared_value_is_exactly_one_of_from_default_by_crop():
    contract = base_contract()
    contract["unit"]["yield"]["metric"] = {"from": "metric", "default": "grain", "justification": JUSTIFIED}
    with pytest.raises(ValidationError, match="exactly one of from, default or by_crop"):
        Contract.model_validate(contract)
    contract["unit"]["yield"]["metric"] = {}
    with pytest.raises(ValidationError, match="exactly one of from, default or by_crop"):
        Contract.model_validate(contract)


def test_a_justification_with_a_raw_field_is_rejected():
    contract = base_contract()
    contract["unit"]["yield"]["basis"] = {"from": "basis", "justification": JUSTIFIED}
    with pytest.raises(ValidationError, match="only goes with a default"):
        Contract.model_validate(contract)


def test_the_schema_is_closed_unknown_keys_are_rejected_at_every_level():
    for path in [(), ("document",), ("unit",), ("unit", "fields"), ("unit", "yield"),
                 ("study",), ("expected",), ("raw",), ("unit", "yield", "metric")]:
        contract = base_contract()
        node = contract
        for step in path:
            node = node[step]
        node["surprise"] = 1
        with pytest.raises(ValidationError, match="surprise"):
            Contract.model_validate(contract)
    contract = base_contract()
    contract["observations"][0]["surprise"] = 1
    with pytest.raises(ValidationError, match="surprise"):
        Contract.model_validate(contract)


def test_yield_is_not_an_observation_entry():
    contract = base_contract(observations=[{"from": "y2", "variable": "crop_yield", "unit": "kg/ha"}])
    with pytest.raises(ValidationError, match="unit.yield section"):
        Contract.model_validate(contract)


def test_a_raw_field_cannot_be_both_mapped_and_ignored():
    for ignored in ("quality", "quality.protein", "yield"):
        contract = base_contract(ignore=[{"field": ignored, "reason": "r"}])
        with pytest.raises(ValidationError, match="both mapped and ignored"):
            Contract.model_validate(contract)


def test_a_raw_field_read_as_two_values_is_rejected():
    contract = base_contract(observations=[
        {"from": "quality.protein", "variable": "grain_protein_content", "unit": "%"},
        {"from": "quality.protein", "variable": "seed_oil_content", "unit": "%"},
    ])
    with pytest.raises(ValidationError, match="more than once"):
        Contract.model_validate(contract)


@pytest.mark.parametrize("bad", ["", ".a", "a.", "a..b", " a"])
def test_raw_paths_are_well_formed(bad):
    contract = base_contract()
    contract["unit"]["fields"]["variety"] = {"from": bad}
    with pytest.raises(ValidationError, match="raw field path"):
        Contract.model_validate(contract)


def test_an_invalid_aggregate_pattern_is_rejected():
    with pytest.raises(ValidationError, match="regular expression"):
        Contract.model_validate(base_contract(sites={"aggregate_patterns": ["(unclosed"]}))


def test_irrigation_field_must_name_its_vocabulary():
    contract = base_contract()
    contract["unit"]["fields"]["irrigation"] = {"from": "irrigation", "vocab": "production_system"}
    with pytest.raises(ValidationError, match="irrigation"):
        Contract.model_validate(contract)


def test_load_contract_reads_yaml_and_reports_problems_as_contract_errors(tmp_path):
    good = tmp_path / "good.yaml"
    good.write_text(yaml.safe_dump(base_contract()), encoding="utf-8")
    assert load_contract(good).source_id == "GENVCE"

    bad_data = copy.deepcopy(base_contract())
    bad_data["ignore"] = [{"field": "x"}]
    bad = tmp_path / "bad.yaml"
    bad.write_text(yaml.safe_dump(bad_data), encoding="utf-8")
    with pytest.raises(ContractError, match="bad.yaml"):
        load_contract(bad)
    with pytest.raises(ContractError, match="cannot read"):
        load_contract(tmp_path / "missing.yaml")


# ═════════════════════════════════════════════════════════════════════════════
# 2. The engine over synthetic rows
# ═════════════════════════════════════════════════════════════════════════════

def raw_row(**over) -> dict:
    """One synthetic raw row (nested as an adapter would emit it)."""
    row = {
        "doc": {"title": "Informe sintetico 2021", "issue": "n1", "year": 2021, "page": 12, "table": "3"},
        "crop": "Cebada",
        "variety": "MAYA",
        "zone": "Zona Fria Semiarida",
        "year": 2021,
        "irrigation": "secano",
        "network": "red-a",
        "yield": 6200,
        "quality": {"protein": 11.5},
        "_validation": {"ok": True},
    }
    row.update(over)
    return row


def run(rows, contract=None, *, units=None, observations=None, sites=0):
    """Run the contract; the expected counts default to a row-per-unit, two observations per row."""
    data = copy.deepcopy(contract or base_contract())
    data["expected"] = {
        "units": len(rows) if units is None else units,
        "observations": 2 * len(rows) if observations is None else observations,
        "sites": sites,
    }
    return run_contract(Contract.model_validate(data), REGISTRIES, rows)


def test_a_synthetic_contract_and_rows_produce_the_expected_bundle():
    bundle = run([raw_row()])

    assert bundle.source_id == "GENVCE"
    (document,) = bundle.documents
    assert (document.title, document.issue, document.year) == ("Informe sintetico 2021", "n1", 2021)
    (study,) = bundle.studies
    assert (study.study_type, study.crop_eppo, study.raw_season, study.raw_scope, study.year) == (
        "variety", "HORVX", "2021", "network=red-a", 2021)
    (variety,) = bundle.varieties
    assert (variety.crop_eppo, variety.name, variety.registry_id, variety.status) == (
        "HORVX", "MAYA", "HORVX:maya", "assumption")

    (unit,) = bundle.units
    assert unit.source_id == "GENVCE" and unit.crop_eppo == "HORVX"
    assert (unit.raw_variety, unit.raw_site, unit.raw_season, unit.raw_irrigation) == (
        "MAYA", "Zona Fria Semiarida", "2021", "secano")
    assert unit.irrigation_regime == RAINFED and unit.year == 2021
    assert unit.document_key == identity.document_key(document)
    assert unit.study_key == identity.study_key(study)
    assert unit.variety_key == identity.variety_key(variety)
    assert unit.locator == "page 12; table 3"
    assert (unit.purpose, unit.yield_kg_ha, unit.yield_metric, unit.yield_basis) == (
        "grain", 6200.0, "grain", "standard_moisture")

    by_variable = {obs.variable_id: obs for obs in bundle.observations}
    assert set(by_variable) == {"crop_yield", "grain_protein_content"}
    protein = by_variable["grain_protein_content"]
    assert (protein.value, protein.unit, protein.value_original, protein.unit_original) == (11.5, "%", 11.5, "%")
    assert protein.raw_key == "quality.protein" and protein.locator == "page 12; table 3"
    assert protein.metric is None and protein.purpose is None
    assert all(obs.unit_key == identity.unit_key(unit) for obs in bundle.observations)
    assert bundle.report.raw_rows == 1
    assert bundle.report.contract_hash == contract_hash(Contract.model_validate(base_contract() | {
        "expected": {"units": 1, "observations": 2, "sites": 0}}))
    assert bundle.report.registries_hash == REGISTRIES.registries_hash


def test_yield_yields_both_an_observation_and_the_units_derived_copy():
    """Amendment E4: the Observation is the truth; the unit's yield fields are a derived copy."""
    bundle = run([raw_row()])
    (unit,) = bundle.units
    (obs,) = (o for o in bundle.observations if o.variable_id == "crop_yield")
    assert obs.value == unit.yield_kg_ha == 6200.0
    assert obs.unit == "kg/ha"
    assert obs.metric == unit.yield_metric == "grain"
    assert obs.basis == unit.yield_basis == "standard_moisture"
    assert obs.moisture_pct == unit.yield_moisture_pct == 13.0
    assert obs.purpose == unit.purpose == "grain"
    assert obs.value_original == unit.yield_value_original == 6200.0
    assert obs.unit_original == unit.yield_unit_original == "kg/ha"
    assert obs.raw_key == "yield"


def test_an_unmapped_raw_field_raises_and_names_it():
    rows = [raw_row(surprise="x"), raw_row(surprise="y", quality={"protein": 10.0, "fat": 2.0}),
            raw_row(spare=None)]
    with pytest.raises(UnmappedFieldError) as caught:
        run(rows)
    assert caught.value.fields == {"quality.fat": 1, "spare": 1, "surprise": 2}
    assert "neither mapped nor ignored" in str(caught.value)


def test_an_ignored_field_covers_its_sub_fields_and_nothing_else():
    contract = base_contract()
    contract["ignore"] = [{"field": "_validation", "reason": "extraction metadata"}]
    run([raw_row(_validation={"ok": True, "deep": {"x": 1}})], contract)  # nested leaves are covered
    with pytest.raises(UnmappedFieldError, match="_validation_other"):
        run([raw_row(_validation_other=1)], contract)  # a longer name is not covered by the prefix


def test_unit_conversion_is_applied_and_the_original_value_and_unit_are_kept():
    contract = base_contract(source_id="CREA")
    contract["unit"]["yield"]["unit"] = "q/ha"
    contract["unit"]["yield"]["moisture_pct"] = {"default": 15.5, "justification": JUSTIFIED}
    bundle = run([raw_row(crop="Mais", **{"yield": 85})], contract)
    (unit,) = bundle.units
    (obs,) = (o for o in bundle.observations if o.variable_id == "crop_yield")
    assert obs.value == 8500.0 and obs.unit == "kg/ha"
    assert (obs.value_original, obs.unit_original) == (85.0, "q/ha")
    assert unit.yield_kg_ha == 8500.0
    assert (unit.yield_value_original, unit.yield_unit_original) == (85.0, "q/ha")
    assert unit.crop_eppo == "ZEAMX"


def test_a_missing_value_is_none_plus_an_explicit_gap_never_a_guess():
    row = raw_row(variety=None, irrigation="  ", **{"yield": None})
    bundle = run([row], observations=1)
    (unit,) = bundle.units
    assert unit.raw_variety is None and unit.variety_key is None and not bundle.varieties
    assert unit.raw_irrigation is None and unit.irrigation_regime is None
    assert unit.yield_kg_ha is None and unit.yield_metric is None and unit.yield_basis is None
    assert unit.purpose == "grain"  # a property of the unit, declared by the contract
    gaps = {gap.field: gap.reason for gap in unit.gaps}
    assert {"raw_variety", "raw_irrigation", "yield_kg_ha"} <= set(gaps)
    assert "'variety'" in gaps["raw_variety"]
    assert [o.variable_id for o in bundle.observations] == ["grain_protein_content"]
    assert bundle.report.missing["variety"] == 1
    assert bundle.report.gaps["unit.yield_kg_ha"] == 1


def test_vocabularies_are_applied_and_an_unknown_literal_is_reported_not_guessed():
    bundle = run([raw_row(irrigation="regadío"), raw_row(irrigation="a veces", zone="otra")], units=2,
                 observations=4)
    by_raw = {u.raw_irrigation: u for u in bundle.units}
    assert by_raw["regadío"].irrigation_regime == IRRIGATED
    odd = by_raw["a veces"]
    assert odd.irrigation_regime is None
    assert any(g.field == "irrigation_regime" and "a veces" in g.reason for g in odd.gaps)
    assert bundle.report.unresolved_vocab == {"irrigation:a veces": 1}


def test_a_registered_alias_resolves_to_the_registry_variety_and_an_unknown_name_is_a_candidate():
    bundle = run([raw_row(variety="96054-518"), raw_row(variety="maya", zone="b"),
                  raw_row(variety="Brand New", zone="c"), raw_row(variety="brand  new", zone="d")],
                 units=4, observations=8)
    by_status = {v.name: v for v in bundle.varieties}
    assert set(by_status) == {"MAYA", "Brand New"}  # alias and case variant collapse; one spelling kept
    assert by_status["MAYA"].registry_id == "HORVX:maya" and by_status["MAYA"].status == "assumption"
    assert by_status["Brand New"].status == "candidate" and by_status["Brand New"].registry_id is None
    keys = {u.raw_variety: u.variety_key for u in bundle.units}
    assert keys["96054-518"] == keys["maya"] == identity.variety_key(by_status["MAYA"])
    assert bundle.report.unregistered_varieties == {"HORVX:Brand New": 2}


def test_documents_and_studies_are_deduplicated_and_provenance_is_kept_per_unit():
    rows = [raw_row(zone="a"), raw_row(zone="b", doc={**raw_row()["doc"], "page": 14})]
    bundle = run(rows, units=2, observations=4)
    assert len(bundle.documents) == 1 and len(bundle.studies) == 1
    assert sorted(u.locator for u in bundle.units) == ["page 12; table 3", "page 14; table 3"]


def test_one_document_with_two_different_urls_is_a_conflict():
    contract = base_contract()
    contract["document"]["url"] = {"from": "doc.url"}
    rows = [raw_row(zone="a", doc={**raw_row()["doc"], "url": "https://example.test/a"}),
            raw_row(zone="b", doc={**raw_row()["doc"], "url": "https://example.test/b"})]
    with pytest.raises(ContractDataError, match="document .* more than one"):
        run(rows, contract)


def test_the_bundle_does_not_depend_on_the_order_of_the_raw_rows():
    rows = [raw_row(zone=f"z{i}", variety=("MAYA" if i % 2 else f"new-{i}"),
                    **{"yield": 5000 + i}) for i in range(12)]
    expected = run(rows, units=12, observations=24)
    for seed in range(3):
        shuffled = rows[:]
        random.Random(seed).shuffle(shuffled)
        again = run(shuffled, units=12, observations=24)
        assert again.model_dump() == expected.model_dump()


def test_exact_duplicate_raw_rows_collapse_and_are_counted():
    bundle = run([raw_row(), raw_row()], units=1, observations=2)
    assert len(bundle.units) == 1 and len(bundle.observations) == 2
    assert bundle.report.raw_rows == 2
    assert (bundle.report.collapsed_duplicate_units, bundle.report.collapsed_duplicate_observations) == (1, 2)


def test_the_same_unit_with_a_different_value_is_a_conflict_not_a_silent_choice():
    with pytest.raises(ContractDataError, match="same unit key.*yield_kg_ha"):
        run([raw_row(), raw_row(**{"yield": 7000})], units=1, observations=2)


def test_the_expected_counts_are_enforced():
    contract = base_contract()
    contract["expected"] = {"units": 2, "observations": 2, "sites": 0}
    with pytest.raises(ExpectedCountError, match="units: expected 2, got 1"):
        run_contract(Contract.model_validate(contract), REGISTRIES, [raw_row()])


@pytest.mark.parametrize("mutate, message", [
    (lambda c: c["observations"][0].update(variable="no_such_variable"), "unregistered variable"),
    (lambda c: c["observations"][0].update(unit="furlongs"), "furlongs"),
    (lambda c: c["observations"][0].update(unit="kg/ha"), "dimension"),
    (lambda c: c["observations"][0].pop("unit"), "state the unit"),
    (lambda c: c["unit"]["yield"].update(unit="%"), "dimension"),
    (lambda c: c["study"].update(type="banana"), "study_type"),
    (lambda c: c["unit"]["yield"]["metric"].update(default="banana"), "yield_metric"),
    (lambda c: c["unit"]["purpose"].update(default="forage"), "contradict"),
    (lambda c: c.update(source_id="NOPE"), "unregistered source"),
    (lambda c: c["unit"]["yield"]["moisture_pct"].update(default=140), "moisture"),
    (lambda c: c["unit"]["yield"].pop("moisture_pct"), "standard_moisture"),
    (lambda c: c["unit"]["yield"]["basis"].update(default="dry_matter"), "only goes with"),
])
def test_a_contract_inconsistent_with_the_registries_fails_before_any_row(mutate, message):
    contract = base_contract()
    mutate(contract)
    with pytest.raises(ContractError, match=message):
        run_contract(Contract.model_validate(contract), REGISTRIES, [])


def test_a_variable_without_a_unit_takes_no_unit_and_an_ordinal_value_stays_in_its_domain():
    contract = base_contract()
    contract["observations"] += [
        {"from": "score", "variable": "emergence_score"},
        {"from": "cycle", "variable": "cycle_class"},
        {"from": "heading", "variable": "heading_date"},
    ]
    bundle = run([raw_row(score=3, cycle="short", heading="12-may")], contract, observations=5)
    by_variable = {o.variable_id: o for o in bundle.observations}
    assert by_variable["emergence_score"].value == 3.0 and by_variable["emergence_score"].unit is None
    assert by_variable["cycle_class"].value_text == "short" and by_variable["cycle_class"].value is None
    assert by_variable["heading_date"].value_text == "12-may"
    with pytest.raises(ContractDataError, match="outside the domain"):
        run([raw_row(score=45, cycle="short", heading="x")], contract, observations=5)
    contract["observations"][1]["unit"] = "%"
    with pytest.raises(ContractError, match="has no unit"):
        run_contract(Contract.model_validate(contract), REGISTRIES, [])


@pytest.mark.parametrize("row, message", [
    (raw_row(**{"yield": "n.d."}), "not a number"),
    (raw_row(**{"yield": True}), "not a number"),
    (raw_row(**{"yield": float("nan")}), "not finite"),
    (raw_row(**{"yield": -5}), "yield_kg_ha"),
    (raw_row(crop="Quinoa de Marte"), "crops registry"),
    (raw_row(crop=None), "crop"),
    (raw_row(doc={"issue": "n1"}), "title"),
    ("not a row", "mapping"),
])
def test_a_raw_row_that_cannot_be_mapped_fails_loudly_with_its_position(row, message):
    with pytest.raises(ContractDataError, match=message) as caught:
        run([raw_row(), row], units=2, observations=4)
    assert caught.value.errors[0].startswith("row 1:")


def test_row_problems_are_collected_not_just_the_first():
    with pytest.raises(ContractDataError) as caught:
        run([raw_row(**{"yield": "x"}), raw_row(crop="???"), raw_row(**{"yield": "y"})])
    assert len(caught.value.errors) == 3
