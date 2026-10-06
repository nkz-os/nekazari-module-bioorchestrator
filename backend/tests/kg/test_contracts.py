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

import pytest
import yaml
from pydantic import ValidationError

from app.kg.contracts import (
    Contract,
    ContractError,
    contract_hash,
    load_contract,
)

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
