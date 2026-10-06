"""Canonical row models: what a row may and may not say, before any key is computed."""
from __future__ import annotations

import datetime as dt

import pytest
from pydantic import ValidationError

from app.kg.model import (
    STANDARD_MOISTURE_BASIS,
    DocumentRow,
    FactorLevel,
    Gap,
    ObservationRow,
    SiteRow,
    StudyRow,
    UnitRow,
    VarietyRow,
)
from app.kg.registries import DEFAULT_REGISTRIES_PATH, load_registries

DOC_KEY = "d" * 64
UNIT_KEY = "a" * 64


def make_unit(**over) -> UnitRow:
    base = {
        "source_id": "GENVCE",
        "document_key": DOC_KEY,
        "crop_eppo": "HORVX",
        "raw_variety": "MAYA",
        "raw_site": "Valladolid",
        "raw_season": "2021",
    }
    base.update(over)
    return UnitRow(**base)


YIELD = {
    "yield_kg_ha": 6200.0,
    "yield_metric": "grain",
    "yield_basis": "standard_moisture",
    "yield_moisture_pct": 13.0,
    "yield_value_original": 6200.0,
    "yield_unit_original": "kg/ha",
    "purpose": "grain",
}


def make_obs(**over) -> ObservationRow:
    base = {"unit_key": UNIT_KEY, "variable_id": "crop_yield", "value": 6200.0}
    base.update(over)
    return ObservationRow(**base)


YIELD_OBS = {
    "unit": "kg/ha",
    "metric": "grain",
    "basis": "standard_moisture",
    "moisture_pct": 13.0,
    "purpose": "grain",
    "value_original": 6200.0,
    "unit_original": "kg/ha",
}


# ── the standard-moisture basis id is the registry's ─────────────────────────

def test_standard_moisture_constant_is_a_registry_yield_basis():
    reg = load_registries(DEFAULT_REGISTRIES_PATH)
    assert reg.vocab_entry("yield_basis", STANDARD_MOISTURE_BASIS) is not None


# ── rows are immutable and closed ────────────────────────────────────────────

def test_rows_are_frozen_and_reject_unknown_fields():
    unit = make_unit()
    with pytest.raises(ValidationError):
        unit.raw_variety = "other"  # frozen
    with pytest.raises(ValidationError):
        make_unit(surprise="x")


def test_blank_raw_text_is_missing():
    unit = make_unit(raw_variety="   ", rootstock="", clone=None)
    assert unit.raw_variety is None and unit.rootstock is None and unit.clone is None


def test_raw_text_is_kept_as_observed():
    assert make_unit(raw_variety="  MAYA (T) ").raw_variety == "  MAYA (T) "


# ── UnitRow ──────────────────────────────────────────────────────────────────

def test_unit_without_yield_is_valid_and_has_no_yield_fields():
    unit = make_unit()
    assert unit.yield_kg_ha is None and unit.yield_basis is None and unit.yield_moisture_pct is None


def test_unit_with_a_complete_yield_is_valid():
    unit = make_unit(**YIELD)
    assert unit.yield_moisture_pct == 13.0 and unit.yield_basis == "standard_moisture"


@pytest.mark.parametrize(
    "missing", ["yield_metric", "yield_basis", "yield_value_original", "yield_unit_original", "purpose"],
)
def test_a_yield_needs_metric_basis_original_value_unit_and_purpose(missing):
    data = {**YIELD, missing: None}
    with pytest.raises(ValidationError, match=missing):
        make_unit(**data)


def test_standard_moisture_needs_the_moisture_percentage():
    data = {**YIELD, "yield_moisture_pct": None}
    with pytest.raises(ValidationError, match="yield_moisture_pct"):
        make_unit(**data)


def test_moisture_percentage_is_only_for_the_standard_moisture_basis():
    data = {**YIELD, "yield_basis": "dry_matter"}
    with pytest.raises(ValidationError, match="yield_moisture_pct"):
        make_unit(**data)
    ok = {**YIELD, "yield_basis": "dry_matter", "yield_moisture_pct": None}
    assert make_unit(**ok).yield_basis == "dry_matter"


def test_unknown_basis_needs_no_moisture():
    data = {**YIELD, "yield_basis": "unknown", "yield_moisture_pct": None}
    assert make_unit(**data).yield_basis == "unknown"


@pytest.mark.parametrize("moisture", [0.0, -1.0, 100.0, 150.0])
def test_moisture_percentage_is_strictly_between_0_and_100(moisture):
    with pytest.raises(ValidationError):
        make_unit(**{**YIELD, "yield_moisture_pct": moisture})


def test_a_yield_field_without_a_yield_is_an_error():
    with pytest.raises(ValidationError, match="yield_kg_ha"):
        make_unit(yield_metric="grain")
    with pytest.raises(ValidationError, match="yield_kg_ha"):
        make_unit(yield_moisture_pct=13.0)


@pytest.mark.parametrize("bad", [-1.0, float("nan"), float("inf")])
def test_yield_is_a_finite_non_negative_number(bad):
    with pytest.raises(ValidationError):
        make_unit(**{**YIELD, "yield_kg_ha": bad})


def test_planting_year_is_an_integer_and_season_stays_text():
    assert make_unit(planting_year=2015).planting_year == 2015
    with pytest.raises(ValidationError):
        make_unit(planting_year=2015.5)


def test_crop_must_be_a_canonical_eppo_code():
    for bad in ("horvx", "Cebada", "", "HOR"):
        with pytest.raises(ValidationError):
            make_unit(crop_eppo=bad)


def test_document_key_must_be_a_sha256_hex():
    for bad in ("", "xyz", "A" * 64, "a" * 63):
        with pytest.raises(ValidationError):
            make_unit(document_key=bad)


def test_factor_levels_are_typed_and_unique_per_factor():
    unit = make_unit(factor_levels=(
        FactorLevel(factor="N_dose", level=120, unit="kg/ha"),
        FactorLevel(factor="tillage", level="no-till"),
    ))
    assert len(unit.factor_levels) == 2
    with pytest.raises(ValidationError, match="N_dose"):
        make_unit(factor_levels=(
            FactorLevel(factor="N_dose", level=120),
            FactorLevel(factor="N_dose", level=150),
        ))


def test_factor_level_rejects_blank_and_non_finite_values():
    for kwargs in ({"factor": " ", "level": 1}, {"factor": "x", "level": " "},
                   {"factor": "x", "level": float("nan")}):
        with pytest.raises(ValidationError):
            FactorLevel(**kwargs)


# ── explicit gaps ────────────────────────────────────────────────────────────

def test_a_gap_names_a_null_optional_field_with_its_reason():
    unit = make_unit(gaps=(Gap(field="raw_irrigation", reason="the report does not state it"),))
    assert unit.gaps[0].field == "raw_irrigation"


def test_a_gap_on_a_field_that_has_a_value_is_an_error():
    with pytest.raises(ValidationError, match="raw_variety"):
        make_unit(gaps=(Gap(field="raw_variety", reason="x"),))


def test_a_gap_on_an_unknown_or_required_field_is_an_error():
    with pytest.raises(ValidationError, match="nonsense"):
        make_unit(gaps=(Gap(field="nonsense", reason="x"),))
    with pytest.raises(ValidationError, match="source_id"):
        make_unit(gaps=(Gap(field="source_id", reason="x"),))


def test_the_same_field_cannot_be_a_gap_twice():
    with pytest.raises(ValidationError, match="raw_irrigation"):
        make_unit(gaps=(Gap(field="raw_irrigation", reason="a"), Gap(field="raw_irrigation", reason="b")))


def test_a_gap_needs_a_reason():
    with pytest.raises(ValidationError):
        Gap(field="raw_irrigation", reason=" ")


# ── ObservationRow ───────────────────────────────────────────────────────────

def test_observation_carries_exactly_one_of_value_and_value_text():
    assert make_obs().value == 6200.0
    assert make_obs(value=None, value_text="resistant").value_text == "resistant"
    with pytest.raises(ValidationError, match="value"):
        make_obs(value=None)
    with pytest.raises(ValidationError, match="value"):
        make_obs(value_text="x")


def test_observation_value_is_finite():
    for bad in (float("nan"), float("inf")):
        with pytest.raises(ValidationError):
            make_obs(value=bad)


def test_variable_id_has_the_registry_shape():
    for bad in ("Crop Yield", "1x", ""):
        with pytest.raises(ValidationError):
            make_obs(variable_id=bad)


def test_observation_date_is_a_date_and_stage_text():
    obs = make_obs(date="2021-05-03", stage="heading")
    assert obs.date == dt.date(2021, 5, 3) and obs.stage == "heading"


def test_a_yield_observation_with_full_context_is_valid():
    obs = make_obs(**YIELD_OBS)
    assert obs.metric == "grain" and obs.moisture_pct == 13.0


@pytest.mark.parametrize("missing", ["purpose", "basis", "unit", "value_original", "unit_original"])
def test_an_observation_with_a_metric_needs_purpose_basis_unit_and_original(missing):
    with pytest.raises(ValidationError, match=missing):
        make_obs(**{**YIELD_OBS, missing: None})


def test_a_purpose_without_a_metric_is_an_error():
    with pytest.raises(ValidationError, match="metric"):
        make_obs(purpose="grain")


def test_observation_moisture_rules_follow_the_basis():
    with pytest.raises(ValidationError, match="moisture_pct"):
        make_obs(**{**YIELD_OBS, "moisture_pct": None})
    with pytest.raises(ValidationError, match="moisture_pct"):
        make_obs(**{**YIELD_OBS, "basis": "fresh_matter"})
    assert make_obs(**{**YIELD_OBS, "basis": "fresh_matter", "moisture_pct": None}).basis == "fresh_matter"


def test_a_non_yield_observation_may_carry_a_basis_without_moisture():
    assert make_obs(variable_id="protein_pct", basis="dry_matter").basis == "dry_matter"


# ── DocumentRow / StudyRow ───────────────────────────────────────────────────

def test_document_needs_a_source_and_a_title():
    doc = DocumentRow(source_id="GENVCE", title="Resultados cebada 2021", year=2021)
    assert doc.issue is None
    with pytest.raises(ValidationError):
        DocumentRow(source_id="GENVCE", title="  ")


def test_document_fingerprint_is_a_sha256_hex():
    with pytest.raises(ValidationError):
        DocumentRow(source_id="GENVCE", title="t", raw_sha256="nothex")
    assert DocumentRow(source_id="GENVCE", title="t", raw_sha256="0" * 64).raw_sha256 == "0" * 64


def test_study_needs_source_type_and_name():
    study = StudyRow(source_id="GENVCE", study_type="variety", crop_eppo="HORVX",
                     raw_season="2021", name="GENVCE · cebada · 2021")
    assert study.raw_scope is None
    with pytest.raises(ValidationError):
        StudyRow(source_id="GENVCE", study_type="variety", name=" ")


# ── SiteRow ──────────────────────────────────────────────────────────────────

def test_field_site_may_have_paired_coordinates_with_their_source():
    site = SiteRow(site_id="ES-VALLADOLID", name="Valladolid", site_kind="field", country="ES",
                   latitude=41.65, longitude=-4.72, coordinate_source="registry")
    assert site.latitude == 41.65


@pytest.mark.parametrize("kwargs", [
    {"latitude": 41.0},                                                   # unpaired
    {"latitude": 41.0, "longitude": -4.0},                                # no coordinate_source
    {"latitude": 91.0, "longitude": 0.0, "coordinate_source": "x"},       # out of range
])
def test_site_coordinates_are_paired_ranged_and_sourced(kwargs):
    with pytest.raises(ValidationError):
        SiteRow(site_id="ES-VALLADOLID", name="Valladolid", site_kind="field", country="ES", **kwargs)


@pytest.mark.parametrize("kind", ["aggregate", "region"])
def test_aggregate_and_region_sites_never_carry_coordinates(kind):
    with pytest.raises(ValidationError, match="coordinates"):
        SiteRow(site_id="IT-MEDIA-NORD", name="Media Nord", site_kind=kind, country="IT",
                latitude=45.0, longitude=8.0, coordinate_source="x")
    assert SiteRow(site_id="IT-MEDIA-NORD", name="Media Nord", site_kind=kind, country="IT").latitude is None


def test_site_id_has_the_registry_shape():
    for bad in ("valladolid", "ES_VALLADOLID", "ES-", ""):
        with pytest.raises(ValidationError):
            SiteRow(site_id=bad, name="x", site_kind="field", country="ES")


# ── VarietyRow ───────────────────────────────────────────────────────────────

def test_variety_registry_id_starts_with_its_crop():
    ok = VarietyRow(crop_eppo="HORVX", name="MAYA", registry_id="HORVX:maya", status="assumption",
                    aliases=("96054-518",))
    assert ok.registry_id == "HORVX:maya"
    with pytest.raises(ValidationError, match="registry_id"):
        VarietyRow(crop_eppo="HORVX", name="MAYA", registry_id="TRZAX:maya", status="candidate")


def test_a_candidate_variety_has_no_aliases():
    with pytest.raises(ValidationError, match="candidate"):
        VarietyRow(crop_eppo="HORVX", name="MAYA", status="candidate", aliases=("x",))
