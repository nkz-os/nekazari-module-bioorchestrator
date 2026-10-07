"""Deterministic identity: keys depend only on observed values, in canonical form.

The five tests the plan pins (same input same keys; a normalisation change leaves unit_key alone;
rootstock or planting density change it; 8.0 equals 8; no collisions over random rows) come first,
then the rules around them: what is and is not part of each key.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import random
import re
import shutil
import unicodedata
from fractions import Fraction
from pathlib import Path

import pytest
import yaml

from app.kg.identity import (
    KEY_VERSION,
    canonical_json,
    document_key,
    key_payload,
    obs_key,
    site_key,
    study_key,
    unit_key,
    variety_key,
)
from app.kg.model import (
    OBSERVATION_KEY_FIELDS,
    OBSERVATION_NON_KEY_FIELDS,
    UNIT_KEY_FIELDS,
    UNIT_NON_KEY_FIELDS,
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

HEX64 = re.compile(r"^[0-9a-f]{64}$")
DOC_KEY = "d" * 64
UNIT_KEY = "a" * 64


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def make_unit(**over) -> UnitRow:
    base = {
        "source_id": "GENVCE",
        "document_key": DOC_KEY,
        "crop_eppo": "HORVX",
        "raw_variety": "MAYA",
        "raw_site": "Valladolid",
        "raw_season": "2021",
        "raw_irrigation": "secano",
    }
    base.update(over)
    return UnitRow(**base)


def make_obs(**over) -> ObservationRow:
    base = {"unit_key": UNIT_KEY, "variable_id": "crop_yield", "value": 6200.0}
    base.update(over)
    return ObservationRow(**base)


# ═════════════════════════════════════════════════════════════════════════════
# 1. Same raw input, same keys
# ═════════════════════════════════════════════════════════════════════════════

def test_same_raw_input_gives_the_same_keys():
    doc_args = {"source_id": "CREA", "title": "Prove varietali mais 2023", "issue": "3", "year": 2023}
    study_args = {"source_id": "GENVCE", "study_type": "variety", "crop_eppo": "HORVX",
                  "raw_season": "2021", "raw_scope": "secano", "name": "GENVCE · cebada · 2021"}
    variety_args = {"crop_eppo": "HORVX", "name": "MAYA", "status": "assumption"}
    levels = (FactorLevel(factor="N_dose", level=120, unit="kg/ha"), FactorLevel(factor="tillage", level="no-till"))

    for _ in range(2):  # rebuilt from scratch each time
        assert document_key(DocumentRow(**doc_args)) == document_key(DocumentRow(**doc_args))
        assert study_key(StudyRow(**study_args)) == study_key(StudyRow(**study_args))
        assert variety_key(VarietyRow(**variety_args)) == variety_key(VarietyRow(**variety_args))
        assert unit_key(make_unit(factor_levels=levels)) == unit_key(make_unit(factor_levels=levels))
        assert obs_key(make_obs(stage="heading")) == obs_key(make_obs(stage="heading"))

    keys = [
        document_key(DocumentRow(**doc_args)),
        study_key(StudyRow(**study_args)),
        variety_key(VarietyRow(**variety_args)),
        unit_key(make_unit(factor_levels=levels)),
        obs_key(make_obs(stage="heading")),
    ]
    assert all(HEX64.match(k) for k in keys)
    assert len(set(keys)) == len(keys)  # the kind is part of the hashed text


def test_the_key_is_the_sha256_of_a_literal_canonical_text():
    """Pins the serialisation itself: sorted keys, no spaces, numbers tagged, NFC, version 1."""
    unit = make_unit(
        factor_levels=(FactorLevel(factor="N_dose", level=120.0, unit="kg/ha"),),
        planting_year=2019,
    )
    expected = (
        '{"clone":null,"crop_eppo":"HORVX","document_key":"' + DOC_KEY + '",'
        '"factor_levels":[["N_dose",{"n":"120"},"kg/ha"]],"kind":"unit","planting_year":{"n":"2019"},'
        '"raw_irrigation":"secano","raw_production_system":null,"raw_season":"2021",'
        '"raw_site":"Valladolid","raw_variety":"MAYA","rootstock":null,"row_discriminator":null,'
        '"source_id":"GENVCE","v":1}'
    )
    assert KEY_VERSION == 1
    assert key_payload(unit) == expected
    assert unit_key(unit) == sha(expected)
    # one literal digest, computed outside the implementation, so a change of format cannot hide
    assert unit_key(unit) == "cabc6a951bc4508de6f22f48e79aa0aee49042aeded92205ee2bab76e8f4e188"


def test_every_hashed_row_has_a_literal_canonical_text():
    obs = make_obs(stage="heading", date="2021-05-03", qualifier="vs MAYA")
    assert key_payload(obs) == (
        '{"date":"2021-05-03","kind":"observation","qualifier":"vs MAYA","stage":"heading",'
        '"unit_key":"' + UNIT_KEY + '","v":1,"variable_id":"crop_yield"}'
    )
    doc = DocumentRow(source_id="CREA", title="Prove varietali mais 2023", issue="3", year=2023)
    assert key_payload(doc) == (
        '{"issue":"3","kind":"document","source_id":"CREA","title":"Prove varietali mais 2023",'
        '"v":1,"year":{"n":"2023"}}'
    )
    study = StudyRow(source_id="GENVCE", study_type="variety", crop_eppo="HORVX", raw_season="2021",
                     name="x")
    assert key_payload(study) == (
        '{"crop_eppo":"HORVX","kind":"study","raw_scope":null,"raw_season":"2021","source_id":"GENVCE",'
        '"study_type":"variety","v":1}'
    )
    variety = VarietyRow(crop_eppo="HORVX", name="Maya  Dos", status="candidate")
    assert key_payload(variety) == '{"crop_eppo":"HORVX","kind":"variety","name":"maya dos","v":1}'


def test_site_key_is_the_canonical_registry_id():
    site = SiteRow(site_id="ES-VALLADOLID", name="Valladolid", site_kind="field", country="ES")
    assert site_key(site) == "ES-VALLADOLID"
    # name, kind, coordinates and the rest never matter: the registry id is the identity
    other = SiteRow(site_id="ES-VALLADOLID", name="Otro nombre", site_kind="aggregate", country="ES")
    assert site_key(other) == "ES-VALLADOLID"


# ═════════════════════════════════════════════════════════════════════════════
# 2. A normalisation-table change does not change unit_key
# ═════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def registries_pair(tmp_path: Path):
    """The shipped registries (A) and a copy whose normalisation tables were edited (B)."""
    dest = tmp_path / "registries"
    shutil.copytree(DEFAULT_REGISTRIES_PATH, dest)

    def edit(name: str, fn) -> None:
        file = dest / name
        data = yaml.safe_load(file.read_text(encoding="utf-8"))
        fn(data)
        file.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")

    def drop_alias(entries, entry_id: str, alias: str) -> None:
        for entry in entries:
            if entry["id"] == entry_id:
                entry["aliases"] = [a for a in entry["aliases"] if a != alias]
                return
        raise AssertionError(entry_id)

    def edit_vocab(data) -> None:
        drop_alias(data["vocabularies"]["irrigation"], "rainfed", "secano")
        drop_alias(data["vocabularies"]["production_system"], "organic", "ecológico")

    def edit_varieties(data) -> None:
        maya = next(v for v in data["varieties"] if v["id"] == "HORVX:maya")
        assert "96054-518" in maya["aliases"]
        maya["aliases"] = []  # the alias group is dissolved: "96054-518" is now unregistered

    def edit_sites(data) -> None:
        lleida = next(s for s in data["sites"] if s["id"] == "ES-LLEIDA")
        lleida["aliases"] = [*lleida.get("aliases", []), "Ponent"]

    edit("vocabularies.yaml", edit_vocab)
    edit("varieties.yaml", edit_varieties)
    edit("sites.yaml", edit_sites)
    return load_registries(DEFAULT_REGISTRIES_PATH), load_registries(dest)


def _build_unit(reg, document: str) -> UnitRow:
    """What the contract engine does: observed fields as printed, derived fields via the registries."""
    raw = {
        "raw_crop": "Cebada de ciclo largo", "raw_variety": "96054-518", "raw_site": "Ponent",
        "raw_irrigation": "secano", "raw_production_system": "ecológico", "raw_season": "2021",
    }
    crop = reg.crop(raw["raw_crop"])
    variety = reg.variety(crop.eppo, raw["raw_variety"])
    site = reg.site(raw["raw_site"])
    return UnitRow(
        source_id="GENVCE",
        document_key=document,
        crop_eppo=crop.eppo,
        raw_variety=raw["raw_variety"],
        raw_site=raw["raw_site"],
        raw_season=raw["raw_season"],
        raw_irrigation=raw["raw_irrigation"],
        raw_production_system=raw["raw_production_system"],
        irrigation_regime=reg.vocab("irrigation", raw["raw_irrigation"]),
        production_system=reg.vocab("production_system", raw["raw_production_system"]),
        site_key=site.id if site else None,
        variety_key=variety_key(VarietyRow(crop_eppo=crop.eppo, name=variety.name, status=variety.status))
        if variety else None,
    )


def test_changing_a_normalisation_table_does_not_change_unit_key(registries_pair):
    reg_a, reg_b = registries_pair
    assert reg_a.registries_hash != reg_b.registries_hash  # the registries really did change

    unit_a = _build_unit(reg_a, DOC_KEY)
    unit_b = _build_unit(reg_b, DOC_KEY)

    # the derived fields moved with the tables ...
    assert unit_a.irrigation_regime != unit_b.irrigation_regime
    assert unit_a.production_system != unit_b.production_system
    assert unit_a.site_key != unit_b.site_key
    assert unit_a.variety_key != unit_b.variety_key
    assert unit_a != unit_b
    # ... and the identity did not
    assert unit_key(unit_a) == unit_key(unit_b)


# ═════════════════════════════════════════════════════════════════════════════
# 3. Rootstock and planting density (and every other observed field) change the key
# ═════════════════════════════════════════════════════════════════════════════

def test_rows_differing_only_in_rootstock_get_different_keys():
    a = make_unit(crop_eppo="PRNDU", rootstock="GF-677")
    b = make_unit(crop_eppo="PRNDU", rootstock="Garnem")
    c = make_unit(crop_eppo="PRNDU")
    assert len({unit_key(a), unit_key(b), unit_key(c)}) == 3


def test_rows_differing_only_in_planting_density_get_different_keys():
    def density(value):
        return make_unit(factor_levels=(FactorLevel(factor="planting_density", level=value, unit="plants/m2"),))

    assert unit_key(density(250)) != unit_key(density(300))
    assert unit_key(density(250)) != unit_key(make_unit())


KEY_FIELD_VARIANTS = {
    "source_id": {"source_id": "CREA"},
    "document_key": {"document_key": "e" * 64},
    "crop_eppo": {"crop_eppo": "ZEAMX"},
    "raw_variety": {"raw_variety": "OTHER"},
    "raw_site": {"raw_site": "Lleida"},
    "raw_season": {"raw_season": "2022"},
    "raw_irrigation": {"raw_irrigation": "regadío"},
    "raw_production_system": {"raw_production_system": "ecológico"},
    "factor_levels": {"factor_levels": (FactorLevel(factor="N_dose", level=120),)},
    "rootstock": {"rootstock": "GF-677"},
    "clone": {"clone": "A1"},
    "planting_year": {"planting_year": 2015},
    "row_discriminator": {"row_discriminator": "rep 2"},
}

YIELD = {
    "yield_kg_ha": 6200.0, "yield_metric": "grain", "yield_basis": "standard_moisture",
    "yield_moisture_pct": 13.0, "yield_value_original": 6200.0, "yield_unit_original": "kg/ha",
    "purpose": "grain",
}

# a derivation exists only where the source states no regime: the base unit has none either
DERIVED = {"raw_irrigation": None, "irrigation_derivation": "yield_threshold_v1",
           "irrigation_yield_low_kg_ha": 4000.0, "irrigation_yield_high_kg_ha": 7000.0}

NON_KEY_FIELD_VARIANTS = {
    "study_key": {"study_key": "c" * 64},
    "site_key": {"site_key": "ES-LLEIDA"},
    "variety_key": {"variety_key": "b" * 64},
    "year": {"year": 2021},
    "irrigation_regime": {"irrigation_regime": "http://aims.fao.org/aos/agrovoc/c_6436"},
    "production_system": {"production_system": "organic"},
    "purpose": {"purpose": "forage"},
    "yield_kg_ha": {"yield_kg_ha": 5000.0},
    "yield_metric": {"yield_metric": "seed"},
    "yield_basis": {"yield_basis": "dry_matter", "yield_moisture_pct": None},
    "yield_moisture_pct": {"yield_moisture_pct": 14.0},
    "yield_value_original": {"yield_value_original": 62.0},
    "yield_unit_original": {"yield_unit_original": "q/ha"},
    "derivation_method": {"derivation_method": "mean of replicates"},
    "locator": {"locator": "table 3"},
    "productivity_class": {"productivity_class": "yield_stratum_high"},
    # the three travel together: a derivation records the cutoffs it used (the unit has a yield here)
    "irrigation_derivation": DERIVED,
    "irrigation_yield_low_kg_ha": DERIVED,
    "irrigation_yield_high_kg_ha": DERIVED,
    "gaps": {"gaps": (Gap(field="clone", reason="not stated"),)},
}


def test_every_unit_field_is_classified_in_these_tests_too():
    assert set(KEY_FIELD_VARIANTS) == set(UNIT_KEY_FIELDS)
    assert set(NON_KEY_FIELD_VARIANTS) == set(UNIT_NON_KEY_FIELDS)


@pytest.mark.parametrize("field", UNIT_KEY_FIELDS)
def test_each_observed_unit_field_changes_the_key(field):
    base = make_unit(**YIELD)
    assert unit_key(make_unit(**YIELD, **KEY_FIELD_VARIANTS[field])) != unit_key(base)


@pytest.mark.parametrize("field", UNIT_NON_KEY_FIELDS)
def test_no_derived_or_provenance_field_changes_the_key(field):
    variant = NON_KEY_FIELD_VARIANTS[field]
    base = make_unit(**{**YIELD, **({"raw_irrigation": None} if "raw_irrigation" in variant else {})})
    changed = make_unit(**{**YIELD, **variant})
    assert changed != base  # the variant is a real change
    assert unit_key(changed) == unit_key(base)


def test_a_unit_is_not_keyed_by_its_values():
    """The yield (or any value) is an Observation; re-reading a changed value updates in place."""
    assert unit_key(make_unit(**YIELD)) == unit_key(make_unit())


# ═════════════════════════════════════════════════════════════════════════════
# 4. 8.0 and 8 are the same key
# ═════════════════════════════════════════════════════════════════════════════

def _with_level(level) -> UnitRow:
    return make_unit(factor_levels=(FactorLevel(factor="N_dose", level=level),))


def test_float_formatting_variants_give_the_same_key():
    assert unit_key(_with_level(8)) == unit_key(_with_level(8.0))
    assert unit_key(_with_level(0)) == unit_key(_with_level(-0.0)) == unit_key(_with_level(0.0))
    assert unit_key(_with_level(10**22)) == unit_key(_with_level(1e22))
    assert unit_key(_with_level(0.5)) == unit_key(_with_level(0.50)) == unit_key(_with_level(5e-1))
    assert canonical_json({"x": 8}) == canonical_json({"x": 8.0}) == '{"x":{"n":"8"}}'


def test_different_numbers_are_different_keys():
    levels = [8, 8.5, 0.1, 0.3, 0.30000000000000004, 1e22, 1e22 + 4e6, -8, 80]
    keys = {unit_key(_with_level(x)) for x in levels}
    assert len(keys) == len(levels)


def test_text_is_never_parsed_as_a_number():
    assert unit_key(_with_level("8")) != unit_key(_with_level(8))
    assert unit_key(_with_level("8.0")) != unit_key(_with_level("8"))


def test_non_finite_numbers_and_foreign_types_are_refused():
    for bad in (float("nan"), float("inf"), -float("inf")):
        with pytest.raises(ValueError, match="finite"):
            canonical_json({"x": bad})
    for bad in ({1}, b"x", object()):
        with pytest.raises(TypeError):
            canonical_json({"x": bad})


# ═════════════════════════════════════════════════════════════════════════════
# 5. Property test: no collisions where the observed fields differ
# ═════════════════════════════════════════════════════════════════════════════
# hypothesis is not installed in the project venv (and nothing is installed for a test), so this is
# a seeded generator with an independent oracle. Pools are tuned to produce near-collisions: blanks
# against None, composed against decomposed accents, padding, separator-like characters, text that
# looks like numbers, integral floats against ints.

TEXT_POOL = [
    None, "", " ", "\t", "a", "A", "a ", " a", "ab", "b", "a|b", "a,b", '"a"', "a\\b", "null", "None",
    "8", "8.0", "é", "e\u0301", "É", "x y", "x  y", "}{", "[]", "\u00a0a\u00a0", "ß", "ss",
]
NUMBER_POOL = [8, 8.0, 8.5, 0, -0.0, 0.0, 1, 0.1, 0.3, 0.30000000000000004, 1e22, 10**22, -3, 120, 120.0]
YEAR_POOL = [None, 1999, 2015, 2015, 2021, 0]
FACTORS = ("N_dose", "tillage", "planting_density")


def _oracle_text(value):
    """Written independently of identity.py: NFC, strip, blank is missing."""
    if value is None:
        return None
    text = unicodedata.normalize("NFC", value).strip()
    return ("s", text) if text else None


def _oracle_number(value):
    return ("n", Fraction(value))  # exact: 8 == 8.0, 0.1 != 0.30000000000000004


def _oracle(unit: UnitRow):
    levels = tuple(sorted(
        ((_oracle_text(lv.factor), _oracle_number(lv.level) if not isinstance(lv.level, str)
          else _oracle_text(lv.level), _oracle_text(lv.unit)) for lv in unit.factor_levels),
        key=repr,
    ))
    return (
        unit.source_id, unit.document_key, unit.crop_eppo,
        _oracle_text(unit.raw_variety), _oracle_text(unit.raw_site), _oracle_text(unit.raw_season),
        _oracle_text(unit.raw_irrigation), _oracle_text(unit.raw_production_system),
        levels, _oracle_text(unit.rootstock), _oracle_text(unit.clone), unit.planting_year,
        _oracle_text(unit.row_discriminator),
    )


def _random_text(rng: random.Random):
    if rng.random() < 0.15:  # fresh text, so the space of rows is not only the pool
        return "".join(rng.choice("abcé |,\"\\0123") for _ in range(rng.randint(1, 5)))
    return rng.choice(TEXT_POOL)


def _random_unit(rng: random.Random) -> UnitRow:
    levels = []
    for factor in rng.sample(FACTORS, rng.randint(0, 2)):
        if rng.random() < 0.5:
            level = rng.choice(NUMBER_POOL)
        else:
            level = rng.choice(["8", "8.0", "no-till", "é", "e\u0301", " 8 "])
        levels.append(FactorLevel(factor=rng.choice([factor, f" {factor} "]), level=level,
                                  unit=rng.choice([None, "kg/ha", "plants/m2"])))
    return UnitRow(
        source_id=rng.choice(["GENVCE", "CREA"]),
        document_key=rng.choice(["d" * 64, "e" * 64]),
        crop_eppo=rng.choice(["HORVX", "ZEAMX"]),
        raw_variety=_random_text(rng), raw_site=_random_text(rng), raw_season=_random_text(rng),
        raw_irrigation=_random_text(rng), raw_production_system=_random_text(rng),
        factor_levels=tuple(levels), rootstock=_random_text(rng), clone=_random_text(rng),
        planting_year=rng.choice(YEAR_POOL), row_discriminator=_random_text(rng),
    )


def _respell_text(value, rng: random.Random):
    """Another spelling of the same observed text: padding, decomposed accents, blank for missing."""
    if value is None or not value.strip():
        return rng.choice([None, "", " ", "\t"])
    text = rng.choice([value, unicodedata.normalize("NFD", value)])
    return rng.choice(["", " ", "\t", "\u00a0"]) + text + rng.choice(["", " ", "\n"])


def _respell_number(level, rng: random.Random):
    if isinstance(level, int) and abs(level) < 2**53:
        return rng.choice([level, float(level)])
    if isinstance(level, float) and level.is_integer():
        return rng.choice([level, int(level)])
    return level


def _respell(unit: UnitRow, rng: random.Random) -> UnitRow:
    """The same observed row written differently. ``model_copy`` skips validation on purpose, so the
    identity code sees these spellings itself instead of the model's blank-to-None clean-up."""
    levels = [
        level.model_copy(update={
            "factor": _respell_text(level.factor, rng),
            "level": _respell_text(level.level, rng) if isinstance(level.level, str)
            else _respell_number(level.level, rng),
            "unit": _respell_text(level.unit, rng),
        })
        for level in unit.factor_levels
    ]
    rng.shuffle(levels)
    text_fields = ("raw_variety", "raw_site", "raw_season", "raw_irrigation", "raw_production_system",
                   "rootstock", "clone", "row_discriminator")
    update = {name: _respell_text(getattr(unit, name), rng) for name in text_fields}
    return unit.model_copy(update={**update, "factor_levels": tuple(levels)})


def _mutate(unit: UnitRow, rng: random.Random) -> UnitRow:
    """Take one observed field from another random row (it may well be equal: the oracle decides)."""
    name = rng.choice(UNIT_KEY_FIELDS)
    return unit.model_copy(update={name: getattr(_random_unit(rng), name)})


def test_random_rows_collide_only_when_their_observed_fields_are_equal():
    rng = random.Random(20261006)
    bases = [_random_unit(rng) for _ in range(400)]
    key_to_oracle: dict[str, object] = {}
    oracle_to_key: dict[object, str] = {}
    draws, respelled = 6000, 0
    for _ in range(draws):
        base = rng.choice(bases)
        roll = rng.random()
        if roll < 0.45:
            unit = _respell(base, rng)
        elif roll < 0.85:
            unit = _mutate(_respell(base, rng), rng)
        else:
            unit = base
        key, oracle = unit_key(unit), _oracle(unit)
        # no collision: one key never stands for two different observed rows ...
        assert key_to_oracle.setdefault(key, oracle) == oracle
        # ... and no instability: one observed row never gets two keys, however it is spelled
        assert oracle_to_key.setdefault(oracle, key) == key
        respelled += unit != base and oracle == _oracle(base)
    # the run exercised both sides: many distinct rows, and many equal rows written differently
    assert len(oracle_to_key) > 1500
    assert respelled > 800
    assert len(key_to_oracle) == len(oracle_to_key)


# ═════════════════════════════════════════════════════════════════════════════
# Canonical form of text, and the other keys
# ═════════════════════════════════════════════════════════════════════════════

def test_text_is_nfc_and_trimmed_before_hashing():
    assert unit_key(make_unit(raw_variety="e\u0301")) == unit_key(make_unit(raw_variety="é"))
    assert unit_key(make_unit(raw_variety="  MAYA\t")) == unit_key(make_unit(raw_variety="MAYA"))
    assert unit_key(make_unit(raw_variety="\u00a0MAYA\u00a0")) == unit_key(make_unit(raw_variety="MAYA"))


def test_blank_and_missing_are_the_same_key():
    assert (unit_key(make_unit(raw_irrigation=None)) == unit_key(make_unit(raw_irrigation=""))
            == unit_key(make_unit(raw_irrigation="   ")))
    assert unit_key(make_unit(raw_irrigation=None)) != unit_key(make_unit(raw_irrigation="None"))


def test_case_and_inner_spacing_are_observed_not_normalised():
    """Raw means raw: only registries fold case or collapse spaces, and the key never uses them."""
    assert unit_key(make_unit(raw_variety="MAYA")) != unit_key(make_unit(raw_variety="maya"))
    assert unit_key(make_unit(raw_variety="MAYA 2")) != unit_key(make_unit(raw_variety="MAYA  2"))


def test_field_boundaries_cannot_be_shifted():
    a = make_unit(raw_variety="a", raw_site="bc")
    b = make_unit(raw_variety="ab", raw_site="c")
    c = make_unit(raw_variety="a|bc", raw_site=None)
    assert len({unit_key(a), unit_key(b), unit_key(c)}) == 3


def test_a_value_moving_between_fields_changes_the_key():
    assert unit_key(make_unit(rootstock="X", clone=None)) != unit_key(make_unit(rootstock=None, clone="X"))
    assert unit_key(make_unit(raw_site="X", raw_season=None)) != unit_key(make_unit(raw_site=None, raw_season="X"))


def test_factor_level_order_does_not_matter_but_content_does():
    n = FactorLevel(factor="N_dose", level=120, unit="kg/ha")
    t = FactorLevel(factor="tillage", level="no-till")
    assert unit_key(make_unit(factor_levels=(n, t))) == unit_key(make_unit(factor_levels=(t, n)))
    assert unit_key(make_unit(factor_levels=(n, t))) != unit_key(make_unit(factor_levels=(n,)))
    other_unit = FactorLevel(factor="N_dose", level=120, unit="lb/ac")
    assert unit_key(make_unit(factor_levels=(n,))) != unit_key(make_unit(factor_levels=(other_unit,)))


OBS_KEY_VARIANTS = {
    "unit_key": {"unit_key": "b" * 64},
    "variable_id": {"variable_id": "thousand_grain_weight"},
    "stage": {"stage": "heading"},
    "date": {"date": dt.date(2021, 5, 3)},
    "qualifier": {"qualifier": "days vs MAYA"},
}

OBS_NON_KEY_VARIANTS = {
    "value": {"value": 5000.0},
    "value_text": {"value": None, "value_text": "resistant"},
    "unit": {"unit": "t/ha"},
    "basis": {"basis": "dry_matter"},
    "moisture_pct": {"basis": "standard_moisture", "moisture_pct": 14.0},
    "metric": {"metric": "grain", "purpose": "grain", "basis": "dry_matter", "unit": "kg/ha",
               "value_original": 1.0, "unit_original": "kg/ha"},
    "purpose": {"metric": "grain", "purpose": "forage", "basis": "dry_matter", "unit": "kg/ha",
                "value_original": 1.0, "unit_original": "kg/ha"},
    "value_original": {"value_original": 62.0},
    "unit_original": {"unit_original": "q/ha"},
    "derivation_method": {"derivation_method": "mean"},
    "raw_key": {"raw_key": "group.key"},
    "locator": {"locator": "p. 4"},
    "gaps": {"gaps": (Gap(field="stage", reason="not stated"),)},
}


def test_every_observation_field_is_classified_in_these_tests_too():
    assert set(OBS_KEY_VARIANTS) == set(OBSERVATION_KEY_FIELDS)
    assert set(OBS_NON_KEY_VARIANTS) == set(OBSERVATION_NON_KEY_FIELDS)


@pytest.mark.parametrize("field", OBSERVATION_KEY_FIELDS)
def test_each_observation_key_field_changes_the_key(field):
    assert obs_key(make_obs(**OBS_KEY_VARIANTS[field])) != obs_key(make_obs())


@pytest.mark.parametrize("field", OBSERVATION_NON_KEY_FIELDS)
def test_an_observations_value_and_context_never_change_its_key(field):
    base = make_obs()
    changed = make_obs(**OBS_NON_KEY_VARIANTS[field])
    assert changed != base
    assert obs_key(changed) == obs_key(base)


def test_obs_key_derives_from_the_unit_key():
    """A unit that gets a new key (a new observed field) takes its observations with it."""
    a, b = make_unit(), make_unit(rootstock="GF-677")
    assert obs_key(make_obs(unit_key=unit_key(a))) != obs_key(make_obs(unit_key=unit_key(b)))


DOC_BASE = {"source_id": "CREA", "title": "Prove varietali mais 2023", "issue": "3", "year": 2023}


@pytest.mark.parametrize("change", [
    {"source_id": "GENVCE"}, {"title": "Prove varietali mais 2024"}, {"issue": "4"}, {"issue": None},
    {"year": 2024}, {"year": None},
])
def test_document_key_follows_source_title_issue_and_year(change):
    assert document_key(DocumentRow(**{**DOC_BASE, **change})) != document_key(DocumentRow(**DOC_BASE))


@pytest.mark.parametrize("change", [
    {"url": "https://example.org/a.pdf"}, {"file_name": "a.pdf"}, {"raw_sha256": "f" * 64},
])
def test_where_a_document_was_read_from_is_not_its_identity(change):
    assert document_key(DocumentRow(**{**DOC_BASE, **change})) == document_key(DocumentRow(**DOC_BASE))


STUDY_BASE = {"source_id": "GENVCE", "study_type": "variety", "crop_eppo": "HORVX", "raw_season": "2021",
              "raw_scope": "secano", "name": "GENVCE · cebada · 2021 · secano"}


@pytest.mark.parametrize("change", [
    {"source_id": "CREA"}, {"study_type": "management"}, {"crop_eppo": "ZEAMX"}, {"crop_eppo": None},
    {"raw_season": "2022"}, {"raw_scope": "regadío"}, {"raw_scope": None},
])
def test_study_key_follows_source_type_crop_season_and_scope(change):
    assert study_key(StudyRow(**{**STUDY_BASE, **change})) != study_key(StudyRow(**STUDY_BASE))


@pytest.mark.parametrize("change", [{"name": "renamed"}, {"year": 2021}, {"design": "RCBD"}])
def test_study_label_year_and_design_are_not_its_identity(change):
    assert study_key(StudyRow(**{**STUDY_BASE, **change})) == study_key(StudyRow(**STUDY_BASE))


def test_variety_key_is_crop_plus_normalised_name():
    base = variety_key(VarietyRow(crop_eppo="HORVX", name="MAYA", status="candidate"))
    # normalised: case, inner spacing, NFC and padding fold together ...
    for same in ("maya", "  Maya ", "MAYA"):
        assert variety_key(VarietyRow(crop_eppo="HORVX", name=same, status="candidate")) == base
    assert (variety_key(VarietyRow(crop_eppo="HORVX", name="e\u0301", status="candidate"))
            == variety_key(VarietyRow(crop_eppo="HORVX", name="É", status="candidate")))
    assert (variety_key(VarietyRow(crop_eppo="HORVX", name="A  B", status="candidate"))
            == variety_key(VarietyRow(crop_eppo="HORVX", name="a b", status="candidate")))
    # ... but the crop and the name itself are the identity
    assert variety_key(VarietyRow(crop_eppo="TRZAX", name="MAYA", status="candidate")) != base
    assert variety_key(VarietyRow(crop_eppo="HORVX", name="MAYA 2", status="candidate")) != base
    # status, registry id, aliases and sources are not
    registered = VarietyRow(crop_eppo="HORVX", name="MAYA", registry_id="HORVX:maya", status="assumption",
                            aliases=("96054-518",), source_ids=("GENVCE",))
    assert variety_key(registered) == base


def test_registry_loaded_varieties_have_unique_keys():
    """Every registered variety (793 today) gets its own key: no two groups fold together."""
    reg = load_registries(DEFAULT_REGISTRIES_PATH)
    keys = {
        variety_key(VarietyRow(crop_eppo=v.crop, name=v.name, status=v.status)) for v in reg.varieties
    }
    assert len(keys) == len(reg.varieties)


def test_key_payload_is_valid_json_and_carries_kind_and_version():
    for row, kind in [
        (make_unit(), "unit"), (make_obs(), "observation"),
        (DocumentRow(**DOC_BASE), "document"), (StudyRow(**STUDY_BASE), "study"),
        (VarietyRow(crop_eppo="HORVX", name="MAYA", status="candidate"), "variety"),
    ]:
        payload = json.loads(key_payload(row))
        assert payload["kind"] == kind and payload["v"] == KEY_VERSION


def test_key_payload_refuses_rows_that_are_not_hashed():
    site = SiteRow(site_id="ES-VALLADOLID", name="Valladolid", site_kind="field", country="ES")
    with pytest.raises(TypeError):
        key_payload(site)
    with pytest.raises(TypeError):
        key_payload("not a row")
