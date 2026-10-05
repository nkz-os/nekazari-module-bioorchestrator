"""Registry data and loader: schema validation of every file, uniqueness, lookups."""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from app.kg.registries import (
    DEFAULT_REGISTRIES_PATH,
    REGISTRY_FILES,
    VOCAB_KINDS,
    RegistryError,
    UnknownEntryError,
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


# ── units ────────────────────────────────────────────────────────────────────

def test_units_have_unique_codes_and_aliases(reg):
    assert len({u.code for u in reg.units}) == len(reg.units)
    for unit in reg.units:
        for name in (unit.code, *unit.aliases):
            assert reg.unit(name) is unit


@pytest.mark.parametrize(
    ("value", "src", "dst", "expected"),
    [
        (13.81, "t/ha", "kg/ha", 13810.0),
        (138.1, "q/ha", "kg/ha", 13810.0),
        (138.1, "dt/ha", "t/ha", 13.81),
        (12345.0, "kg/ha", "t/ha", 12.345),
        (7, "kg/ha", "kg/ha", 7.0),
    ],
)
def test_convert_is_exact_in_decimal(reg, value, src, dst, expected):
    assert reg.convert(value, src, dst) == expected


@pytest.mark.parametrize("value", [0.0, 1.0, 8.0, 123.45, 4267.0, 20680.0, 0.1, 1e-3])
@pytest.mark.parametrize(("a", "b"), [("kg/ha", "t/ha"), ("kg/ha", "dt/ha"), ("t/ha", "dt/ha"),
                                      ("kg/hL", "kg/hl")])
def test_unit_conversions_round_trip(reg, value, a, b):
    assert reg.convert(reg.convert(value, a, b), b, a) == pytest.approx(value, rel=1e-12, abs=1e-12)


def test_convert_rejects_incompatible_unknown_and_bad_values(reg):
    with pytest.raises(RegistryError, match="cannot convert"):
        reg.convert(1.0, "kg/ha", "cm")
    with pytest.raises(UnknownEntryError):
        reg.convert(1.0, "kg/ha", "furlong")
    for bad in (True, "3", float("nan"), float("inf"), None):
        with pytest.raises(ValueError, match="cannot convert"):
            reg.convert(bad, "kg/ha", "t/ha")


def test_unknown_unit_raises(reg):
    with pytest.raises(UnknownEntryError, match="unregistered unit"):
        reg.unit("bushel/acre")


def test_source_unit_strings_map_to_codes(reg):
    assert reg.source_unit("CREA", "q/ha").code == "dt/ha"
    assert reg.source_unit("GENVCE", "kg/ha").code == "kg/ha"
    assert reg.source_unit("GENVCE", "kg/hl").code == "kg/hL"
    assert reg.source_unit("GENVCE", "días").code == "d"
    assert reg.source_unit("CREA", "furlong") is None
    assert reg.source_unit("NOPE", "kg/ha") is None


def test_unit_factor_must_be_a_positive_decimal(copy_dir):
    dest, edit = copy_dir
    edit("units.yaml", lambda d: d["units"][1].update(factor="0"))
    with pytest.raises(RegistryError, match="schema validation failed"):
        load_registries(dest)
    edit("units.yaml", lambda d: d["units"][1].update(factor="abc"))
    with pytest.raises(RegistryError, match="schema validation failed"):
        load_registries(dest)


def test_source_unit_to_unknown_code_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("units.yaml", lambda d: d["source_units"]["CREA"].update({"q.li/ha": "nope"}))
    with pytest.raises(RegistryError, match="unknown unit"):
        load_registries(dest)


def test_duplicate_unit_code_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("units.yaml", lambda d: d["units"].append(dict(d["units"][0])))
    with pytest.raises(RegistryError, match="duplicate unit code"):
        load_registries(dest)


def test_unit_alias_shared_by_two_units_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("units.yaml", lambda d: d["units"][1].__setitem__("aliases", ["q/ha"]))  # dt/ha's alias
    with pytest.raises(RegistryError, match="unit alias"):
        load_registries(dest)


# ── vocabularies ─────────────────────────────────────────────────────────────

def test_vocabulary_kinds_are_exactly_the_planned_ones(reg):
    for kind in VOCAB_KINDS:
        assert reg.vocab_entries(kind)
    with pytest.raises(UnknownEntryError):
        reg.vocab_entries("nope")


def test_irrigation_literals_resolve_to_agrovoc_uris(reg):
    assert reg.vocab("irrigation", "secano") == "http://aims.fao.org/aos/agrovoc/c_6436"
    assert reg.vocab("irrigation", "Regadío") == "http://aims.fao.org/aos/agrovoc/c_3954"
    assert reg.vocab("irrigation", "irrigato") == "http://aims.fao.org/aos/agrovoc/c_3954"
    assert reg.vocab("irrigation", "http://aims.fao.org/aos/agrovoc/c_6436") == (
        "http://aims.fao.org/aos/agrovoc/c_6436")
    assert reg.vocab_entry("irrigation", "irrigated").id == "irrigated"


@pytest.mark.parametrize("value", [None, "", "  ", "drip", "mixed"])
def test_unrecognised_vocabulary_value_is_none(reg, value):
    assert reg.vocab("irrigation", value) is None


def test_vocabulary_ids_values_and_aliases_are_unique_per_kind(reg):
    for kind in VOCAB_KINDS:
        entries = reg.vocab_entries(kind)
        assert len({e.id for e in entries}) == len(entries)
        names = [" ".join(n.casefold().split()) for e in entries for n in {e.id, e.stored, *e.aliases}]
        assert len(names) == len(set(names)), kind


def test_irrigation_vocabulary_matches_the_evidence_policy(reg):
    from app.graph import evidence_policy as ep

    assert reg.vocab("irrigation", "rainfed") == ep.IRRIGATION_URIS[ep.REGIME_RAINFED]
    assert reg.vocab("irrigation", "irrigated") == ep.IRRIGATION_URIS[ep.REGIME_IRRIGATED]
    for entry in reg.vocab_entries("irrigation"):
        regime = ep.irrigation_regime(entry.stored)
        assert regime is not None
        for alias in (entry.id, *entry.aliases):
            # every literal the registry accepts is classified the same way by the policy,
            # or not at all (the policy only knows secano/regadio spellings)
            assert ep.irrigation_regime(alias) in (None, regime)


def test_yield_metric_purposes_match_the_evidence_policy(reg):
    from app.graph import evidence_policy as ep

    for entry in reg.vocab_entries("yield_metric"):
        derived = ep.yield_purpose(entry.id, None)
        assert derived == (entry.purpose or ep.PURPOSE_UNKNOWN), entry.id


def test_yield_basis_ids_are_read_by_the_evidence_policy(reg):
    from app.graph import evidence_policy as ep

    ids = {e.id for e in reg.vocab_entries("yield_basis")}
    assert {ep.BASIS_DRY_MATTER, ep.BASIS_FRESH_MATTER, ep.BASIS_UNKNOWN} <= ids
    assert ep.forage_basis({"yieldBasis": "dry_matter"}) == "dry_matter"
    assert ep.forage_basis({"yieldBasis": "fresh_matter"}) == "fresh_matter"


def test_crop_main_product_and_purposes_are_registered_vocabulary(reg):
    metrics = {e.id for e in reg.vocab_entries("yield_metric")}
    purposes = {e.id for e in reg.vocab_entries("purpose")}
    for crop in reg.crops:
        assert crop.main_product in metrics
        assert set(crop.purposes) <= purposes


def test_missing_or_extra_vocabulary_kind_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("vocabularies.yaml", lambda d: d["vocabularies"].pop("study_type"))
    with pytest.raises(RegistryError, match="kinds must be exactly"):
        load_registries(dest)


def test_vocabulary_alias_collision_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("vocabularies.yaml", lambda d: d["vocabularies"]["irrigation"][1]["aliases"].append("secano"))
    with pytest.raises(RegistryError, match="irrigation vocabulary alias"):
        load_registries(dest)


def test_crop_with_unregistered_main_product_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("crops.yaml", lambda d: d["crops"][0].update(main_product="tuber"))
    with pytest.raises(RegistryError, match="not a yield_metric"):
        load_registries(dest)


# ── variables ────────────────────────────────────────────────────────────────

def test_variable_ids_are_unique_stable_snake_case(reg):
    ids = [v.id for v in reg.variables]
    assert len(ids) == len(set(ids)) >= 40
    for variable_id in ids:
        assert reg.variable(variable_id).id == variable_id


def test_every_variable_has_a_registered_unit_when_it_needs_one(reg):
    for variable in reg.variables:
        if variable.scale in ("ratio", "percent"):
            assert reg.unit(variable.unit)
        else:
            assert variable.unit is None
        if variable.scale == "ordinal" and variable.domain:
            assert variable.domain[0] < variable.domain[1]


def test_no_crop_ontology_id_is_unverified(reg):
    assert all(v.crop_ontology_id is None for v in reg.variables)


def test_unregistered_variable_raises(reg):
    with pytest.raises(UnknownEntryError, match="unregistered variable"):
        reg.variable("not_a_variable")


def test_yield_variables_are_denormalized_and_everything_else_is_not(reg):
    flagged = {v.id for v in reg.variables if v.denormalize}
    assert flagged == {"crop_yield", "relative_yield_pct"}
    assert reg.variable("crop_yield").unit == "kg/ha"


def test_raw_keys_resolve_to_one_variable(reg):
    assert reg.variable_for_raw_key("GENVCE", "proteina_pct").id == "grain_protein_content"
    assert reg.variable_for_raw_key("CREA", "umidita_raccolta_pct").id == "grain_moisture_harvest"
    assert reg.variable_for_raw_key("GENVCE", "oidio_pct").id == "powdery_mildew_pct"
    # scale not stated by the source: deliberately unresolved, never guessed
    for bare in ("oidio", "roya_parda", "helmintosporiosis", "rincosporiosis", "floracion_femenina_dias"):
        assert reg.variable_for_raw_key("GENVCE", bare) is None
    assert reg.variable_for_raw_key("NOPE", "proteina_pct") is None


def test_disease_variables_state_their_scale(reg):
    for variable in reg.variables:
        if variable.id.endswith("_score_0_9"):
            assert variable.scale == "ordinal"
            assert variable.domain == (0, 9)
        if variable.id.endswith("_pct"):
            assert variable.scale == "percent"


def test_raw_key_claimed_by_two_variables_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("variables.yaml", lambda d: d["variables"][2]["raw_keys"]["GENVCE"].append("proteina_pct"))
    with pytest.raises(RegistryError, match="maps to both"):
        load_registries(dest)


def test_ratio_without_unit_and_unregistered_unit_are_errors(copy_dir):
    dest, edit = copy_dir
    edit("variables.yaml", lambda d: d["variables"][2].update(unit=None))
    with pytest.raises(RegistryError, match="needs a unit"):
        load_registries(dest)
    edit("variables.yaml", lambda d: d["variables"][2].update(unit="bushel"))
    with pytest.raises(RegistryError, match="unregistered unit"):
        load_registries(dest)


def test_ordinal_domain_and_ontology_id_format_are_enforced(copy_dir):
    dest, edit = copy_dir

    def bad_domain(d):
        ordinal = next(v for v in d["variables"] if v["scale"] == "ordinal" and v.get("domain"))
        ordinal["domain"] = [5, 5]

    edit("variables.yaml", bad_domain)
    with pytest.raises(RegistryError, match="min < max"):
        load_registries(dest)


def test_malformed_crop_ontology_id_is_rejected(copy_dir):
    dest, edit = copy_dir
    edit("variables.yaml", lambda d: d["variables"][0].update(crop_ontology_id="CO_321:abc"))
    with pytest.raises(RegistryError, match="schema validation failed"):
        load_registries(dest)


def test_duplicate_variable_id_is_an_error(copy_dir):
    dest, edit = copy_dir

    def dup(d):
        d["variables"].append({**d["variables"][3], "raw_keys": {}})

    edit("variables.yaml", dup)
    with pytest.raises(RegistryError, match="duplicate variable id"):
        load_registries(dest)
