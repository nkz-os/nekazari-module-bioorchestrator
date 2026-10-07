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


def test_the_zone_definitions_are_part_of_the_hash_and_are_validated(copy_dir):
    dest, _ = copy_dir
    assert "genvce_zone_definitions.yaml" in REGISTRY_FILES
    first = load_registries(dest).registries_hash
    path = dest / "genvce_zone_definitions.yaml"
    path.write_text(path.read_text(encoding="utf-8") + "\n# edited\n", encoding="utf-8")
    assert load_registries(dest).registries_hash != first
    path.write_text("version: 1\n", encoding="utf-8")
    with pytest.raises(RegistryError):
        load_registries(dest)


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


def test_ucum_spellings_are_pinned(reg):
    # hectare is "har" in UCUM; these strings are interchange identifiers, so a change is a decision
    assert {u.code: u.ucum for u in reg.units} == {
        "kg/ha": "kg/har", "t/ha": "t/har", "dt/ha": "dt/har", "%": "%", "kg/hL": "kg/hL",
        "g": "g", "cm": "cm", "d": "d", "/m2": "/m2", "1": "1",
    }


def test_convert_accepts_numpy_scalars(reg):
    import numpy as np

    assert reg.convert(np.float64(13.81), "t/ha", "kg/ha") == 13810.0
    assert reg.convert(np.float32(2.5), "t/ha", "kg/ha") == 2500.0
    assert reg.convert(np.int64(7), "t/ha", "kg/ha") == 7000.0
    assert reg.convert(np.float64(12345.0), "kg/ha", "t/ha") == 12.345
    assert isinstance(reg.convert(np.float64(1.0), "kg/ha", "t/ha"), float)
    with pytest.raises(ValueError, match="cannot convert"):
        reg.convert(np.float64("nan"), "kg/ha", "t/ha")
    with pytest.raises(ValueError, match="cannot convert"):
        reg.convert(np.bool_(True), "kg/ha", "t/ha")


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


def test_ordinal_domain_must_have_min_below_max(copy_dir):
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


# ── sources ──────────────────────────────────────────────────────────────────

def test_source_ids_match_the_current_sources_registry(reg):
    import json

    legacy = json.loads((DEFAULT_REGISTRIES_PATH.parents[1] / "data" / "sources_registry.json")
                        .read_text(encoding="utf-8"))
    assert {s.source_id for s in reg.sources} == {e["source_id"] for e in legacy}
    for entry in legacy:
        source = reg.source(entry["source_id"])
        assert source.name == entry["name"]
        assert source.institution == entry["institution"]
        assert source.country == entry["country"]
        assert source.url == entry.get("url")
        # the attribution fields must agree with the attribution patch once it is in the legacy file
        licence = source.licence
        if "attribution_text" in entry:
            assert licence.attribution_text == entry["attribution_text"]
            assert licence.attribution_url == entry["attribution_url"]
            assert licence.licence_id == entry["licence_id"]
            assert licence.processing_note == entry["processing_note"]
        if "download_date" in entry:
            assert licence.download_date.isoformat() == entry["download_date"]
        if "source_documents" in entry:
            assert [d.model_dump() for d in licence.source_documents] == entry["source_documents"]


def test_every_source_has_a_licence_block_with_checked_at(reg):
    from datetime import date

    for source in reg.sources:
        assert isinstance(source.licence.checked_at, date), source.source_id
        assert source.licence.commercial_use in {"allowed", "permission_granted", "denied", "unknown"}


def test_only_genvce_and_crea_are_loadable_today(reg):
    assert {s.source_id for s in reg.loadable_sources()} == {"GENVCE", "CREA"}
    for source in reg.sources:
        assert source.loadable == (source.licence.commercial_use in ("allowed", "permission_granted"))


def test_audit_verdicts_are_recorded(reg):
    denied = {"NAVARRA-AGRARIA", "INTIA-EXP", "CTIFL", "LFL-BAYERN", "INIAV-LVR", "ITACYL", "IFAPA",
              "IFAPA_ALMOND", "IFAPA_ALMENDRO_2023", "AHDB", "TAGEM_TR_2012", "TAGEM_TR_CATALOG_2015",
              "TAGEM_TR_CATALOG_2017"}
    unknown = {"BSL", "NEBIH", "EVENA", "INRAMAROC", "EU-TRIAL-REPORTS", "ECOCROP-GAEZ-V4", "CPVO",
               "SCIENTIA-PIAVE", "VISION2024", "REDALYC-PLEUROTUS-2017", "WAGENINGEN-FUNGAL-SUBSTRATES-2021",
               "NATURE-CORDYCEPS-2026", "HUNGARY-KING-OYSTER-2016", "EXCALIBUR-H2020"}
    assert {s.source_id for s in reg.sources if s.licence.commercial_use == "denied"} == denied
    assert {s.source_id for s in reg.sources if s.licence.commercial_use == "unknown"} == unknown


def test_genvce_attribution_is_the_literal_citation_with_the_real_download_date(reg):
    licence = reg.source("GENVCE").licence
    assert licence.commercial_use == "allowed"
    assert licence.download_date.isoformat() == "2026-06-01"
    assert licence.attribution_text == (
        "Fuente: Datos Abiertos GENVCE. Url: https://genvce.org/mapa-de-resultados/ (Descarga: 01/06/2026.)")
    assert licence.download_date.strftime("%d/%m/%Y") in licence.attribution_text
    assert licence.quote and "Descarga" in licence.quote and licence.quote_language == "es"


def test_crea_is_cc_by_with_the_five_booklets_and_a_processing_note(reg):
    licence = reg.source("CREA").licence
    assert licence.commercial_use == "allowed"
    assert licence.licence_id == "CC-BY-3.0-IT"
    assert [d.year for d in licence.source_documents] == [2021, 2022, 2023, 2024, 2025]
    assert "CC BY 3.0 IT" in licence.attribution_text
    assert licence.processing_note["en"] and licence.processing_note["es"]


def test_unknown_source_raises(reg):
    with pytest.raises(UnknownEntryError, match="unregistered source"):
        reg.source("NOPE")


def test_commercial_use_enum_is_enforced(copy_dir):
    dest, edit = copy_dir
    edit("sources.yaml", lambda d: d["sources"][0]["licence"].update(commercial_use="maybe"))
    with pytest.raises(RegistryError, match="schema validation failed"):
        load_registries(dest)


def test_licence_block_is_required(copy_dir):
    dest, edit = copy_dir
    edit("sources.yaml", lambda d: d["sources"][0].pop("licence"))
    with pytest.raises(RegistryError, match="schema validation failed"):
        load_registries(dest)


def test_checked_at_is_required(copy_dir):
    dest, edit = copy_dir
    edit("sources.yaml", lambda d: d["sources"][1]["licence"].pop("checked_at"))
    with pytest.raises(RegistryError, match="schema validation failed"):
        load_registries(dest)


def test_loadable_source_needs_attribution_and_granted_permission_needs_a_reference(copy_dir):
    dest, edit = copy_dir

    def genvce(d):
        return next(s for s in d["sources"] if s["source_id"] == "GENVCE")["licence"]

    edit("sources.yaml", lambda d: genvce(d).update(attribution_text=""))
    with pytest.raises(RegistryError, match="attribution_text"):
        load_registries(dest)
    edit("sources.yaml", lambda d: genvce(d).update(attribution_text="x", commercial_use="permission_granted"))
    with pytest.raises(RegistryError, match="permission_ref"):
        load_registries(dest)


def test_denied_source_needs_evidence(copy_dir):
    dest, edit = copy_dir
    edit("sources.yaml", lambda d: d["sources"][0]["licence"].update(quote=None, notes=None))
    with pytest.raises(RegistryError, match="quote or a note"):
        load_registries(dest)


def test_a_quote_needs_its_language(copy_dir):
    dest, edit = copy_dir
    edit("sources.yaml", lambda d: d["sources"][0]["licence"].update(quote_language=None))
    with pytest.raises(RegistryError, match="quote_language"):
        load_registries(dest)


@pytest.mark.parametrize(
    ("name", "mutate"),
    [
        ("variables.yaml", lambda d: d["variables"][0]["raw_keys"].update({"NOSRC": ["x"]})),
        ("crops.yaml", lambda d: d["crops"][0].update(seen_in=["NOSRC"])),
        ("units.yaml", lambda d: d["source_units"].update({"NOSRC": {"kg/ha": "kg/ha"}})),
    ],
)
def test_references_to_unknown_sources_are_errors(copy_dir, name, mutate):
    dest, edit = copy_dir
    edit(name, mutate)
    with pytest.raises(RegistryError, match="unknown source"):
        load_registries(dest)


def test_duplicate_source_id_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("sources.yaml", lambda d: d["sources"].append(dict(d["sources"][0])))
    with pytest.raises(RegistryError, match="duplicate source id"):
        load_registries(dest)


# ── sites ────────────────────────────────────────────────────────────────────

def test_site_ids_names_and_aliases_resolve_to_exactly_one_site(reg):
    for site in reg.sites:
        for name in (site.id, site.name, *site.aliases):
            assert reg.site(name) is site
    assert len({s.id for s in reg.sites}) == len(reg.sites)


@pytest.mark.parametrize(
    ("raw", "site_id"),
    [
        ("Valladolid", "ES-VALLADOLID"),
        ("córdoba", "ES-CORDOBA"),
        ("  Villafranca Piemonte (TO) ", "IT-VILLAFRANCA-PIEMONTE"),
        ("Villafranca Piemonte", "IT-VILLAFRANCA-PIEMONTE"),
        ("Media 14 Località", "IT-CREA-AVG-14"),
        ("Zamadueñas (Valladolid)", "ES-ZAMADUENAS"),
        ("ES-LUGO", "ES-LUGO"),
    ],
)
def test_site_resolution(reg, raw, site_id):
    assert reg.site(raw).id == site_id


def test_valladolid_and_zamaduenas_are_not_merged_and_the_assumption_is_recorded(reg):
    assert reg.site("Valladolid") is not reg.site("Zamadueñas")
    for site_id in ("ES-VALLADOLID", "ES-ZAMADUENAS"):
        site = reg.site(site_id)
        assert site.status == "assumption"
        assert "ASSUMPTION" in site.note


@pytest.mark.parametrize("raw", [None, "", "Fundulea", "Foggia", "Narnia"])
def test_unresolved_or_deliberately_unregistered_sites_are_none(reg, raw):
    assert reg.site(raw) is None


def test_aggregates_have_a_site_kind_and_no_coordinates(reg):
    aggregates = [s for s in reg.sites if s.site_kind == "aggregate"]
    ids = {s.id for s in aggregates}
    assert {"IT-CREA-AVG-8", "IT-CREA-AVG-10", "IT-CREA-AVG-13", "IT-CREA-AVG-14"} <= ids
    # GENVCE prints means of zones, strata and groups, never a trial location: every site it owns is one
    assert {s.id for s in reg.sites if s.sources == ("GENVCE",)} <= ids
    assert any(i.startswith("ES-GENVCE-") for i in ids)
    for site in reg.sites:
        assert site.site_kind in {e.id for e in reg.vocab_entries("site_kind")}
        if site.site_kind != "field":
            assert site.latitude is None and site.longitude is None


def test_site_coordinates_are_documented_and_never_farm_precision(reg):
    for site in reg.sites:
        if site.latitude is not None:
            assert site.coordinate_source
            # municipality-level geocodes: no more than five decimals, never a surveyed point
            assert round(site.latitude, 5) == site.latitude and round(site.longitude, 5) == site.longitude


def test_site_sources_are_registered(reg):
    for site in reg.sites:
        for source_id in site.sources:
            assert reg.source(source_id)


def test_aggregate_with_coordinates_is_an_error(copy_dir):
    dest, edit = copy_dir

    def disguise(d):
        site = next(s for s in d["sites"] if s["id"] == "IT-CREA-AVG-8")
        site.update(latitude=45.0, longitude=9.0, coordinate_source="x")

    edit("sites.yaml", disguise)
    with pytest.raises(RegistryError, match="no coordinates"):
        load_registries(dest)


def test_site_alias_collision_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("sites.yaml", lambda d: d["sites"][1].setdefault("aliases", []).append("Valladolid"))
    with pytest.raises(RegistryError, match="site alias"):
        load_registries(dest)


def test_unknown_site_kind_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("sites.yaml", lambda d: d["sites"][0].update(site_kind="plot"))
    with pytest.raises(RegistryError, match="unknown site_kind"):
        load_registries(dest)


def test_coordinates_need_a_source_and_come_in_pairs(copy_dir):
    dest, edit = copy_dir
    edit("sites.yaml", lambda d: d["sites"][2].pop("coordinate_source"))
    with pytest.raises(RegistryError, match="coordinate_source"):
        load_registries(dest)


def test_assumption_without_a_note_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("sites.yaml", lambda d: d["sites"][0].pop("note"))
    with pytest.raises(RegistryError, match="needs its note"):
        load_registries(dest)


# ── ranges ───────────────────────────────────────────────────────────────────

IRRIGATED_URI = "http://aims.fao.org/aos/agrovoc/c_3954"
RAINFED_URI = "http://aims.fao.org/aos/agrovoc/c_6436"


def test_range_review_invariants_hold_for_every_range(reg):
    assert reg.ranges
    for rng in reg.ranges:
        assert rng.min < rng.max
        if rng.status == "reviewed":
            assert (rng.reviewer or "").strip() and (rng.evidence or "").strip(), rng.id
        else:
            assert rng.status == "assumption"
            assert "pending agronomist review" in rng.note, rng.id


def test_ranges_are_unique_and_reference_registered_crops_variables_and_units(reg):
    assert len({r.id for r in reg.ranges}) == len(reg.ranges)
    keys = [(r.crop, r.variable, tuple(sorted(r.conditions.items()))) for r in reg.ranges]
    assert len(keys) == len(set(keys))
    for rng in reg.ranges:
        assert reg.crop(rng.crop).eppo == rng.crop
        assert reg.variable(rng.variable).unit == rng.unit


def test_range_for_picks_the_most_specific_matching_range(reg):
    base = reg.range_for("HORVX", "crop_yield", {"purpose": "grain"})
    rainfed = reg.range_for("HORVX", "crop_yield", {"purpose": "grain", "irrigation": "rainfed"})
    irrigated = reg.range_for("HORVX", "crop_yield", {"purpose": "grain", "irrigation": "irrigated"})
    assert (base.id, rainfed.id, irrigated.id) == (
        "crop_yield.HORVX.grain", "crop_yield.HORVX.grain.rainfed", "crop_yield.HORVX.grain.irrigated")
    assert (base.max, rainfed.max, irrigated.max) == (14000, 10000, 13500)


def test_range_for_accepts_literals_uris_and_eppo_aliases(reg):
    expected = "crop_yield.ZEAMX.grain.irrigated"
    for irrigation in ("irrigated", "regadío", "irrigato", IRRIGATED_URI):
        assert reg.range_for("ZEAMA", "crop_yield", {"purpose": "grain", "irrigation": irrigation}).id == expected
    assert reg.range_for("eppo:HORVX", "crop_yield", {"purpose": "grain", "irrigation": RAINFED_URI}).id == (
        "crop_yield.HORVX.grain.rainfed")


def test_range_for_falls_back_to_the_less_specific_range(reg):
    # no rainfed maize range: the unconditional grain envelope applies
    assert reg.range_for("ZEAMX", "crop_yield", {"purpose": "grain", "irrigation": "rainfed"}).id == (
        "crop_yield.ZEAMX.grain")
    # wheat has no irrigated range
    assert reg.range_for("TRZAX", "crop_yield", {"purpose": "grain", "irrigation": "irrigated"}).id == (
        "crop_yield.TRZAX.grain")
    # an unrecognised or missing condition value is not a match for a conditional range
    assert reg.range_for("HORVX", "crop_yield", {"purpose": "grain", "irrigation": "drip"}).id == (
        "crop_yield.HORVX.grain")
    assert reg.range_for("HORVX", "crop_yield", {"purpose": "grain", "irrigation": None}).id == (
        "crop_yield.HORVX.grain")


def test_range_for_without_purpose_or_with_forage_checks_nothing(reg):
    assert reg.range_for("ZEAMX", "crop_yield") is None
    assert reg.range_for("ZEAMX", "crop_yield", {"irrigation": "irrigated"}) is None
    assert reg.range_for("ZEAMX", "crop_yield", {"purpose": "forage"}) is None


def test_range_for_unknown_crop_is_none_and_unknown_variable_or_condition_raises(reg):
    assert reg.range_for("NOPE", "crop_yield", {"purpose": "grain"}) is None
    assert reg.range_for("CIEAR", "crop_yield", {"purpose": "grain"}) is None
    with pytest.raises(UnknownEntryError):
        reg.range_for("ZEAMX", "not_a_variable")
    with pytest.raises(ValueError, match="unknown range condition"):
        reg.range_for("ZEAMX", "crop_yield", {"colour": "red"})


def test_range_with_a_climate_class_condition_is_more_specific(copy_dir):
    dest, edit = copy_dir

    def add(d):
        d["ranges"].append({
            "id": "crop_yield.HORVX.grain.rainfed.BSk", "crop": "HORVX", "variable": "crop_yield",
            "conditions": {"purpose": "grain", "irrigation": "rainfed", "climate_class": "BSk"},
            "min": 400, "max": 8000, "unit": "kg/ha", "status": "assumption", "note": "pending agronomist review"})

    edit("ranges.yaml", add)
    loaded = load_registries(dest)
    conditions = {"purpose": "grain", "irrigation": "rainfed", "climate_class": "BSk"}
    assert loaded.range_for("HORVX", "crop_yield", conditions).max == 8000
    assert loaded.range_for("HORVX", "crop_yield", {**conditions, "climate_class": "Csa"}).max == 10000


def test_equally_specific_overlapping_ranges_are_rejected(copy_dir):
    dest, edit = copy_dir

    def add(d):
        d["ranges"].append({
            "id": "crop_yield.HORVX.organic", "crop": "HORVX", "variable": "crop_yield",
            "conditions": {"production_system": "organic"},
            "min": 400, "max": 8000, "unit": "kg/ha", "status": "assumption", "note": "pending agronomist review"})

    edit("ranges.yaml", add)  # one condition, like the base range, and both can match
    with pytest.raises(RegistryError, match="equally specific"):
        load_registries(dest)


def test_ranges_with_identical_conditions_are_rejected(copy_dir):
    dest, edit = copy_dir
    edit("ranges.yaml", lambda d: d["ranges"].append({**d["ranges"][0], "id": "dup"}))
    with pytest.raises(RegistryError, match="same conditions"):
        load_registries(dest)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda r: r.update(crop="CIEAS"), "canonical EPPO code"),
        (lambda r: r.update(variable="nope"), "unregistered variable"),
        (lambda r: r.update(unit="t/ha"), "differs from the variable"),
        (lambda r: r.update(conditions={"purpose": "plot"}), "vocabulary id"),
        (lambda r: r.update(conditions={"colour": "red"}), "unknown condition"),
        (lambda r: r.update(min=9, max=9), "min must be below max"),
        (lambda r: r.update(status="reviewed"), "names its reviewer"),
        (lambda r: r.update(note="fine"), "pending agronomist review"),
    ],
)
def test_invalid_range_is_an_error(copy_dir, mutate, message):
    dest, edit = copy_dir
    edit("ranges.yaml", lambda d: mutate(d["ranges"][0]))
    with pytest.raises(RegistryError, match=message):
        load_registries(dest)


def test_reviewed_range_needs_reviewer_and_evidence(copy_dir):
    dest, edit = copy_dir
    edit("ranges.yaml", lambda d: d["ranges"][0].update(status="reviewed", reviewer=None, evidence="trial data"))
    with pytest.raises(RegistryError, match="names its reviewer"):
        load_registries(dest)
    edit("ranges.yaml", lambda d: d["ranges"][0].update(reviewer="agronomist-1", evidence=None))
    with pytest.raises(RegistryError, match="carries its evidence"):
        load_registries(dest)
    edit("ranges.yaml", lambda d: d["ranges"][0].update(reviewer="agronomist-1", evidence="trial data"))
    assert load_registries(dest).ranges[0].status == "reviewed"


# ── varieties ────────────────────────────────────────────────────────────────

def test_varieties_cover_the_four_crops_with_unique_ids(reg):
    assert {v.crop for v in reg.varieties} == {"ZEAMX", "HORVX", "TRZAX", "BRSNN"}
    assert len({v.id for v in reg.varieties}) == len(reg.varieties) > 700
    for variety in reg.varieties:
        assert reg.crop(variety.crop).eppo == variety.crop
        assert variety.sources and all(reg.source(s) for s in variety.sources)


def test_every_variety_name_and_alias_resolves_to_one_variety_per_crop(reg):
    for variety in reg.varieties:
        for name in (variety.name, *variety.aliases):
            assert reg.variety(variety.crop, name) is variety


def test_variety_status_invariants_hold_for_every_variety(reg):
    for variety in reg.varieties:
        if variety.aliases:
            assert variety.status != "candidate", variety.id
            assert (variety.evidence or "").strip(), variety.id
        if variety.status == "candidate":
            assert not variety.aliases
        if variety.status == "reviewed":
            assert (variety.reviewer or "").strip() and (variety.evidence or "").strip(), variety.id


def test_reviewed_variety_needs_reviewer_and_evidence(copy_dir):
    dest, edit = copy_dir
    edit("varieties.yaml", lambda d: d["varieties"][0].update(status="reviewed", reviewer=None, evidence="catalogue"))
    with pytest.raises(RegistryError, match="names its reviewer"):
        load_registries(dest)
    edit("varieties.yaml", lambda d: d["varieties"][0].update(reviewer="agronomist-1", evidence=None))
    with pytest.raises(RegistryError, match="carries its evidence"):
        load_registries(dest)
    edit("varieties.yaml", lambda d: d["varieties"][0].update(reviewer="agronomist-1", evidence="catalogue"))
    assert load_registries(dest).varieties[0].status == "reviewed"


def test_annotation_variants_resolve_to_the_clean_name(reg):
    clean = reg.variety("ZEAMX", "DKC6667YG")
    assert clean.status == "assumption"
    for raw in ("DKC6667YG (T)", "DKC6667YG (T) *", "dkc6667yg (t)*", "  DKC6667YG  "):
        assert reg.variety("ZEAMX", raw) is clean
    assert reg.variety("HORVX", "Pewter (R)") is reg.variety("HORVX", "PEWTER (T)")
    assert reg.variety("ZEAMA", "P1921 *") is reg.variety("ZEAMX", "P1921")


def test_breeder_code_and_denomination_in_one_raw_string_are_one_variety(reg):
    maya = reg.variety("HORVX", "96054-518 (MAYA)")
    assert maya.name == "MAYA" and reg.variety("HORVX", "96054-518") is maya
    mufasa = reg.variety("TRZAX", "MUFASA (FD14WW060)")
    assert mufasa.name == "MUFASA" and reg.variety("TRZAX", "FD14WW060") is mufasa
    rocio = reg.variety("HORVX", "ROCÍO (NSL03-6838)")
    assert rocio.name == "ROCÍO" and reg.variety("HORVX", "NSL03-6838") is rocio


def test_formatting_variants_are_not_merged(reg):
    # no fuzzy fusion: spacing/punctuation variants stay separate until a person decides
    pairs = [("ZEAMX", "LG30.444", "LG 30.444"), ("ZEAMX", "MAS 68K", "MAS 68.K"),
             ("ZEAMX", "INDEM668", "INDEM 668"), ("HORVX", "ROCIO", "ROCÍO")]
    for crop, a, b in pairs:
        first, second = reg.variety(crop, a), reg.variety(crop, b)
        assert first is not None and second is not None, (a, b)
        assert first is not second


def test_a_variety_belongs_to_one_crop(reg):
    assert reg.variety("HORVX", "HISPANIC") is not None
    assert reg.variety("ZEAMX", "HISPANIC") is None


@pytest.mark.parametrize("name", ["FDL Columna", "FDL Abund", "Pitar", "Antalis", "Claudio", "Svevo"])
def test_names_of_out_of_scope_rows_are_not_registered(reg, name):
    # Romanian wheat stored under CREA (unlicensed paper) and the placeholder durum rows
    for crop in ("TRZAX", "ZEAMX"):
        assert reg.variety(crop, name) is None


@pytest.mark.parametrize(("crop", "name"), [(None, "P1921"), ("ZEAMX", None), ("ZEAMX", ""), ("NOPE", "P1921"),
                                            ("ZEAMX", "NOT A VARIETY")])
def test_unregistered_variety_is_none(reg, crop, name):
    assert reg.variety(crop, name) is None


def test_variety_alias_shared_by_two_varieties_is_an_error(copy_dir):
    dest, edit = copy_dir

    def clash(d):
        first, second = d["varieties"][0], d["varieties"][1]
        second.update(aliases=[first["name"]], evidence="x", status="assumption")

    edit("varieties.yaml", clash)
    with pytest.raises(RegistryError, match="more than one variety"):
        load_registries(dest)


def test_alias_group_without_evidence_or_with_candidate_status_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("varieties.yaml", lambda d: d["varieties"][0].update(aliases=["X1"], status="assumption", evidence=None))
    with pytest.raises(RegistryError, match="evidence"):
        load_registries(dest)
    edit("varieties.yaml", lambda d: d["varieties"][0].update(evidence="because", status="candidate"))
    with pytest.raises(RegistryError, match="not a candidate"):
        load_registries(dest)


def test_variety_with_unregistered_crop_or_source_is_an_error(copy_dir):
    dest, edit = copy_dir
    edit("varieties.yaml", lambda d: d["varieties"][0].update(sources=["NOSRC"]))
    with pytest.raises(RegistryError, match="unknown source"):
        load_registries(dest)


def test_variety_id_must_start_with_its_crop(copy_dir):
    dest, edit = copy_dir
    edit("varieties.yaml", lambda d: d["varieties"][0].update(crop="HORVX"))
    with pytest.raises(RegistryError, match="starts with its crop"):
        load_registries(dest)
