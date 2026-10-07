"""CREA adapter and contract.

The fixtures under ``fixtures/crea`` are rows copied from the raw extraction files (same layout as
``<source folder>/data/extractions``): a few rows of each booklet 2021-2025 (the network mean and the
field site), the 19 rows of Romanian wheat stored under CREA, and the 3 placeholder rows. A last test
runs the whole raw data when its location is given in ``NKZ_DATA_SOURCES_DIR`` (the private raw-data
repository) and checks the contract's expected counts.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest

from app.kg import identity
from app.kg.adapters import AdapterError, crea
from app.kg.contracts import (
    ContractDataError,
    Expected,
    UnmappedFieldError,
    load_contract,
    run_contract,
)
from app.kg.registries import load_registries

FIXTURES = Path(__file__).parent / "fixtures" / "crea"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "data" / "sources" / "CREA.yaml"
REGISTRIES = load_registries()
CONTRACT = load_contract(CONTRACT_PATH)

FIXTURE_ROWS, FIXTURE_UNITS, FIXTURE_OBSERVATIONS, FIXTURE_SITES = 16, 16, 132, 5
NETWORK_MEANS = {2021: "Media 14 Località", 2022: "Media 10 Località", 2023: "Media 8 Località",
                 2024: "Media 13 Località", 2025: "Media 14 Località"}
FIELD_SITE = "Villafranca Piemonte (TO)"


def contract_for(units: int, observations: int, sites: int):
    return CONTRACT.model_copy(update={"expected": Expected(units=units, observations=observations, sites=sites)})


@pytest.fixture(scope="module")
def loaded():
    return crea.load(FIXTURES)


@pytest.fixture(scope="module")
def bundle(loaded):
    return run_contract(contract_for(FIXTURE_UNITS, FIXTURE_OBSERVATIONS, FIXTURE_SITES), REGISTRIES, loaded.rows)


def trial(**over):
    row = {"crop": "Mais", "crop_scientific": "Zea mays", "variety": "ALFA", "agroclimatic_zone": "Pianura Padana",
           "year": 2024, "yield_kg_ha": 13000.0, "yield_relative_pct": 100.0,
           "quality_params": {"umidita_pct": 20.0, "peso_ettolitrico_kg_hl": 70.0},
           "disease_scores": None, "agronomic_traits": {"altezza_pianta_cm": 250}, "yield_notes": None,
           "special_status": None, "irrigation_regime": "irrigato", "trial_location": "Media 13 Località",
           "page_in_issue": 7, "table_number": 1, "confidence": "high"}
    row.update(over)
    return row


def booklet(trials, year=2024, eppo="ZEAMX"):
    return {"year": year, "section_key": "mais", "crop": "Mais", "eppo": eppo, "variety_trials": trials}


def rows(trials, year=2024, eppo="ZEAMX", sha=None):
    log = crea.WarningLog()
    out = crea.booklet_rows(booklet(trials, year, eppo), "f.json", log, pdf_sha256=sha)
    return out, {w.code: w for w in log.result()}


# ═════════════════════════════════════════════════════════════════════════════
# the booklets
# ═════════════════════════════════════════════════════════════════════════════

def test_a_row_carries_the_booklet_the_table_the_place_and_what_was_extracted():
    (row,), _ = rows([trial()], sha="a" * 64)
    assert row["doc"] == {"title": "Risultati reti nazionali di confronto varietale mais 2024", "year": 2024,
                          "file_name": "Fascicolo_risultati_Mais_2024.pdf", "raw_sha256": "a" * 64}
    assert row["table"] == {"number": "1", "page": 7}
    assert (row["crop"], row["variety"], row["site"], row["season"]) == ("ZEAMX", "ALFA", "Media 13 Località", "2024")
    assert row["quality_params"]["umidita_pct"] == 20.0 and "disease_scores" not in row and "yield_notes" not in row


def test_the_crop_is_the_eppo_code_the_file_declares_and_must_agree_with_the_rows():
    (row,), _ = rows([trial()], eppo="ZEAMA")
    assert row["crop"] == "ZEAMA"
    with pytest.raises(AdapterError, match="not the maize of a maize booklet"):
        rows([trial(crop="Grano tenero", crop_scientific="Triticum aestivum")])
    with pytest.raises(AdapterError, match="differs from the booklet year"):
        rows([trial(year=2019)])
    with pytest.raises(AdapterError, match="names its year and the EPPO code"):
        crea.booklet_rows({"variety_trials": []}, "f.json", crea.WarningLog())


def test_an_unknown_extraction_field_is_refused_not_dropped():
    with pytest.raises(AdapterError, match="unknown extraction field"):
        rows([{**trial(), "soil_ph": 7.1}])


def test_a_non_empty_disease_group_reaches_the_contract_which_refuses_it(loaded):
    (row,), _ = rows([trial(disease_scores={"oidio_pct": 3})])
    assert row["disease_scores"] == {"oidio_pct": 3}
    with pytest.raises(UnmappedFieldError, match="disease_scores.oidio_pct"):
        run_contract(contract_for(1, 0, 0), REGISTRIES, [row])


# ═════════════════════════════════════════════════════════════════════════════
# irrigation: only where the booklet supports it for the whole row
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize(("year", "place", "stated"), [
    (2023, "Media 8 Località", "irrigato"),
    (2023, FIELD_SITE, "irrigato"),
    (2024, "Media 13 Località", "irrigato"),
    (2024, FIELD_SITE, "irrigato"),
    (2025, FIELD_SITE, "irrigato"),
    (2025, "Media 14 Località", None),  # a location of the mean has a blank irrigation entry
    (2022, FIELD_SITE, None),           # no agronomic sheet that year
    (2021, FIELD_SITE, None),
    (2021, "Media 14 Località", None),
    (2022, "Media 10 Località", None),  # includes a location the booklet flags "Asciutta"
])
def test_irrigation_is_passed_on_only_where_the_booklet_states_it(year, place, stated):
    (row,), _ = rows([trial(year=year, trial_location=place)], year=year)
    assert row["irrigation"] == stated


def test_a_regime_left_out_is_counted_under_the_reason_that_applies():
    _, warnings = rows([trial(year=2022, trial_location="Media 10 Località"),
                        trial(year=2022, trial_location=FIELD_SITE)], year=2022)
    assert warnings["irrigation_mixed_regimes"].count == 1
    assert warnings["irrigation_not_stated_by_source"].count == 1


# ═════════════════════════════════════════════════════════════════════════════
# load(): the files, the out-of-scope rows, the manifest
# ═════════════════════════════════════════════════════════════════════════════

def test_load_reads_the_five_booklets_and_excludes_the_rows_that_are_not_crea_data(loaded):
    assert len(loaded.rows) == FIXTURE_ROWS
    assert {row["doc"]["year"] for row in loaded.rows} == {2021, 2022, 2023, 2024, 2025}
    names = [name for name, _ in loaded.inputs]
    assert names == sorted(names) and len(names) == 7  # five booklets and the two files excluded row by row


def test_the_19_mislabelled_rows_are_each_recorded_as_a_warning_not_fixed_silently(loaded):
    codes = {w.code: w for w in loaded.warnings}
    wheat = codes["mislabelled_rows_excluded"]
    assert wheat.count == 19 and len(wheat.where) == 19
    assert all(place.startswith("crea_mais_1993.json[") and "Grano tenero" in place for place in wheat.where)
    assert "stored in the legacy graph as CREA maize" in wheat.message
    assert codes["placeholder_rows_excluded"].count == 3 and len(codes["placeholder_rows_excluded"].where) == 3
    # none of them reached the rows, under any label
    assert not any(row["crop"] != "ZEAMX" or row["site"] in ("Fundulea", "Foggia") for row in loaded.rows)


def test_a_file_the_adapter_does_not_know_is_refused(tmp_path):
    shutil.copytree(FIXTURES, tmp_path / "crea")
    (tmp_path / "crea/data/extractions/other_crop_2024.json").write_text("{}")
    with pytest.raises(AdapterError, match="does not know"):
        crea.load(tmp_path / "crea")


def test_a_folder_with_no_booklet_is_refused(tmp_path):
    (tmp_path / "data/extractions").mkdir(parents=True)
    with pytest.raises(AdapterError, match="no extraction files"):
        crea.load(tmp_path)
    shutil.copy(FIXTURES / "data/extractions/grano_extractions.json", tmp_path / "data/extractions")
    with pytest.raises(AdapterError, match="no CREA booklet extraction"):
        crea.load(tmp_path)


def test_the_pdf_fingerprint_comes_from_the_raw_manifest_and_a_missing_entry_is_an_error(tmp_path):
    shutil.copytree(FIXTURES, tmp_path / "crea")
    digests = {year: hashlib.sha256(str(year).encode()).hexdigest() for year in NETWORK_MEANS}
    lines = [f"{digest} 1 data/pdfs/Fascicolo_risultati_Mais_{year}.pdf" for year, digest in digests.items()]
    (tmp_path / "crea/RAW_MANIFEST.sha256").write_text("# sha256 size path\n" + "\n".join(lines) + "\n")
    result = crea.load(tmp_path / "crea")
    assert {row["doc"]["year"]: row["doc"]["raw_sha256"] for row in result.rows} == digests
    (tmp_path / "crea/RAW_MANIFEST.sha256").write_text("\n".join(lines[:-1]) + "\n")
    with pytest.raises(AdapterError, match="no entry for data/pdfs/Fascicolo_risultati_Mais_2025.pdf"):
        crea.load(tmp_path / "crea")


# ═════════════════════════════════════════════════════════════════════════════
# the contract on the fixtures
# ═════════════════════════════════════════════════════════════════════════════

def test_every_row_becomes_one_unit_at_a_registered_site(bundle):
    assert len(bundle.units) == FIXTURE_UNITS
    assert not bundle.report.unresolved_sites and not bundle.report.unresolved_vocab
    kinds = {s.site_id: s.site_kind for s in bundle.sites}
    assert kinds == {"IT-CREA-AVG-8": "aggregate", "IT-CREA-AVG-10": "aggregate", "IT-CREA-AVG-13": "aggregate",
                     "IT-CREA-AVG-14": "aggregate", "IT-VILLAFRANCA-PIEMONTE": "field"}
    assert all(s.latitude is None for s in bundle.sites if s.site_kind == "aggregate")


def test_the_booklets_state_their_moisture_purpose_and_metric(bundle):
    assert {(u.yield_moisture_pct, u.purpose, u.yield_metric, u.yield_basis) for u in bundle.units} == {
        (15.5, "grain", "grain", "standard_moisture")}
    assert {u.crop_eppo for u in bundle.units} == {"ZEAMX"}


def test_the_yield_is_the_extractions_kg_per_ha_which_is_the_printed_quintals_times_100(bundle):
    dm = next(u for u in bundle.units if u.raw_variety == "DM5312" and u.raw_season == "2024"
              and u.raw_site == "Media 13 Località")
    assert (dm.yield_kg_ha, dm.yield_unit_original) == (13700.0, "kg/ha")  # the booklet prints 137.0 q/ha


def test_the_observations_are_the_booklets_traits_with_the_unit_the_source_prints(bundle):
    variables = {o.variable_id for o in bundle.observations}
    assert variables == {"crop_yield", "relative_yield_pct", "grain_moisture_harvest", "test_weight", "plant_height",
                         "ear_height", "lodging_pct", "stalk_breakage_pct", "plant_density"}
    weights = [o for o in bundle.observations if o.variable_id == "test_weight"]
    assert weights and {o.unit for o in weights} == {"kg/hL"} and {o.unit_original for o in weights} == {"kg/hl"}


def test_the_irrigated_rows_of_the_booklet_keep_their_regime_and_the_others_have_none(bundle):
    regimes = {(u.raw_season, u.raw_site): u.raw_irrigation for u in bundle.units}
    assert regimes[("2023", "Media 8 Località")] == "irrigato" and regimes[("2024", FIELD_SITE)] == "irrigato"
    assert regimes[("2022", "Media 10 Località")] is None and regimes[("2021", FIELD_SITE)] is None
    assert all(u.irrigation_regime is None for u in bundle.units if u.raw_irrigation is None)
    assert all(any(g.field == "raw_irrigation" for g in u.gaps) for u in bundle.units if u.raw_irrigation is None)


def test_the_booklet_is_the_document_with_its_file_and_a_study_per_table(bundle):
    assert len(bundle.documents) == 5 and all(d.file_name.startswith("Fascicolo_risultati_Mais_") for d in bundle.documents)
    assert len(bundle.studies) == 10  # five years x (network mean, field site)


# ═════════════════════════════════════════════════════════════════════════════
# the ZEAMA / ZEAMX twins
# ═════════════════════════════════════════════════════════════════════════════

def test_the_zeama_twin_of_every_row_collapses_into_the_same_unit_through_the_crop_alias(loaded):
    twins = []
    for source in json.loads((FIXTURES / "data/extractions/crea_mais_2024.json").read_text())["variety_trials"]:
        pair = []
        for eppo in ("ZEAMX", "ZEAMA"):
            log = crea.WarningLog()
            pair += crea.booklet_rows(booklet([copy.deepcopy(source)], eppo=eppo), "f.json", log)
        twins += pair
    assert len(twins) == 8
    only_2024 = run_contract(contract_for(4, 34, 2), REGISTRIES, twins)
    assert len(only_2024.units) == 4 and only_2024.report.collapsed_duplicate_units == 4
    assert {u.crop_eppo for u in only_2024.units} == {"ZEAMX"}
    assert len({identity.unit_key(u) for u in only_2024.units}) == 4


def test_a_twin_with_a_different_value_is_not_merged_but_refused():
    first, _ = rows([trial()])
    second, _ = rows([trial(yield_kg_ha=14000.0)], eppo="ZEAMA")
    with pytest.raises(ContractDataError, match="same unit key"):
        run_contract(contract_for(1, 0, 1), REGISTRIES, first + second)


# ═════════════════════════════════════════════════════════════════════════════
# the contract itself
# ═════════════════════════════════════════════════════════════════════════════

def test_the_contract_states_its_raw_layer_and_the_pinned_commit():
    assert CONTRACT.adapter == "app.kg.adapters.crea"
    assert CONTRACT.raw.repo == "nkz-data-sources" and "crea_mais_20" in CONTRACT.raw.paths[0]
    assert "@4c8e2762d48877b58f40b379bd5bb598d4f7c291" in CONTRACT.raw.extraction_version


def test_the_contract_quotes_the_booklet_for_purpose_metric_basis_and_moisture():
    unit = CONTRACT.unit
    assert unit.yield_.moisture_pct.default == 15.5
    for declared in (unit.purpose, unit.yield_.metric, unit.yield_.basis, unit.yield_.moisture_pct):
        assert "15.5" in declared.justification or "granella" in declared.justification
    assert {entry.field for entry in CONTRACT.ignore} == {"confidence", "special_status", "agroclimatic_zone"}


# ═════════════════════════════════════════════════════════════════════════════
# the whole raw data (only where the private repository is available)
# ═════════════════════════════════════════════════════════════════════════════

RAW_REPO = os.environ.get("NKZ_DATA_SOURCES_DIR", "")


@pytest.mark.skipif(not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to the raw-data repository to run")
def test_the_whole_extraction_builds_and_matches_the_contracts_expected_counts():
    result = crea.load(Path(RAW_REPO) / "crea")
    assert len(result.rows) == 320
    codes = {w.code: w.count for w in result.warnings}
    assert codes["mislabelled_rows_excluded"] == 19 and codes["placeholder_rows_excluded"] == 3
    built = run_contract(CONTRACT, REGISTRIES, result.rows)
    assert (len(built.units), len(built.observations), len(built.sites)) == (
        CONTRACT.expected.units, CONTRACT.expected.observations, CONTRACT.expected.sites)
    assert all(d.raw_sha256 for d in built.documents)
