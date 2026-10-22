"""GENVCE-TRIALS adapter: the per-locality sheet extractions of the GENVCE results map -> raw rows.

Fixtures under ``fixtures/genvce_trials/data/results_map/extractions``: the real extraction of the maize
600-700 sheet of Cadreita 2025 (XLSX + PDF, 14 varieties) and of the winter soft-wheat sheet of Olite 2026
(PDF only, 8 varieties), plus a synthetic twin of the Olite sheet (id 30399) whose map entry title states
"no tratado con fungicida", as the map does for some sites.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import date
from pathlib import Path

import pytest

from app.graph.evidence_policy import is_field_evidence
from app.kg import identity
from app.kg.adapters import AdapterError, genvce_trials
from app.kg.contracts import Expected, load_contract, run_contract
from app.kg.gate import run_gate
from app.kg.model import FactorLevel
from app.kg.registries import load_registries

FIXTURES = Path(__file__).parent / "fixtures" / "genvce_trials"
EXTRACTIONS = FIXTURES / "data" / "results_map" / "extractions"


@pytest.fixture(scope="module")
def loaded():
    return genvce_trials.load(FIXTURES)


def _rows_of(loaded, sheet_id: int) -> list[dict]:
    return [row for row in loaded.rows if row["sheet"]["id"] == sheet_id]


def test_a_row_carries_the_sheet_the_locality_the_variety_and_the_traits(loaded):
    rows = _rows_of(loaded, 28829)
    assert len(rows) == 14
    by_variety = {row["variety"]: row for row in rows}
    row = by_variety["P1441"]

    sheet = row["sheet"]
    assert (sheet["sowing_date"], sheet["harvest_date"], sheet["zone_label"]) == ("2025-05-29", "2025-11-24", "Regadíos templados")
    assert sheet["executing_entity"].startswith("Instituto Navarro")
    assert (sheet["trial_mean_kg_ha"], sheet["check_mean_kg_ha"], sheet["sd_kg_ha"], sheet["cv_pct"]) == (15099.0, 16231.5, 1471.02, 9.74)
    assert sheet["design"] == "Bloques Completos al Azar" and sheet["crop_label"] == "Maíz FAO 600-700"
    assert sheet["url_pdf"].endswith(".pdf") and sheet["url_xlsx"].endswith(".xlsx")
    assert "fungicide" not in sheet

    doc = row["doc"]
    assert doc["title"] == "Ensayo Maíz ciclo 600-700, Cadreita, 2025"
    assert doc["issue"] == "28829" and doc["year"] == 2025
    assert doc["file_name"] == "Cadreita-Maiz-2025-600-700-1.pdf" and len(doc["raw_sha256"]) == 64

    assert (row["crop"], row["site"], row["season"], row["irrigation"]) == ("Maíz FAO 600-700", "Cadreita", "2025", "regadío")
    assert "cycle_class" not in row and "production_system" not in row
    assert row["is_check"] is False and row["is_pre_evaluation"] is False
    assert (row["yield_std_kg_ha"], row["yield_moisture_pct"], row["yield_raw_kg_ha"]) == (17012.0, 14.0, 18767.0)
    assert (row["relative_yield_pct"], row["statistical_group"]) == (104.81, "a")
    assert row["traits"] == {
        "flowering_female_date": "2025-08-02", "grain_moisture_pct": 22.1, "plant_density_m2": 9.7,
        "plant_height_cm": 254.3, "ear_height_cm": 107.3, "stalk_breakage_pct": 0.3,
    }
    assert by_variety["DKC6351YG"]["is_check"] is True


def test_winter_wheat_sheet_is_rainfed_winter_and_dated_by_harvest_year(loaded):
    rows = _rows_of(loaded, 30324)
    assert len(rows) == 8
    chambo = next(row for row in rows if row["variety"] == "CHAMBO")
    assert (chambo["season"], chambo["irrigation"], chambo["cycle_class"]) == ("2026", "secano", "invierno")
    assert chambo["sheet"]["sowing_date"] == "2025-10-27" and chambo["doc"]["year"] == 2026
    assert "yield_raw_kg_ha" not in chambo and chambo["yield_moisture_pct"] == 13.0
    assert chambo["traits"]["heading_date"] == "2026-04-17" and chambo["traits"]["test_weight_kg_hl"] == 83.1
    assert chambo["traits"]["septoria_score_0_9"] == 0.0


def test_fungicide_variant_is_a_sheet_field_only_when_stated(loaded):
    untreated = _rows_of(loaded, 30399)
    assert untreated and all(row["sheet"]["fungicide"] == "untreated" for row in untreated)
    assert all("fungicide" not in row["sheet"] for row in _rows_of(loaded, 30324))
    assert genvce_trials.fungicide_variant("Trigo blando de primavera tratado con fungicida", "trigo-blando-de-primavera-tratado-con-fungicida-2") == "treated"
    assert genvce_trials.fungicide_variant("Trigo duro", "trigo-duro-nt") == "untreated"
    assert genvce_trials.fungicide_variant("Trigo duro", "trigo-duro-t-4") == "treated"
    assert genvce_trials.fungicide_variant("Cebada de invierno", "cebada-de-invierno-7") is None
    # the other spellings the map uses
    assert genvce_trials.fungicide_variant("Trigo blando de invierno sin fungicida", "trigo-blando-de-invierno-3") == "untreated"
    assert genvce_trials.fungicide_variant("Trigo duro con fungicida", "trigo-duro-9") == "treated"
    assert genvce_trials.fungicide_variant("Trigo blando de primavera NT", "trigo-blando-de-primavera-12") == "untreated"
    assert genvce_trials.fungicide_variant("Trigo blando de primavera T", "trigo-blando-de-primavera-13") == "treated"
    assert genvce_trials.fungicide_variant("Trigo blando de invierno", "trigo-blando-de-invierno-4",
                                           title="Ensayo Trigo blando de invierno T, Azpa, 2024") == "treated"
    assert genvce_trials.fungicide_variant("Triticale", "triticale-2", title="Ensayo Triticale, Olite, 2024") is None


def test_a_regime_stated_only_in_the_entry_title_is_used(tmp_path):
    source = _copy_fixtures(tmp_path)
    path = source / "data/results_map/extractions/28829.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["zone_label"] = None
    data["post_title"] = "Colza Secano"
    path.write_text(json.dumps(data), encoding="utf-8")

    rows = _rows_of(genvce_trials.load(source), 28829)

    assert all(row["irrigation"] == "secano" for row in rows)


def test_dates_contradicting_the_harvest_year_are_repaired_or_dropped(tmp_path):
    source = _copy_fixtures(tmp_path)
    path = source / "data/results_map/extractions/30324.json"  # a 2026 harvest
    data = json.loads(path.read_text(encoding="utf-8"))

    data["sowing_date"], data["harvest_date"] = "2026-10-27", "2026-06-10"  # an autumn sowing printed in the harvest year
    path.write_text(json.dumps(data), encoding="utf-8")
    sheet = _rows_of(genvce_trials.load(source), 30324)[0]["sheet"]
    assert (sheet["sowing_date"], sheet["harvest_date"]) == ("2025-10-27", "2026-06-10")

    data["sowing_date"], data["harvest_date"] = "2025-10-27", "2025-06-10"  # a harvest printed a year early
    path.write_text(json.dumps(data), encoding="utf-8")
    sheet = _rows_of(genvce_trials.load(source), 30324)[0]["sheet"]
    assert (sheet["sowing_date"], sheet["harvest_date"]) == ("2025-10-27", None)

    data["sowing_date"], data["harvest_date"] = "2024-11-26", "2025-07-01"  # both a year early for a 2026 harvest
    path.write_text(json.dumps(data), encoding="utf-8")
    loaded = genvce_trials.load(source)
    sheet = _rows_of(loaded, 30324)[0]["sheet"]
    assert (sheet["sowing_date"], sheet["harvest_date"]) == (None, None)
    assert {w.code for w in loaded.warnings} >= {"sowing_date_dropped", "harvest_date_dropped"}


def test_organic_label_sets_the_production_system(tmp_path):
    source = _copy_fixtures(tmp_path)
    data = json.loads((source / "data/results_map/extractions/30324.json").read_text(encoding="utf-8"))
    data["crop_label"] = "Trigo blando ecológico de primavera"
    (source / "data/results_map/extractions/30324.json").write_text(json.dumps(data), encoding="utf-8")

    rows = _rows_of(genvce_trials.load(source), 30324)

    assert all(row["production_system"] == "ecológico" and row["cycle_class"] == "primavera" for row in rows)
    assert all(row["crop"] == "Trigo blando ecológico de primavera" for row in rows)


def test_an_entry_without_yield_is_a_row_without_yield(tmp_path):
    source = _copy_fixtures(tmp_path)
    path = source / "data/results_map/extractions/28829.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["rows"][0]["yield_std_kg_ha"] = None  # as the parsers write an entry whose yield cell is empty
    path.write_text(json.dumps(data), encoding="utf-8")

    loaded = genvce_trials.load(source)

    row = next(r for r in loaded.rows if r["sheet"]["id"] == 28829 and r["variety"] == "P1441")
    assert "yield_std_kg_ha" not in row and "yield_moisture_pct" not in row
    assert row["traits"]["plant_height_cm"] == 254.3
    assert any(w.code == "entry_without_yield" for w in loaded.warnings)


def test_a_score_outside_its_printed_scale_is_dropped_and_counted(tmp_path):
    source = _copy_fixtures(tmp_path)
    path = source / "data/results_map/extractions/30324.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["rows"][0]["traits"]["septoria_score_0_9"] = 20.0  # a percentage printed under a "(0-9)" head
    path.write_text(json.dumps(data), encoding="utf-8")

    loaded = genvce_trials.load(source)

    chambo = next(r for r in loaded.rows if r["sheet"]["id"] == 30324 and r["variety"] == "CHAMBO")
    assert "septoria_score_0_9" not in chambo["traits"] and chambo["traits"]["brown_rust_score_0_9"] == 0.0
    assert any(w.code == "value_outside_printed_scale" for w in loaded.warnings)


def test_a_harvest_date_not_after_sowing_is_dropped_and_counted(tmp_path):
    source = _copy_fixtures(tmp_path)
    path = source / "data/results_map/extractions/28829.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["harvest_date"] = "2025-05-29"  # equal to the sowing date, as some sheets print
    path.write_text(json.dumps(data), encoding="utf-8")

    loaded = genvce_trials.load(source)

    row = next(r for r in loaded.rows if r["sheet"]["id"] == 28829)
    assert row["sheet"]["sowing_date"] == "2025-05-29" and row["sheet"]["harvest_date"] is None
    assert any(w.code == "harvest_not_after_sowing" for w in loaded.warnings)


def test_an_unknown_trait_column_is_refused_not_dropped(tmp_path):
    source = _copy_fixtures(tmp_path)
    path = source / "data/results_map/extractions/28829.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["rows"][0]["traits"]["altura_total_m"] = 2.5
    path.write_text(json.dumps(data), encoding="utf-8")

    with pytest.raises(AdapterError, match="altura_total_m"):
        genvce_trials.load(source)


def test_an_unknown_extraction_field_is_refused(tmp_path):
    source = _copy_fixtures(tmp_path)
    path = source / "data/results_map/extractions/28829.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["new_field"] = 1
    path.write_text(json.dumps(data), encoding="utf-8")

    with pytest.raises(AdapterError, match="new_field"):
        genvce_trials.load(source)


def test_inputs_are_fingerprinted(loaded):
    assert len(loaded.inputs) == 3
    for relative, sha in loaded.inputs:
        assert relative.startswith("data/results_map/extractions/")
        assert sha == hashlib.sha256((FIXTURES / relative).read_bytes()).hexdigest()


def _copy_fixtures(tmp_path: Path) -> Path:
    target = tmp_path / "genvce"
    shutil.copytree(FIXTURES, target)
    return target


# ── the contract over the fixtures ──

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "data" / "sources" / "GENVCE-TRIALS.yaml"
REGISTRIES = load_registries()


def _expected_observations() -> int:
    """One crop_yield per row plus every optional row field and every trait the fixtures carry."""
    total = 0
    for path in sorted(EXTRACTIONS.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        cycle = 1 if (" de invierno" in data["crop_label"] or " de primavera" in data["crop_label"]) else 0
        for row in data["rows"]:
            total += 1 + sum(row.get(k) is not None for k in ("relative_yield_pct", "statistical_group", "yield_raw_kg_ha"))
            total += len(row["traits"]) + cycle
    return total


@pytest.fixture(scope="module")
def bundle(loaded):
    contract = load_contract(CONTRACT_PATH)
    expected = Expected(units=30, observations=_expected_observations(), sites=2)
    return run_contract(contract.model_copy(update={"expected": expected}), REGISTRIES, loaded.rows)


def test_contract_parses_and_runs_on_the_fixtures(bundle):
    assert bundle.source_id == "GENVCE-TRIALS"
    assert len(bundle.units) == 30 and len(bundle.sites) == 2
    assert len(bundle.studies) == 3 and len(bundle.documents) == 3
    assert {site.site_id for site in bundle.sites} == {"ES-GENVCE-TRIALS-CADREITA", "ES-GENVCE-TRIALS-OLITE"}
    assert all(site.site_kind == "field" and site.latitude is not None for site in bundle.sites)

    maize = [u for u in bundle.units if u.raw_site == "Cadreita"]
    assert len(maize) == 14 and all(u.crop_eppo == "ZEAMX" and u.year == 2025 for u in maize)
    p1441 = next(u for u in maize if u.raw_variety == "P1441")
    assert (p1441.raw_irrigation, p1441.yield_kg_ha, p1441.yield_basis, p1441.yield_moisture_pct) == (
        "regadío", 17012.0, "standard_moisture", 14.0)
    assert p1441.yield_metric == "grain" and p1441.purpose == "grain" and p1441.is_check is False
    assert next(u for u in maize if u.raw_variety == "DKC6351YG").is_check is True
    studies = {identity.study_key(s): s for s in bundle.studies}
    maize_study = studies[p1441.study_key]
    assert maize_study.raw_scope == "sheet.id=28829; sheet.crop_label=Maíz FAO 600-700; sheet.post_title=Maíz 600-700"
    assert (maize_study.sowing_date, maize_study.harvest_date) == (date(2025, 5, 29), date(2025, 11, 24))
    assert maize_study.executing_entity.startswith("Instituto Navarro") and maize_study.cv_pct == 9.74
    documents = {identity.document_key(d): d for d in bundle.documents}
    assert documents[p1441.document_key].issue == "28829"

    by_variable = {}
    for obs in bundle.observations:
        by_variable.setdefault(obs.variable_id, []).append(obs)
    assert set(by_variable) >= {"crop_yield", "relative_yield_pct", "statistical_group", "crop_yield_as_harvested",
                                "flowering_start_date", "heading_date", "jointing_date", "maturity_date",
                                "grain_moisture_harvest", "plant_density", "spike_density", "plant_height",
                                "ear_height", "stalk_breakage_pct", "thousand_kernel_weight", "test_weight",
                                "grain_protein_content", "lodging_pct", "powdery_mildew_score_0_9",
                                "septoria_score_0_9", "brown_rust_score_0_9", "yellow_rust_score_0_9", "cycle_class"}
    group = next(o for o in by_variable["statistical_group"] if o.unit_key == identity.unit_key(p1441))
    assert group.value_text == "a" and group.value is None
    flowering = next(o for o in by_variable["flowering_start_date"] if o.unit_key == identity.unit_key(p1441))
    assert flowering.value_text == "2025-08-02"
    wheat = [u for u in bundle.units if u.raw_site == "Olite"]
    assert all(u.crop_eppo == "TRZAX" and u.year == 2026 and u.raw_irrigation == "secano" for u in wheat)
    assert p1441.factor_levels == () and p1441.production_system is None


def test_two_sheets_of_the_same_terna_are_two_studies(bundle):
    olite = [u for u in bundle.units if u.raw_site == "Olite"]
    assert len({u.study_key for u in olite}) == 2 and len({u.document_key for u in olite}) == 2
    untreated = [u for u in olite if u.factor_levels]
    assert len(untreated) == 8
    assert untreated[0].factor_levels == (FactorLevel(factor="fungicide_treatment", level="untreated", unit=None),)
    report = run_gate(bundle, REGISTRIES, "production")
    assert "duplicate_key" not in report.error_counts and "key_conflict" not in report.error_counts
    assert report.errors == (), report.error_counts


def test_zone_label_drives_the_regime_and_the_unit_is_field_tier(bundle):
    regimes = {u.raw_site: u.irrigation_regime for u in bundle.units}
    assert regimes["Cadreita"] != regimes["Olite"] and None not in regimes.values()
    assert is_field_evidence(None, "Cadreita", "field", source_id="GENVCE-TRIALS") is True
    assert is_field_evidence(None, "Cadreita", "field", source_id="GENVCE") is False
