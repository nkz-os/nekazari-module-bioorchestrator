"""GENVCE adapter and contract.

The fixtures under ``fixtures/genvce`` are rows copied from the raw extraction files (same layout as
``<source folder>/data/extractions``), chosen to hold each quirk once: groups labelled the way the
table prints them, regimes the extraction assumed, bare disease keys, numbers printed as text, a
score outside its scale, string table numbers, notes copied from other tables. The rules are then
pinned on small synthetic extractions. A last test runs the whole raw data when its location is given
in ``NKZ_DATA_SOURCES_DIR`` (the private raw-data repository) and checks the contract's expected counts.
"""
from __future__ import annotations

import copy
import os
from pathlib import Path

import pytest

from app.kg import identity
from app.kg.adapters import AdapterError, genvce
from app.kg.contracts import (
    ContractDataError,
    Expected,
    UnmappedFieldError,
    load_contract,
    run_contract,
)
from app.kg.registries import load_registries

FIXTURES = Path(__file__).parent / "fixtures" / "genvce"
CONTRACT_PATH = Path(__file__).resolve().parents[2] / "data" / "sources" / "GENVCE.yaml"
REGISTRIES = load_registries()
CONTRACT = load_contract(CONTRACT_PATH)

# what the fixture files produce
FIXTURE_ROWS, FIXTURE_UNITS, FIXTURE_OBSERVATIONS, FIXTURE_SITES = 32, 32, 233, 15


@pytest.fixture(scope="module")
def loaded():
    return genvce.load(FIXTURES)


@pytest.fixture(scope="module")
def bundle(loaded):
    contract = CONTRACT.model_copy(update={"expected": Expected(
        units=FIXTURE_UNITS, observations=FIXTURE_OBSERVATIONS, sites=FIXTURE_SITES)})
    return run_contract(contract, REGISTRIES, loaded.rows)


def by(loaded, **match) -> list[dict]:
    def hit(row):
        return all(_dig(row, key) == value for key, value in match.items())
    return [row for row in loaded.rows if hit(row)]


def _dig(row, dotted):
    value = row
    for part in dotted.split("__"):
        value = value.get(part) if isinstance(value, dict) else None
    return value


def extraction(trials, issue_period="2013/2014", topic="cereales-invierno", **meta):
    return {
        "metadata": {"source": "GENVCE", "issue_number": 0, "issue_period": issue_period,
                     "article_title": "GENVCE Informe", "article_author": "GENVCE Consortium",
                     "article_topic": topic, "year": int(issue_period[-4:]), "page_in_issue": None,
                     "extraction_model": "m", "extraction_date": "2026-06-15", "confidence": "high", **meta},
        "variety_trials": trials,
    }


def trial(**over):
    row = {"crop": "Cebada", "crop_scientific": "Hordeum vulgare", "variety": "ALFA", "agroclimatic_zone": None,
           "year": 2014, "yield_kg_ha": 5000.0, "yield_relative_pct": 100.0, "quality_params": None,
           "disease_scores": None, "agronomic_traits": None, "yield_notes": None, "special_status": None,
           "irrigation_regime": None, "trial_location": None, "page_in_issue": 5, "table_number": 4,
           "confidence": "high"}
    row.update(over)
    return row


def rows(trials, **kw):
    log = genvce.WarningLog()
    out = genvce.rows_from_extraction(extraction(trials, **kw), "f.json", log)
    return out, {w.code: w for w in log.result()}


# ═════════════════════════════════════════════════════════════════════════════
# the adapter: shape and refusals
# ═════════════════════════════════════════════════════════════════════════════

def test_a_row_carries_the_document_the_table_the_label_and_what_was_extracted():
    (row,), _ = rows([trial(quality_params={"humedad_pct": 12.5}, special_status="testigo")])
    assert row["doc"] == {"title": "GENVCE Informe", "issue": "2013/2014", "year": 2014, "topic": "cereales-invierno"}
    assert row["table"] == {"number": "4", "page": 5}
    assert (row["crop"], row["variety"], row["season"]) == ("Cebada", "ALFA", "2014")
    assert row["quality_params"] == {"humedad_pct": 12.5} and "disease_scores" not in row
    assert row["special_status"] == "testigo" and row["yield_kg_ha"] == 5000.0


def test_a_trial_location_is_refused_because_genvce_prints_none():
    with pytest.raises(AdapterError, match="no trial location"):
        rows([trial(trial_location="Valladolid")])


def test_an_unknown_extraction_field_is_refused_not_dropped():
    with pytest.raises(AdapterError, match="unknown extraction field"):
        rows([{**trial(), "soil_ph": 7.1}])


def test_a_crop_label_that_contradicts_its_species_is_refused():
    with pytest.raises(AdapterError, match="species"):
        rows([trial(crop="Cebada", crop_scientific="Zea mays")])
    with pytest.raises(AdapterError, match="not one the adapter knows"):
        rows([trial(crop="Avena", crop_scientific="Avena sativa")])


def test_a_file_that_is_not_an_extraction_is_refused():
    with pytest.raises(AdapterError, match="no metadata"):
        genvce.rows_from_extraction({"variety_trials": []}, "f.json", genvce.WarningLog())
    with pytest.raises(AdapterError, match="metadata lacks"):
        genvce.rows_from_extraction({"metadata": {"source": "GENVCE"}, "variety_trials": []}, "f.json", genvce.WarningLog())


def test_table_numbers_are_text_and_a_tabla_prefix_is_not_part_of_them():
    out, _ = rows([trial(table_number="Tabla 7"), trial(table_number=9), trial(table_number="5")])
    assert [r["table"]["number"] for r in out] == ["7", "9", "5"]
    with pytest.raises(AdapterError, match="not a table number"):
        rows([trial(table_number=True)])


def test_none_groups_and_empty_notes_are_dropped_and_blank_text_is_missing():
    (row,), _ = rows([trial(quality_params={}, yield_notes={}, variety="  OMEGA ", special_status=" ")])
    assert "quality_params" not in row and "yield_notes" not in row
    assert row["variety"] == "OMEGA" and row["special_status"] is None


# ═════════════════════════════════════════════════════════════════════════════
# the zone: the observed group label, never a city
# ═════════════════════════════════════════════════════════════════════════════

def test_the_zone_is_the_label_the_extraction_printed_and_none_without_one():
    out, _ = rows([trial(agroclimatic_zone="Zona Fría Semiárida"), trial(agroclimatic_zone=" "), trial()])
    assert [r["zone"] for r in out] == ["Zona Fría Semiárida", None, None]


def test_the_tables_own_label_wins_over_the_extractions_relabel_and_is_reported():
    (row,), warnings = rows([trial(agroclimatic_zone="Zona Subhúmeda Interior",
                                   yield_notes={"zone": "Norte", "year_range": "2019-2020"})])
    assert row["zone"] == "Norte" and row["season"] == "2019-2020"
    assert "yield_notes" not in row  # consumed: the contract never sees a second zone
    assert warnings["zone_label_from_table"].count == 1


def test_a_multi_year_period_is_the_season_a_single_report_year_otherwise():
    out, _ = rows([trial(yield_notes={"periodo": "media 2010-2012"}), trial(), trial(year=2013)])
    assert [r["season"] for r in out] == ["media 2010-2012", "2014", "2013"]
    with pytest.raises(AdapterError, match="both year_range and periodo"):
        rows([trial(yield_notes={"periodo": "x", "year_range": "y"})])


def test_no_row_of_the_fixtures_lands_on_a_reference_city(loaded):
    labels = {row["zone"] for row in loaded.rows}
    assert not labels & {"Valladolid", "Lleida", "Córdoba", "Lugo"}


# ═════════════════════════════════════════════════════════════════════════════
# irrigation: only what a label states
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize(("zone", "stated"), [
    ("Secanos áridos y semiáridos fríos", "secano"),
    ("Secano", "secano"),
    ("Regadíos fríos", "regadío"),
    ("Regadío", "regadío"),
    ("Secanos y regadíos templados", None),  # both: the label mixes regimes
    ("Zona Fría Semiárida", None),
    ("Nacional", None),
    (None, None),
])
def test_irrigation_is_what_the_zone_label_states(zone, stated):
    (row,), _ = rows([trial(agroclimatic_zone=zone)])
    assert row["irrigation"] == stated


@pytest.mark.parametrize(("zone", "blocked", "klass"), [
    ("Secanos y regadíos templados", "group label mixes rainfed and irrigated", None),
    ("Rendimiento alto", "group label is a yield stratum", "yield_stratum_high"),
    ("Rendimiento Medio", "group label is a yield stratum", "yield_stratum_medium"),
    ("Productividad Baja", "group label is a yield stratum", "yield_stratum_low"),
    ("Secanos húmedos y de alto potencial fríos", None, "rainfed_humid_high_potential"),
    ("Secanos áridos y semiáridos fríos y templados", None, "rainfed_arid_semiarid"),
    ("Secanos templados", None, None),
    ("Zona Templada", None, None),
    (None, None, None),
])
def test_mixed_and_yield_stratum_groups_are_marked_and_the_class_is_what_the_label_states(zone, blocked, klass):
    (row,), _ = rows([trial(agroclimatic_zone=zone)])
    assert row["regime_not_derivable"] == blocked
    assert row["productivity_class"] == klass


def test_an_assumed_regime_is_left_out_and_counted():
    out, warnings = rows([trial(irrigation_regime="secano"), trial(irrigation_regime="regadío"),
                          trial(irrigation_regime=None)])
    assert [r["irrigation"] for r in out] == [None, None, None]
    assert warnings["irrigation_not_stated_by_source"].count == 2


def test_a_regime_that_contradicts_its_label_gives_way_to_the_label():
    (row,), warnings = rows([trial(agroclimatic_zone="Regadíos", irrigation_regime="secano")])
    assert row["irrigation"] == "regadío"
    assert warnings["irrigation_conflicts_with_zone_label"].count == 1


def test_rainfed_maize_at_irrigated_yields_is_no_longer_rainfed(loaded):
    """The 2019/20 maize tables carry 'secano' on every row at 14-19 t/ha; the 15 maize reports never
    mention irrigation, so the adapter does not pass the extraction's assumption on."""
    maize = [r for r in by(loaded, doc__issue="2019/2020")]
    assert maize and all(r["irrigation"] is None for r in maize)
    assert max(r["yield_kg_ha"] for r in maize) > 14000


def test_the_label_states_the_regime_in_the_strata_reports(loaded):
    first = by(loaded, doc__issue="2005/2006")
    assert [(r["zone"], r["irrigation"]) for r in first if r["zone"]] == [
        ("Secanos áridos y semiáridos fríos", "secano"), ("Regadíos", "regadío"),
        ("Rendimiento bajo", None), ("Secanos y regadíos templados", None)]


# ═════════════════════════════════════════════════════════════════════════════
# disease scales, text numbers, scores outside their scale
# ═════════════════════════════════════════════════════════════════════════════

BARE = {"oidio": 3, "roya_parda": 1, "helmintosporiosis": 5, "rincosporiosis": 2}


def test_bare_disease_keys_are_0_9_scores_in_the_campaigns_whose_tables_print_that():
    (row,), warnings = rows([trial(disease_scores=dict(BARE))], issue_period="2005/2006")
    assert row["disease_scores"] == {"oidio_escala_0_9": 3, "roya_parda_escala_0_9": 1,
                                     "helmintosporiosis_escala_0_9": 5, "rincosporiosis_escala_0_9": 2}
    assert warnings["disease_scale_resolved"].count == 4


def test_bare_disease_keys_stay_bare_where_the_report_prints_a_scale_per_table():
    (row,), warnings = rows([trial(disease_scores=dict(BARE))], issue_period="2023/2024")
    assert row["disease_scores"] == BARE
    assert warnings["disease_scale_unresolved"].count == 4
    assert "disease_scale_resolved" not in warnings


def test_the_resolution_is_by_campaign_never_by_the_size_of_a_value():
    (row,), _ = rows([trial(disease_scores={"oidio": 45, "helmintosporiosis": 2})], issue_period="2023/2024")
    assert set(row["disease_scores"]) == {"oidio", "helmintosporiosis"}


def test_the_2014_rincosporiosis_scale_key_is_the_0_9_score_its_report_prints():
    (row,), _ = rows([trial(disease_scores={"rincosporiosis_escala": 2, "oidio_pct": 40})], issue_period="2013/2014")
    assert row["disease_scores"] == {"rincosporiosis_escala_0_9": 2, "oidio_pct": 40}
    (other,), _ = rows([trial(disease_scores={"rincosporiosis_escala": 2})], issue_period="2022/2023")
    assert "rincosporiosis_escala" in other["disease_scores"]


def test_a_renaming_that_collides_with_an_existing_key_is_refused():
    with pytest.raises(AdapterError, match="after renaming"):
        rows([trial(disease_scores={"oidio": 3, "oidio_escala_0_9": 4})], issue_period="2006/2007")


def test_numbers_printed_as_text_are_numbers_and_dates_stay_text():
    (row,), warnings = rows([trial(quality_params={"floracion_femenina_dias_respecto_P1921": "-2",
                                                   "humedad_pct": "13,5", "fecha_espigado": "17-abr"})])
    assert row["quality_params"] == {"floracion_femenina_dias_respecto_P1921": -2, "humedad_pct": 13.5,
                                     "fecha_espigado": "17-abr"}
    assert warnings["number_text_parsed"].count == 2


def test_a_score_outside_the_scale_its_key_names_is_left_out_and_reported_with_its_value():
    (row,), warnings = rows([trial(disease_scores={"oidio_escala_0_9": 38, "rincosporiosis_escala_0_9": 4,
                                                   "stay_green_0_5": 7})])
    assert row["disease_scores"] == {"oidio_escala_0_9": None, "rincosporiosis_escala_0_9": 4, "stay_green_0_5": None}
    warning = warnings["ordinal_value_out_of_scale"]
    assert warning.count == 2 and any(where.endswith("oidio_escala_0_9=38") for where in warning.where)


def test_the_fixture_rows_show_each_resolution(loaded):
    (first,) = [r for r in by(loaded, doc__issue="2005/2006") if r["zone"] is None]
    assert {"oidio_escala_0_9", "roya_parda_escala_0_9", "helmintosporiosis_escala_0_9",
            "rincosporiosis_escala_0_9"} <= set(first["disease_scores"])
    assert any("helmintosporiosis" in r["disease_scores"] for r in by(loaded, doc__issue="2023/2024"))
    assert all("rincosporiosis_escala_0_9" in r["disease_scores"] for r in by(loaded, doc__issue="2013/2014"))
    pandora = by(loaded, variety="PANDORA")[0]
    assert pandora["disease_scores"]["oidio_escala_0_9"] is None  # 38 under a 0-9 key
    assert by(loaded, variety="ZAPOTEK YG")[0]["quality_params"]["floracion_femenina_dias_respecto_P1921"] == -2


def test_the_adapter_reports_what_it_did(loaded):
    codes = {w.code: w for w in loaded.warnings}
    assert set(codes) == {"disease_scale_resolved", "disease_scale_unresolved", "irrigation_not_stated_by_source",
                          "number_text_parsed", "ordinal_value_out_of_scale", "zone_label_from_table"}
    assert codes["ordinal_value_out_of_scale"].count == 1
    assert codes["zone_label_from_table"].count == 2


# ═════════════════════════════════════════════════════════════════════════════
# organic wheat
# ═════════════════════════════════════════════════════════════════════════════

def test_organic_is_read_from_the_crop_label_and_never_assumed_otherwise():
    out, _ = rows([trial(crop="Trigo blando ecológico de invierno", crop_scientific="Triticum aestivum"),
                   trial(crop="Trigo blando", crop_scientific="Triticum aestivum")])
    assert [r["production_system"] for r in out] == ["ecológico", None]


# ═════════════════════════════════════════════════════════════════════════════
# load(): the files, in a fixed order
# ═════════════════════════════════════════════════════════════════════════════

def test_load_reads_the_files_in_name_order_with_their_fingerprints(loaded):
    names = [path for path, _ in loaded.inputs]
    assert names == sorted(names) and len(names) == 14
    assert all(len(digest) == 64 for _, digest in loaded.inputs)
    assert len(loaded.rows) == FIXTURE_ROWS


def test_load_ignores_the_batch_statistics_file_and_refuses_an_empty_folder(tmp_path):
    (tmp_path / "data/extractions").mkdir(parents=True)
    with pytest.raises(AdapterError, match="no extraction files"):
        genvce.load(tmp_path)
    (tmp_path / "data/extractions/batch_stats.json").write_text('{"total": 1}')
    with pytest.raises(AdapterError, match="no extraction files"):
        genvce.load(tmp_path)


# ═════════════════════════════════════════════════════════════════════════════
# the contract on the fixtures
# ═════════════════════════════════════════════════════════════════════════════

def test_every_row_becomes_one_unit_and_nothing_collapses(bundle):
    assert len(bundle.units) == FIXTURE_UNITS
    assert bundle.report.collapsed_duplicate_units == bundle.report.collapsed_duplicate_observations == 0


def test_the_site_is_the_observed_group_registered_as_an_aggregate_without_coordinates(bundle):
    assert {s.site_kind for s in bundle.sites} == {"aggregate"}
    assert all(s.latitude is None and s.longitude is None for s in bundle.sites)
    assert all(s.site_id.startswith("ES-GENVCE-") for s in bundle.sites)
    by_label = {u.raw_site: u.site_key for u in bundle.units if u.raw_site}
    assert by_label["Norte"] == "ES-GENVCE-NORTE"
    assert by_label["Zona Fría Semiárida y Zona Templada"] == "ES-GENVCE-ZONA-FRIA-SEMIARIDA-Y-ZONA-TEMPLADA"
    assert not bundle.report.unresolved_sites


def test_a_table_without_a_group_has_no_observed_site_says_why_and_sits_on_the_unlabelled_aggregate(bundle):
    unlocated = [u for u in bundle.units if u.raw_site is None]
    assert unlocated and all(u.site_key == "ES-GENVCE-UNLABELLED" for u in unlocated)
    assert all(any(g.field == "raw_site" for g in u.gaps) for u in unlocated)


def test_the_standard_moisture_follows_the_crop_the_reports_state(bundle):
    moisture = {(u.crop_eppo, u.yield_moisture_pct) for u in bundle.units if u.yield_kg_ha is not None}
    assert moisture == {("HORVX", 13.0), ("TRZAX", 13.0), ("ZEAMX", 14.0), ("BRSNN", 9.0)}
    assert {u.purpose for u in bundle.units} == {"grain"}
    assert {u.yield_metric for u in bundle.units if u.yield_kg_ha is not None} == {"grain"}
    assert {u.yield_basis for u in bundle.units if u.yield_kg_ha is not None} == {"standard_moisture"}


def test_the_same_variety_in_two_tables_of_one_report_is_two_units(bundle):
    units = [u for u in bundle.units if u.raw_variety == "DKC6980"]
    assert sorted(u.row_discriminator for u in units) == ["4", "5"]
    assert len({identity.unit_key(u) for u in units}) == 2
    assert len({u.study_key for u in units}) == 2


def test_the_printed_panel_label_is_a_factor_and_the_report_year_the_season(bundle):
    ciclo = [u for u in bundle.units if u.raw_variety in ("HISPANIC", "CLAMOR")]
    assert {u.factor_levels[0].level for u in ciclo} == {"Cebada de ciclo largo", "Cebada de ciclo corto"}
    assert {u.raw_season for u in bundle.units if u.raw_variety == "HISPANIC"} == {"2006"}


def test_a_multi_year_period_keeps_its_text_and_has_no_single_year(bundle):
    (period,) = {u.raw_season for u in bundle.units if u.raw_season and "-" in u.raw_season}
    assert period == "2019-2020"
    units = [u for u in bundle.units if u.raw_season == period]
    assert all(u.year is None and any(g.field == "year" for g in u.gaps) for u in units)


def test_organic_wheat_is_organic(bundle):
    organic = [u for u in bundle.units if u.crop_eppo == "TRZAX"]
    assert organic and {u.raw_production_system for u in organic} == {"ecológico"}
    assert {u.production_system for u in organic} == {"organic"}


def test_check_varieties_named_in_a_key_become_the_observations_qualifier(bundle):
    qualifiers = {o.qualifier for o in bundle.observations if o.variable_id == "silking_days_vs_check"}
    assert "P1921" in qualifiers
    counts = {o.qualifier for o in bundle.observations if o.variable_id == "trial_count_in_mean"}
    assert {"2016-2017", "conjunto"} <= counts


def test_a_bare_silking_offset_is_the_offset_to_a_check_without_a_named_one(bundle):
    bare = [o for o in bundle.observations if o.raw_key == "agronomic_traits.floracion_femenina_dias"]
    assert bare and all(o.variable_id == "silking_days_vs_check" and o.qualifier is None for o in bare)


def test_disease_scores_resolved_by_campaign_reach_their_variables(bundle):
    variables = {o.variable_id for o in bundle.observations}
    assert {"powdery_mildew_score_0_9", "brown_rust_score_0_9", "helminthosporium_score_0_9",
            "scald_score_0_9"} <= variables


def test_values_the_adapter_leaves_out_are_missing_observations_not_invented_ones(bundle):
    pandora = next(u for u in bundle.units if u.raw_variety == "PANDORA")
    keys = [o.raw_key for o in bundle.observations if o.unit_key == identity.unit_key(pandora)]
    assert "disease_scores.oidio_escala_0_9" not in keys
    assert bundle.report.missing["disease_scores.oidio_escala_0_9"] >= 1


def test_notes_copied_from_other_tables_are_ignored_with_their_reasons():
    reasons = {entry.field: entry.reason for entry in CONTRACT.ignore}
    for field in ("yield_notes.produccion_tabla9_kg_ha", "yield_notes.produccion_conjunta_kg_ha",
                  "yield_notes.tercil_superior", "special_status", "confidence"):
        assert reasons[field].strip()


def test_an_unmapped_extraction_key_stops_the_build(loaded):
    broken = copy.deepcopy(list(loaded.rows))
    broken[0].setdefault("quality_params", {})["contenido_en_ceniza_pct"] = 1.2
    contract = CONTRACT.model_copy(update={"expected": Expected(units=1, observations=0, sites=0)})
    with pytest.raises(UnmappedFieldError, match="contenido_en_ceniza_pct"):
        run_contract(contract, REGISTRIES, broken)


def test_a_group_that_resolves_to_a_field_site_is_refused_never_disguised(loaded):
    broken = copy.deepcopy(list(loaded.rows[:1]))
    broken[0]["zone"] = "Valladolid"
    contract = CONTRACT.model_copy(update={"expected": Expected(units=1, observations=0, sites=0)})
    with pytest.raises(ContractDataError, match="aggregate pattern"):
        run_contract(contract, REGISTRIES, broken)


# ═════════════════════════════════════════════════════════════════════════════
# the contract itself
# ═════════════════════════════════════════════════════════════════════════════

RAW_CROP_LABELS = (
    "Maíz", "Maíz para grano", "Maíz ciclo 400-500", "Maíz ciclo 600", "Maíz ciclo 700", "Cebada",
    "Cebada de ciclo largo", "Cebada de ciclo corto", "Cebada de invierno", "Cebada de primavera", "Trigo blando",
    "Trigo blando de invierno", "Trigo blando ecológico de invierno", "Trigo blando ecológico de primavera",
    "Colza de otoño",
)


@pytest.mark.parametrize("label", RAW_CROP_LABELS)
def test_every_crop_label_of_the_extraction_resolves_in_the_crops_registry(label):
    assert REGISTRIES.crop(label) is not None


def test_the_contract_states_its_raw_layer_and_the_pinned_commit():
    assert CONTRACT.adapter == "app.kg.adapters.genvce"
    assert CONTRACT.raw.repo == "nkz-data-sources" and "extractions" in CONTRACT.raw.paths[0]
    assert "@798a73db5436eccf0ac8d4e40c7d66949195566a" in CONTRACT.raw.extraction_version


def test_the_contract_quotes_the_source_for_purpose_metric_basis_and_moisture():
    unit = CONTRACT.unit
    assert set(unit.purpose.by_crop) == {"HORVX", "TRZAX", "ZEAMX", "BRSNN"}
    assert {c: f.default for c, f in unit.yield_.moisture_pct.by_crop.items()} == {
        "HORVX": 13, "TRZAX": 13, "ZEAMX": 14, "BRSNN": 9}
    for fixed in (*unit.yield_.moisture_pct.by_crop.values(), *unit.yield_.metric.by_crop.values(),
                  *unit.purpose.by_crop.values()):
        assert "Informe GENVCE" in fixed.justification


def test_every_registered_genvce_site_is_an_aggregate():
    owned = [s for s in REGISTRIES.sites if s.sources == ("GENVCE",)]
    assert len(owned) == 45 and {s.site_kind for s in owned} == {"aggregate"}


# ═════════════════════════════════════════════════════════════════════════════
# the same row printed in two tables of one document
# ═════════════════════════════════════════════════════════════════════════════

def _row(variety, table, page, yield_kg=1000.0, doc_topic="t"):
    return {"doc": {"title": "D", "issue": "2023/2024", "year": 2023, "topic": doc_topic},
            "table": {"number": table, "page": page}, "crop": "Maíz", "variety": variety, "zone": "Z",
            "season": "2023", "yield_kg_ha": yield_kg}


def test_a_row_repeated_in_a_higher_table_is_dropped_and_the_table_kept_as_alias():
    log = genvce.WarningLog()
    rows = [_row("A", "17", 14), _row("B", "14", 12), _row("A", "14", 12), _row("C", "17", 14)]
    kept = genvce.drop_repeated_rows(rows, log)
    assert [(r["variety"], r["table"]["number"]) for r in kept] == [("B", "14"), ("A", "14"), ("C", "17")]
    assert kept[1]["table"]["aliases"] == "17 (page 14)"
    assert "aliases" not in kept[0]["table"]
    assert log.result()[0].code == "repeated_table_row_dropped" and log.result()[0].count == 1


def test_rows_whose_values_differ_or_of_another_document_are_never_merged():
    log = genvce.WarningLog()
    rows = [_row("A", "14", 12), _row("A", "17", 14, yield_kg=1001.0), _row("A", "20", 1, doc_topic="other"),
            _row("A", "9", 1)]
    kept = genvce.drop_repeated_rows(rows, log)
    assert sorted(r["table"]["number"] for r in kept) == ["17", "20", "9"]  # only the true repeat (14) went


def test_the_lowest_table_number_is_numeric_not_alphabetical_and_the_same_table_is_not_an_alias():
    log = genvce.WarningLog()
    rows = [_row("A", "9", 5), _row("A", "14", 12), _row("A", "14", 12)]
    kept = genvce.drop_repeated_rows(rows, log)
    assert len(kept) == 1 and kept[0]["table"]["number"] == "9"
    assert kept[0]["table"]["aliases"] == "14 (page 12)"
    same_table = genvce.drop_repeated_rows([_row("A", "14", 12), _row("A", "14", 12)], genvce.WarningLog())
    assert len(same_table) == 2  # repetition inside one table is the engine's business, not an alias


# ═════════════════════════════════════════════════════════════════════════════
# the whole raw data (only where the private repository is available)
# ═════════════════════════════════════════════════════════════════════════════

RAW_REPO = os.environ.get("NKZ_DATA_SOURCES_DIR", "")


@pytest.mark.skipif(not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to the raw-data repository to run")
def test_the_whole_extraction_builds_and_matches_the_contracts_expected_counts():
    result = genvce.load(Path(RAW_REPO) / "genvce")
    assert len(result.rows) == 3855  # 3862 extracted, 7 repeated rows of a lower table dropped
    dropped = [w for w in result.warnings if w.code == "repeated_table_row_dropped"]
    assert [w.count for w in dropped] == [7]
    built = run_contract(CONTRACT, REGISTRIES, result.rows)
    assert (len(built.units), len(built.observations), len(built.sites)) == (
        CONTRACT.expected.units, CONTRACT.expected.observations, CONTRACT.expected.sites)
    assert not built.report.unresolved_sites and not built.report.unresolved_vocab
