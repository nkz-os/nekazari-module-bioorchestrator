"""GENVCE zone definitions: registry validity, classification at the published limits, unit zone keys,
parcel zone allow/deny/undecided. Pure, no Neo4j."""
from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from app.kg.zone_definitions import (
    DEFAULT_ZONE_DEFINITIONS_PATH,
    ZONE_MATCH_BASIS,
    ZoneDefinitionError,
    default_zone_definitions,
    fold,
    load_zone_definitions,
    zone_key,
)

CEREAL_TITLE = "GENVCE Cereales de Invierno (trigo, cebada, avena, centeno, triticale)"
ANNUAL_TITLE = "GENVCE Informe Anual — Campaña 2017/2018"
OILSEED_TITLE = "GENVCE Oleaginosas (girasol, colza) — Campaña 2013/2014"
MAIZE_TITLE = "GENVCE Maíz — Campaña 2020/2021"


@pytest.fixture(scope="module")
def zones():
    return default_zone_definitions()


def _key(zones, title, issue, crop, site, table="1"):
    return zones.zone_key_for(source_id="GENVCE", title=title, issue=issue, crop_eppo=crop,
                              raw_site=site, table=table)


def test_registry_loads_and_every_definition_is_cited(zones):
    assert zones.source_id == "GENVCE"
    assert len(zones.definitions) == 21
    for d in zones.definitions:
        assert d.citation.pdf_page > 0 and d.citation.section and d.citation.table and d.citation.document


def test_published_thresholds_of_the_2023_24_cereal_report(zones):
    d = zones.by_id("genvce-winter-cereals-2023-24")
    # cold < 11, temperate 11-13 inclusive, warm > 13 (section 2.1.3)
    assert [d.classify("temperature", t) for t in (10.99, 11.0, 13.0, 13.01)] == [
        "cold", "temperate", "temperate", "warm"]
    # semiarid <= 500, subhumid (500, 700), humid > 700; exactly 700 is in no printed class
    assert [d.classify("rainfall", r) for r in (500.0, 500.01, 699.9, 700.0, 700.01)] == [
        "semiarid", "subhumid", "subhumid", None, "humid"]
    assert d.classify("temperature", None) is None


def test_rapeseed_2010_17_thresholds(zones):
    d = zones.by_id("genvce-rapeseed-2013-14")
    assert [d.classify("temperature", t) for t in (11.9, 12.0, 12.1)] == ["cold", None, "temperate"]
    assert [d.classify("rainfall", r) for r in (600.0, 600.1)] == ["arid_semiarid", "humid"]


def test_no_definition_for_non_climatic_or_unpublished(zones):
    # 2005/06-2010/11 winter cereals (yield criterion), maize (no thresholds), rapeseed 2017/18+
    for issue in ("2005/2006", "2009/2010", "2010/2011"):
        assert zones.definition_for("GENVCE", "GENVCE Informe Anual — Campaña X", issue) is None
    assert zones.definition_for("GENVCE", MAIZE_TITLE, "2020/2021") is None
    assert zones.definition_for("GENVCE", "GENVCE Oleaginosas (girasol, colza)", "2017/2018") is None
    assert zones.definition_for("CREA", CEREAL_TITLE, "2023/2024") is None


def test_unit_zone_key_only_where_label_states_the_classes(zones):
    k = _key(zones, CEREAL_TITLE, "2023/2024", "HORVX", "Zona Fría")
    assert k == zone_key("genvce-winter-cereals-2023-24", "Zona Fría")
    # case/accents folded, exact otherwise
    assert _key(zones, CEREAL_TITLE, "2023/2024", "HORVX", "  zona   FRIA ") is None  # accents are not folded away
    assert _key(zones, CEREAL_TITLE, "2023/2024", "HORVX", "zona fría") == k
    # labels that do not state the classes: no key (country level)
    for label in ("General", "Zona Subhúmeda Interior", "Zona Húmeda Atlántica", "Nacional",
                  "Conjunto Frías, Templadas y Cálidas", "Zona Fría Semiárida y Zona Templada"):
        assert _key(zones, CEREAL_TITLE, "2022/2023", "HORVX", label) is None
    # a crop the definition does not cover (maize inside a cereal document)
    assert _key(zones, CEREAL_TITLE, "2023/2024", "ZEAMX", "Zona Fría") is None
    # another document family
    assert _key(zones, MAIZE_TITLE, "2020/2021", "ZEAMX", "Zona Fría Semiárida") is None
    # annual-report title is a cereal report too
    assert _key(zones, ANNUAL_TITLE, "2017/2018", "TRZAX", "Zonas frías") is not None
    assert _key(zones, CEREAL_TITLE, "2023/2024", "HORVX", None) is None


def test_table_exception_of_the_prefgenvce_table(zones):
    assert _key(zones, CEREAL_TITLE, "2012/2013", "HORVX", "Zona Fría Semiárida", table="15") is not None
    assert _key(zones, CEREAL_TITLE, "2012/2013", "HORVX", "Zona Fría Semiárida", table="20") is None


def _parcel(zones, temp, rain, regime=None):
    return zones.classify_parcel(april_tas_c=temp, annual_rain_mm=rain, regime=regime)


def test_parcel_zone_allow_deny(zones):
    cold = zone_key("genvce-winter-cereals-2023-24", "Zona Fría")
    warm = zone_key("genvce-winter-cereals-2023-24", "Zona Cálida")
    p = _parcel(zones, 14.0, 350.0)
    assert warm in p.allow and cold in p.deny and p.classes["genvce-winter-cereals-2023-24"] == {
        "temperature": "warm", "rainfall": "semiarid"}
    # a label that states both classes needs both
    fria_semi = zone_key("genvce-winter-cereals-2022-23", "Zona Fría Semiárida")
    assert fria_semi in _parcel(zones, 9.0, 450.0).allow
    assert fria_semi in _parcel(zones, 9.0, 800.0).deny           # cold but humid
    assert fria_semi in _parcel(zones, 9.0, 700.0).undecided      # exactly on a gap of the printed scale
    assert fria_semi in _parcel(zones, 9.0, None).undecided
    # the two sets never overlap
    p = _parcel(zones, 12.0, 700.0)
    assert not (p.allow & p.deny) and not (p.allow & p.undecided) and not (p.deny & p.undecided)


def test_regime_matters_only_for_labels_that_state_one(zones):
    sec = zone_key("genvce-rapeseed-2013-14", "Secanos áridos y semiáridos fríos")
    reg = zone_key("genvce-rapeseed-2013-14", "Regadíos fríos")
    p = _parcel(zones, 10.0, 450.0, regime="secano")
    assert sec in p.allow and reg in p.deny
    p = _parcel(zones, 10.0, 450.0, regime="regadio")
    assert sec in p.deny and reg in p.allow
    p = _parcel(zones, 10.0, 450.0, regime=None)
    assert sec in p.undecided and reg in p.undecided
    cereal_cold = zone_key("genvce-winter-cereals-2023-24", "Zona Fría")
    assert cereal_cold in _parcel(zones, 10.0, 450.0, regime=None).allow


def test_describe_keys_cites_the_definition(zones):
    info = zones.describe_keys([zone_key("genvce-winter-cereals-2023-24", "Zona Fría")])
    assert info == [{
        "definition_id": "genvce-winter-cereals-2023-24", "zone_label": "Zona Fría",
        "citation": "GENVCE report, winter cereals, campaign 2023-2024, section 2.1.3 Zonas de experimentación, PDF page 3",
    }]
    assert ZONE_MATCH_BASIS == "chelsa_1981_2010_climatology"


# ── registry guards ──────────────────────────────────────────────────────────

def _edited(tmp_path: Path, edit) -> Path:
    raw = yaml.safe_load(DEFAULT_ZONE_DEFINITIONS_PATH.read_text(encoding="utf-8"))
    raw = copy.deepcopy(raw)
    edit(raw)
    out = tmp_path / "defs.yaml"
    out.write_text(yaml.safe_dump(raw, allow_unicode=True), encoding="utf-8")
    return out


def test_label_must_state_the_class_it_constrains(tmp_path):
    def edit(raw):
        labels = [dict(x) for x in raw["definitions"][2]["labels"]]
        labels[0] = {"label": "Zona Fría", "temperature": ["warm"]}
        raw["definitions"][2]["labels"] = labels
    with pytest.raises(ZoneDefinitionError, match="does not state"):
        load_zone_definitions(_edited(tmp_path, edit))


def test_label_may_not_hide_a_class_it_states(tmp_path):
    def edit(raw):
        labels = [dict(x) for x in raw["definitions"][2]["labels"]]  # the YAML aliases share one list
        i = next(n for n, x in enumerate(labels) if x["label"] == "Zona Fría Semiárida")
        labels[i] = {"label": "Zona Fría Semiárida", "temperature": ["cold"]}
        raw["definitions"][2]["labels"] = labels
    with pytest.raises(ZoneDefinitionError, match="does not constrain"):
        load_zone_definitions(_edited(tmp_path, edit))


def test_label_must_state_every_required_axis(tmp_path):
    def edit(raw):
        raw["definitions"][0]["labels"] = [{"label": "Zona Fría", "temperature": ["cold"]}]  # A needs T and R
    with pytest.raises(ZoneDefinitionError, match="required axis"):
        load_zone_definitions(_edited(tmp_path, edit))


def test_duplicate_campaign_of_a_family_is_refused(tmp_path):
    def edit(raw):
        raw["definitions"][1]["campaigns"] = list(raw["definitions"][0]["campaigns"])
    with pytest.raises(ZoneDefinitionError, match="two definitions"):
        load_zone_definitions(_edited(tmp_path, edit))


def test_fold_is_exact_after_nfc_case_and_whitespace():
    assert fold("  Zona   FRÍA ") == "zona fría"


def test_the_2016_17_report_zones_cereals_with_the_labels_it_prints(zones):
    d = zones.by_id("genvce-winter-cereals-2016-17")
    assert d.classify("rainfall", 600.0) == "subhumid"  # rainfall classes: PDF page 3 of the report
    title = "GENVCE Cereales de Invierno (trigo, cebada, avena, centeno, triticale) — Campaña 2016/2017"
    assert _key(zones, title, "2016/2017", "TRZAX", "Zonas frías") == zone_key(d.id, "Zonas frías")
    assert _key(zones, title, "2016/2017", "HORVX", "General") is None


def test_combined_cold_and_temperate_labels_are_zoned_in_either_order(zones):
    for label in ("Zonas frías y templadas", "Zonas templadas y frías"):
        assert _key(zones, CEREAL_TITLE, "2019/2020", "TRZAX", label) is not None
        assert _key(zones, CEREAL_TITLE, "2019/2020", "TRZAX", label.lower()) is not None


def test_organic_wheat_reports_get_no_cereal_zone_definition(zones):
    # the organic reports print the same temperature and rainfall classes (section 2.1.3), but their extracted zone
    # labels have not been checked line by line against the printed ones, so the cereal definitions do not apply
    for issue, title in (("2019/2020", "GENVCE Trigo Ecológico — Campaña 2019/2020"),
                         ("2020/2021", "GENVCE Trigo Ecológico — Campaña 2020/2021")):
        assert zones.definition_for("GENVCE", title, issue) is None
        assert _key(zones, title, issue, "TRZAX", "Zona Fría") is None
