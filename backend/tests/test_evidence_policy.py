"""Evidence policy classifiers (pure Python, no Neo4j).

Fixture values are the shapes found in the production graph (read-only inventory,
2026-10-04): ``source_id``/``dataSource`` pairs, ``yieldMetric`` values,
``qualityParams`` JSON strings, ``aggregationScope`` values and site names.
"""
from __future__ import annotations

import json

import pytest

from app.graph import evidence_policy as ep
from app.ingestion.normalization_registry import _SOURCE_ALIASES, canonical_source_id
from app.ingestion.trial_site_geo import AGGREGATE_PATTERNS

# ── (a) source policy ────────────────────────────────────────────────────────

@pytest.mark.parametrize(("source_id", "data_source"), [
    ("BSL", None),
    ("BSL", "bsa bundessortenamt"),
    ("BSL", "bsa"),
    (None, "bsa"),                       # variant only in dataSource
    (None, "  BSA Bundessortenamt "),    # case / whitespace
    ("bsl", None),
])
def test_bsl_variants_are_excluded_numeric_sources(source_id, data_source):
    assert ep.is_excluded_source(source_id, data_source)


@pytest.mark.parametrize(("source_id", "data_source"), [
    ("GENVCE", None),
    ("NAVARRA-AGRARIA", None),
    ("LEGACY", "legacy"),
    ("LEGACY", "navarra_agraria"),
    ("AHDB", "ahdb"),
    ("LFL-BAYERN", "lfl_bayern"),
    ("CTIFL", "ctifl"),
    (None, None),
])
def test_measured_sources_are_not_excluded(source_id, data_source):
    assert not ep.is_excluded_source(source_id, data_source)


def test_every_excluded_variant_canonicalises_to_bsl():
    assert ep.NUMERIC_YIELD_EXCLUDED_SOURCES >= {"bsl", "bsa", "bsa bundessortenamt"}
    for variant in ep.NUMERIC_YIELD_EXCLUDED_SOURCES:
        assert variant == variant.strip().lower()
        assert canonical_source_id(variant) == "BSL"


def test_every_bsl_alias_is_an_excluded_source():
    """Reverse direction: a new BSL alias in the registry cannot slip past the policy."""
    bsl_aliases = {alias for alias, canon in _SOURCE_ALIASES.items() if canon == "BSL"} | {"bsl"}
    assert {"bsa", "bsa bundessortenamt", "bundessortenamt", "bsl"} <= bsl_aliases
    assert bsl_aliases <= ep.NUMERIC_YIELD_EXCLUDED_SOURCES
    for alias in bsl_aliases:
        assert ep.is_excluded_source(alias, None) and ep.is_excluded_source(None, alias)


# ── (b) purpose ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize(("metric", "expected"), [
    (None, "unknown"), ("", "unknown"),
    ("fruit_weight_kg_ha", "unknown"),           # olive, the only metric in production
    ("kernel_kg_ha", "unknown"),
    ("grain_kg_ha", "grain"), ("seed_kg_ha", "grain"),
    ("fresh_fruit_kg_ha", "fresh"), ("fresh_grape_kg_ha", "fresh"), ("fresh grape", "fresh"),
    ("Fresh_Fruit", "fresh"),
    ("forage_dm_kg_ha", "forage"), ("silage_kg_ha", "forage"),
    ("fresh_matter_kg_ha", "forage"), ("green_matter_kg_ha", "forage"),
])
def test_yield_purpose_from_metric(metric, expected):
    assert ep.yield_purpose(metric, None) == expected


# Forage-analysis keys seen on silage maize, foxtail millet and alfalfa trials.
_FORAGE_QP = [
    '{"dry_matter_pct": 34.8, "starch_pct": 31.0, "ndf_pct": 40.1}',
    '{"materia_seca_pct": 32.78, "almidon_pct_sms": 34.86, "fibra_neutro_detergente_pct": 36.56}',
    '{"ms_pct": 17.1, "cenizas_pct": 12.4, "pb_pct": 7.4, "fb_pct": 35.3, "fnd_pct": 66.6}',
    '{"aporte_mazorca_pct": 64.1, "almidon_pct": 27.0}',
    '{"digestibilidad_materia_organica_pct": 73.2, "concentracion_energetica_UFL_kg_ms": 0.91}',
    '{"NDF_pct": 41.28, "digestibility_pct": 66.72, "energy_UFL_kg_ms": 0.8}',
    '{"ear_contribution_pct": 53.7, "crude_protein_pct": 6.8}',
    {"fnd_pct_s_ms": 41.28, "dmo_pct": 66.72},  # already-decoded mapping
]


@pytest.mark.parametrize("qp", _FORAGE_QP)
def test_forage_quality_params_make_the_purpose_forage(qp):
    assert ep.has_forage_indicators(qp)
    assert ep.yield_purpose(None, qp) == "forage"
    assert ep.yield_purpose("grain_kg_ha", qp) == "forage"  # forage evidence wins


# Grain / fruit / quality keys that must NOT be read as forage.
_GRAIN_QP = [
    None,
    "",
    '{"humidity_pct": 13.8, "thousand_grain_weight_g": 44.0, "specific_weight_kg_hl": 69.6}',
    '{"humedad_grano_pct": 16.5, "peso_hectolitrico_kg_hl": 71.8, "proteina_pct": 10.7}',
    '{"protein_pct": 10.6, "proteina_bruta_pct": 7.68, "harvest_moisture_pct": 25.5}',
    '{"aehrenschieben": 5, "reife": 5, "tausendkornmasse": 6, "rohprotein_pct": 4}',
    '{"yield_metric": "kernel_kg_ha", "aphid_severity_max_2018": 0.3}',
    '{"olive_fruit_kg_ha": 12150, "yield_metric_note": "cumulative/mean annual oil in quality_params only"}',
    '{"brix": 6.53, "total_yield_t_ha": 51.79, "commercial_yield_pct": 91.45}',
    # Starch and dry matter alone are tuber/fruit quality traits, not forage markers.
    '{"starch_pct": 18.0, "dry_matter_pct": 22.0}',
    '{"note": "ndf_pct mentioned only in a value"}',
    {"humidity_pct": 14.0},
]


@pytest.mark.parametrize("qp", _GRAIN_QP)
def test_grain_quality_params_are_not_forage(qp):
    assert not ep.has_forage_indicators(qp)
    assert ep.yield_purpose(None, qp) == "unknown"


def test_forage_keys_are_lowercase_and_quoted_matching_needs_json_keys():
    for key in ep.FORAGE_QUALITY_KEYS:
        assert key == key.lower()
    assert not ep.has_forage_indicators('{"xndf_pct": 1.0, "ndf_pct_note": "n/a"}')
    assert ep.has_forage_indicators(json.dumps({"NDF_pct": 1.0}))


@pytest.mark.parametrize(("purpose", "main", "forage"), [
    ("grain", True, False), ("unknown", True, False), ("fresh", True, False),
    ("forage", False, True),
])
def test_purpose_modes(purpose, main, forage):
    assert ep.in_purpose_mode(purpose, "main") is main
    assert ep.in_purpose_mode(purpose, "forage") is forage


def test_unknown_purpose_mode_is_rejected():
    with pytest.raises(ValueError):
        ep.in_purpose_mode("grain", "fresh")
    with pytest.raises(ValueError):
        ep.cypher_numeric_yield_eligible("vt", "grain")


@pytest.mark.parametrize(("code", "family"), [
    ("TRZAX", "grain"), ("ZEAMX", "grain"), ("ZEAMA", "grain"), ("BRSNN", "grain"),
    ("PIBSX", "grain"), (" horvx ", "grain"),
    ("MEDSA", "forage"),
    ("LYPES", "fresh"), ("VITVI", "fresh"), ("PRNAV", "fresh"), ("MABSD", "fresh"),
    ("PRNDU", "other"), ("OLVEU", "other"), ("SOLTU", "other"), ("XXXXX", "other"),
    ("", "other"), (None, "other"),
])
def test_crop_family(code, family):
    assert ep.crop_family(code) == family


def test_crop_families_are_disjoint_uppercase_codes():
    seen: set[str] = set()
    for codes in ep.CROP_FAMILIES.values():
        assert not (codes & seen)
        seen |= codes
        assert all(c == c.upper() and len(c) == 5 for c in codes)


# ── numeric-yield eligibility (a + b) ────────────────────────────────────────

_MEASURED = {"source_id": "GENVCE", "dataSource": None, "cropEppo": "TRZAX",
             "qualityParams": '{"humedad_pct": 17.7}'}
_BSL = {"source_id": "BSL", "dataSource": "bsa", "cropEppo": "TRZAX"}
_SILAGE = {"source_id": "NAVARRA-AGRARIA", "cropEppo": "ZEAMX", "year": 2019,
           "qualityParams": '{"dry_matter_pct": 34.8, "ndf_pct": 40.1}', "yieldKgHa": 26176.0}
_TOMATO = {"source_id": "CTIFL", "dataSource": "ctifl", "cropEppo": "LYPES"}
_OLIVE = {"source_id": "NAVARRA-AGRARIA", "cropEppo": "OLVEU", "yieldMetric": "fruit_weight_kg_ha"}


def test_main_mode_keeps_every_family_but_forage_and_excluded_sources():
    for trial in (_MEASURED, _TOMATO, _OLIVE):
        assert ep.is_numeric_yield_eligible(trial)
        assert not ep.is_numeric_yield_eligible(trial, "forage")
    assert not ep.is_numeric_yield_eligible(_BSL)
    assert not ep.is_numeric_yield_eligible(_BSL, "forage")
    assert not ep.is_numeric_yield_eligible(_SILAGE)
    assert ep.is_numeric_yield_eligible(_SILAGE, "forage")


def test_grain_yield_is_grain_family_main_product_only():
    assert ep.is_grain_yield(_MEASURED)
    assert not ep.is_grain_yield(_BSL)
    assert not ep.is_grain_yield(_SILAGE)
    assert not ep.is_grain_yield(_TOMATO)
    assert not ep.is_grain_yield(_OLIVE)
    assert not ep.is_grain_yield({**_MEASURED, "yieldMetric": "fresh_kg_ha"})


# ── forage basis and dry-matter conversion ───────────────────────────────────

@pytest.mark.parametrize(("trial", "basis"), [
    ({"yieldBasis": "dry_matter"}, "dry_matter"),
    ({"yieldBasis": " Fresh_Matter "}, "fresh_matter"),
    ({"yieldMetric": "forage_dry_matter_kg_ha"}, "dry_matter"),
    ({"yieldMetric": "forage_fresh_matter_kg_ha"}, "fresh_matter"),
    ({"yieldMetric": "materia_verde_kg_ha"}, "fresh_matter"),
    # Cited source rules (Navarra Agraria forage tables state kg ms/ha).
    (_SILAGE, "dry_matter"),
    ({"source_id": "NAVARRA-AGRARIA", "cropEppo": "MEDSA", "year": 2021}, "dry_matter"),
    ({"source_id": "NAVARRA-AGRARIA", "cropEppo": "SETIT", "year": 2014.0}, "dry_matter"),
    # The record's yieldBasis wins over a source rule.
    ({**_SILAGE, "yieldBasis": "fresh_matter"}, "fresh_matter"),
    # No statement: a DM % or the magnitude never decides the basis.
    ({**_SILAGE, "year": 2018}, "unknown"),
    ({"source_id": "LEGACY", "cropEppo": "", "year": 2014,
      "qualityParams": '{"ms_pct": 17.1, "fnd_pct": 66.6}', "yieldKgHa": 9604.0}, "unknown"),
    ({"source_id": "X", "cropEppo": "ZEAMX", "year": 2019, "yieldKgHa": 60000.0,
      "qualityParams": '{"dry_matter_pct": 33.0, "ndf_pct": 40.0}'}, "unknown"),
    ({}, "unknown"),
])
def test_forage_basis(trial, basis):
    assert ep.forage_basis(trial) == basis


def test_source_rules_cite_a_document_and_a_quote():
    assert ep.FORAGE_BASIS_SOURCE_RULES
    for rule in ep.FORAGE_BASIS_SOURCE_RULES:
        assert rule.basis in ("dry_matter", "fresh_matter")
        assert rule.document and rule.quote
        assert rule.source_id == rule.source_id.upper() and rule.crop_eppo == rule.crop_eppo.upper()


@pytest.mark.parametrize(("qp", "pct"), [
    ('{"dry_matter_pct": 34.8, "ndf_pct": 40.1}', 34.8),
    ('{"protein_pct": 9.1, "materia_seca_pct": 32.78}', 32.78),
    ('{"ms_pct": 17.1, "fnd_pct": 66.6}', 17.1),
    ({"MS_pct": 20}, 20.0),
    ('{"dry_matter_pct": null, "ms_pct": 21.0}', 21.0),
    ('{"dry_matter_pct": 0}', None),
    ('{"dry_matter_pct": 134.0}', None),
    ('{"humidity_pct": 22.0}', None),
    ("not json", None),
    (None, None),
])
def test_dry_matter_pct(qp, pct):
    assert ep.dry_matter_pct(qp) == pct


def test_forage_dm_yield_converts_only_known_basis():
    assert ep.forage_dm_yield(_SILAGE) == 26176.0  # source rule: already dry matter
    fresh = {"yieldBasis": "fresh_matter", "yieldKgHa": 60000.0,
             "qualityParams": '{"dry_matter_pct": 33.0}'}
    assert ep.forage_dm_yield(fresh) == pytest.approx(19800.0)
    assert ep.forage_dm_yield({**fresh, "qualityParams": None}) is None  # no DM % to convert
    assert ep.forage_dm_yield({**_SILAGE, "year": 2018}) is None          # unknown basis
    assert ep.forage_dm_yield({**_SILAGE, "yieldKgHa": None}) is None


# Mixed forage set: only rows convertible to kg dry matter/ha contribute a number, while
# every forage-eligible row still counts.
_FORAGE_MIX = {
    "dry_matter": {"source_id": "X", "yieldMetric": "forage_dry_matter_kg_ha",
                   "yieldKgHa": 18000.0, "qualityParams": '{"ms_pct": 21.0, "fnd_pct": 50.0}'},
    "unknown": {"source_id": "X", "yieldKgHa": 9604.0, "qualityParams": '{"ms_pct": 17.1, "fnd_pct": 66.6}'},
    "fresh_with_dm": {"source_id": "X", "yieldBasis": "fresh_matter", "yieldKgHa": 60000.0,
                      "qualityParams": '{"dry_matter_pct": 33.0, "ndf_pct": 40.0}'},
    "fresh_without_dm": {"source_id": "X", "yieldMetric": "forage_fresh_matter_kg_ha", "yieldKgHa": 50000.0},
}


def test_forage_numeric_evidence_needs_a_convertible_dry_matter_yield():
    eligible = {k: ep.is_numeric_yield_eligible(t, "forage") for k, t in _FORAGE_MIX.items()}
    numeric = {k: ep.is_forage_numeric_evidence(t) for k, t in _FORAGE_MIX.items()}
    assert eligible == dict.fromkeys(_FORAGE_MIX, True)  # counts: all four rows are forage evidence
    assert numeric == {"dry_matter": True, "unknown": False,
                       "fresh_with_dm": True, "fresh_without_dm": False}
    values = [ep.forage_dm_yield(t) for t in _FORAGE_MIX.values() if ep.is_forage_numeric_evidence(t)]
    assert values == [18000.0, pytest.approx(19800.0)]


def test_forage_numeric_evidence_keeps_source_purpose_and_value_gates():
    dm = _FORAGE_MIX["dry_matter"]
    assert not ep.is_forage_numeric_evidence({**dm, "source_id": "BSL"})          # excluded source
    assert not ep.is_forage_numeric_evidence({**dm, "qualityParams": None,
                                              "yieldMetric": "grain_kg_ha"})       # not forage
    assert not ep.is_forage_numeric_evidence({**dm, "yieldKgHa": None})            # no value
    assert not ep.is_forage_numeric_evidence({})


# ── (c) site kind ────────────────────────────────────────────────────────────

_AGGREGATE_SITES = [
    # CREA
    "Media 14 Località", "Media 8 Località",
    # NEBIH
    "Országos", "Országos átlag", "Átlag (9 helyszín)", "Average 10 locations",
    "Hungary (multiple locations)", "Magyarország",
    # AHDB / national registries / multi-location means
    "UK national list", "Poland (national average)", "France", "Spain (multiple locations)",
    "Sweden (mean of 3 trials)", "Skåne, Östergötland, Uppland", "Småland",
    # BSL containers and the national row
    "BSL Deutschland Cfb", "BSL Deutschland Dfb", "BSL Deutschland Uebergang",
    "Bundesweit", "Bundesweit (Deutschland)",
    # Regional / zone-level results
    "Navarra", "Secanos frescos de Navarra", "Red GENVCE", "Castilla y León",
    "Andalucía (IFAPA)", "Bassin Sud-Est",
    # INRA Maroc agro-ecological zones
    "Zones Bour favorable", "Zones arides et semi-arides", "Zones favorables",
    "Bour favorable et montagne", "Bour et zones semi-arides",
    "Régions arides et semi-arides", "Toutes les régions de culture du colza au Maroc",
    "Chaouia, Abda et Zaër", "Chaouia-Abda-Zaër-Saïs", "Large adaptation", "Non spécifié",
    # Unknown / empty
    "unknown", "Múltiples localidades", None, "", "   ",
]

_FIELD_SITES = [
    "Valladolid", "Zamadueñas (Valladolid)", "Cadreita", "Lleida", "Córdoba", "Oskotz",
    "Doneztebe", "Juansenea (Doneztebe) - C1N0", "Würzburg", "Freising",
    "Ostenfeld (Schleswig-Holstein)", "Villafranca Piemonte (TO)", "Fundulea",
    "Mosonmagyaróvár", "Abaújszántó", "Sopronhorpács", "Tekirdağ İnanlı", "İzmir",
    "Rípodas (Navarra)", "Sesma (Navarra)", "Invernadero Richel, Navarra", "ITGA Navarra",
    "Balandran (Bellegarde)", "Pleumeur-Gautier (Terre d'Essais)", "Avignon", "Beja",
]


@pytest.mark.parametrize("name", _AGGREGATE_SITES)
def test_aggregate_sites(name):
    assert ep.is_aggregate_site(name)
    assert ep.site_kind(name) == ep.SITE_KIND_AGGREGATE


@pytest.mark.parametrize("name", _FIELD_SITES)
def test_field_sites(name):
    assert not ep.is_aggregate_site(name)
    assert ep.site_kind(name) == ep.SITE_KIND_FIELD


def test_aggregate_patterns_extend_the_geo_backfill_patterns():
    assert set(AGGREGATE_PATTERNS) <= set(ep.AGGREGATE_SITE_PATTERNS)
    for p in ep.AGGREGATE_SITE_PATTERNS + ep.AGGREGATE_SITE_NAMES:
        assert p == p.lower() and p.strip()


@pytest.mark.parametrize(("scope", "expected"), [
    (None, True), ("site", True), ("Site", True),
    ("national", False), ("regional", False), ("unlocated", False),
])
def test_field_scope(scope, expected):
    assert ep.is_field_scope(scope) is expected


def test_field_evidence_needs_field_scope_and_field_site():
    assert ep.is_field_evidence("site", "Cadreita")
    assert ep.is_field_evidence(None, "Cadreita")
    assert not ep.is_field_evidence("regional", "Cadreita")
    assert not ep.is_field_evidence("site", "Media 14 Località")
    assert not ep.is_field_evidence("national", "BSL Deutschland Cfb")


# ── (d) content-dedup key ────────────────────────────────────────────────────

_TRIAL = {
    "cropEppo": "ZEAMX", "varietyNormalized": "CODIWAY", "year": 2019, "yieldKgHa": 12300.0,
    "yieldNoteS1": None, "irrigationRegime": "http://aims.fao.org/aos/agrovoc/c_3954",
    "productionSystem": None, "mergeKey": "a|1", "trialLocationKey": "doneztebe",
}


def test_content_key_ignores_identity_and_normalisation_fields_and_site_order():
    twin = {**_TRIAL, "mergeKey": "a|2", "trialLocationKey": "doneztebe-santesteban"}
    assert ep.content_key(_TRIAL, ["Doneztebe", "Santesteban"]) == \
        ep.content_key(twin, ["Santesteban", "Doneztebe", "Doneztebe"])


@pytest.mark.parametrize(("field", "value"), [
    ("cropEppo", "ZEAMA"), ("varietyNormalized", "OTHER"), ("year", 2020),
    ("yieldKgHa", 12301.0), ("yieldNoteS1", 7.0),
    ("irrigationRegime", "http://aims.fao.org/aos/agrovoc/c_6436"),
    ("productionSystem", "organic"),
])
def test_content_key_distinguishes_observed_content(field, value):
    assert ep.content_key(_TRIAL, ["Doneztebe"]) != \
        ep.content_key({**_TRIAL, field: value}, ["Doneztebe"])


# The orchard / perennial fields of one block: trials that differ only in one of them
# are different observations (almond rootstock trials, planting density trials, ...).
_ORCHARD = {
    **_TRIAL, "cropEppo": "PRNDU", "rootstock": "GF-677", "scion": "Guara",
    "trainingSystem": "open vase", "plantingYear": 2008, "plantingDensityTreesHa": 400,
    "cropCycle": "perennial",
}


def test_content_key_ignores_nothing_orchard_when_identical():
    assert ep.content_key(_ORCHARD, ["Sesma"]) == ep.content_key({**_ORCHARD, "mergeKey": "z"}, ["Sesma"])


@pytest.mark.parametrize(("field", "value"), [
    ("rootstock", "Garnem"), ("scion", "Lauranne"), ("trainingSystem", "hedgerow"),
    ("plantingYear", 2010), ("plantingDensityTreesHa", 625), ("cropCycle", "annual"),
    ("rootstock", None), ("plantingDensityTreesHa", None),
])
def test_content_key_keeps_orchard_differences_distinct(field, value):
    assert ep.content_key(_ORCHARD, ["Sesma"]) != ep.content_key({**_ORCHARD, field: value}, ["Sesma"])


def test_content_key_treats_absent_and_empty_orchard_fields_alike():
    absent = {k: v for k, v in _ORCHARD.items() if k not in ep.CONTENT_KEY_BLANK_DEFAULT_FIELDS}
    blank = {**absent, **{f: "" for f in ep.CONTENT_KEY_BLANK_DEFAULT_FIELDS}}
    nulls = {**absent, **dict.fromkeys(ep.CONTENT_KEY_BLANK_DEFAULT_FIELDS)}
    assert ep.content_key(absent, []) == ep.content_key(blank, []) == ep.content_key(nulls, [])
    assert ep.content_key({**absent, "plantingDensityTreesHa": 0}, []) != ep.content_key(absent, [])


def test_content_key_distinguishes_sites():
    assert ep.content_key(_TRIAL, ["Doneztebe"]) != ep.content_key(_TRIAL, ["Oskotz"])


# ── (e) Cypher fragment builders ─────────────────────────────────────────────

@pytest.mark.parametrize("builder", [
    ep.cypher_excluded_source, ep.cypher_yield_purpose, ep.cypher_numeric_yield_eligible,
    ep.cypher_crop_family, ep.cypher_grain_yield, ep.cypher_forage_basis,
    ep.cypher_dry_matter_pct, ep.cypher_forage_dm_yield, ep.cypher_forage_numeric_evidence,
    ep.cypher_field_scope, ep.cypher_aggregate_site, ep.cypher_content_key,
])
def test_cypher_builders_use_the_given_alias(builder):
    frag = builder("x1")
    assert "x1." in frag
    assert "vt." not in frag and "ts." not in frag


@pytest.mark.parametrize("bad", ["", "v t", "v) DETACH DELETE (n", "1v", "v.x"])
def test_cypher_builders_reject_non_identifier_aliases(bad):
    with pytest.raises(ValueError):
        ep.cypher_numeric_yield_eligible(bad)
    with pytest.raises(ValueError):
        ep.cypher_aggregate_site(bad)


def test_cypher_field_evidence_composes_trial_and_site_aliases():
    frag = ep.cypher_field_evidence("v", "t")
    assert "v.aggregationScope" in frag and "t.name" in frag


def test_cypher_literals_are_escaped():
    assert ep._cypher_list(["a'b", "c\\d"]) == "['a\\'b', 'c\\\\d']"


# ── (f) evidence tiers, policy yield and off-mode counts (row policy twins) ──

@pytest.mark.parametrize(("scope", "site", "tier"), [
    ("site", "Cadreita", "field"),
    (None, "Cadreita", "field"),
    ("regional", "Cadreita", "regional"),
    ("national", "Cadreita", "regional"),
    ("site", "UK national list", "regional"),
    ("site", "Media 8 Località", "regional"),
    ("site", "", "regional"),
    (None, None, "regional"),
])
def test_evidence_tier_of_a_row(scope, site, tier):
    assert ep.evidence_tier(scope, site) == tier


def test_policy_yield_main_mode():
    base = {"source_id": "GENVCE", "cropEppo": "TRZAX", "yieldKgHa": 6200}
    assert ep.policy_yield(base) == 6200.0
    assert ep.policy_yield({**base, "yieldKgHa": None}) is None            # missing is None, never 0
    assert ep.policy_yield({**base, "source_id": "BSL"}) is None           # note x constant, not kg
    forage = {**base, "qualityParams": '{"ndf_pct": 41}'}
    assert ep.policy_yield(forage) is None                                 # off-mode in main
    assert ep.policy_yield({**base, "yieldMetric": "fruit_weight_kg_ha"}) == 6200.0


def test_policy_yield_forage_mode_is_dry_matter_only():
    qp = '{"ndf_pct": 41, "dry_matter_pct": 33}'
    dry = {"source_id": "NAVARRA-AGRARIA", "cropEppo": "ZEAMX", "year": 2019, "yieldKgHa": 25000,
           "qualityParams": qp}
    assert ep.policy_yield(dry, "forage") == 25000.0                       # cited source rule
    fresh = {"source_id": "X", "cropEppo": "ZEAMX", "yieldBasis": "fresh_matter", "yieldKgHa": 60000,
             "qualityParams": qp}
    assert ep.policy_yield(fresh, "forage") == pytest.approx(19800.0)      # converted with the DM %
    unknown = {"source_id": "X", "cropEppo": "ZEAMX", "year": 2015, "yieldKgHa": 18000, "qualityParams": qp}
    assert ep.policy_yield(unknown, "forage") is None                      # basis never inferred
    assert ep.policy_yield({**dry, "source_id": "BSL"}, "forage") is None
    assert ep.policy_yield({"source_id": "GENVCE", "yieldKgHa": 6200}, "forage") is None  # not forage


def test_off_mode_and_unconverted_counts():
    forage = {"source_id": "X", "cropEppo": "ZEAMX", "year": 2015, "yieldKgHa": 18000,
              "qualityParams": '{"ndf_pct": 41}'}
    grain = {"source_id": "X", "cropEppo": "ZEAMX", "yieldKgHa": 12000}
    assert ep.is_other_purpose_evidence(forage) and not ep.is_other_purpose_evidence(grain)
    assert not ep.is_other_purpose_evidence({**forage, "source_id": "BSL"})   # excluded source
    assert not ep.is_other_purpose_evidence(forage, "forage")                 # never counted off-mode there
    assert ep.has_unconverted_kg(forage, "forage")                            # kg, basis unknown
    assert not ep.has_unconverted_kg(forage)                                  # off-mode in main
    assert not ep.has_unconverted_kg({**forage, "yieldKgHa": None}, "forage")
    assert not ep.has_unconverted_kg(grain)                                   # has a number


def test_unknown_tier_is_rejected():
    with pytest.raises(ValueError):
        ep.check_tier("national")


def test_row_policy_validates_aliases_carry_and_mode():
    block = ep.cypher_row_policy("forage", "t", "s", carry=("matched",))
    assert "t.qualityParams" in block and "s.name" in block and ", matched" in block
    assert all(col in block for col in ep.ROW_POLICY_COLUMNS)
    with pytest.raises(ValueError):
        ep.cypher_row_policy("grain")
    with pytest.raises(ValueError):
        ep.cypher_row_policy("main", carry=("x) DETACH DELETE (n",))


def test_row_policy_evaluates_the_purpose_once_and_counts_other_purpose_in_main_only():
    main, forage = ep.cypher_row_policy("main"), ep.cypher_row_policy("forage")
    assert main.count("ndf_pct") == 1 and forage.count("ndf_pct") == 1
    assert "(NOT ep_excluded AND NOT ep_in_mode) AS ep_other" in main
    assert "false AS ep_other" in forage


# ── single home of the tier gates, prefilters and the numeric predicate ──────
def test_numeric_candidate_is_the_predicate_the_row_policy_builds_ep_y_from():
    block = ep.cypher_row_policy("main")
    candidate = ep.cypher_numeric_candidate("vt", excluded="ep_excluded")
    assert candidate == "(vt.yieldKgHa IS NOT NULL AND NOT ep_excluded)"
    assert block.count(candidate) == 2  # ep_y and ep_unconv
    # a query's cheap WHERE uses the same predicate with the source test inlined
    assert ep.cypher_numeric_candidate("vt") == f"(vt.yieldKgHa IS NOT NULL AND NOT {ep.cypher_excluded_source('vt')})"
    assert ep.cypher_tier_prefilter("regional") == "AND " + ep.cypher_numeric_candidate("vt")
    assert ep.cypher_tier_prefilter("regional", "t") == "AND " + ep.cypher_numeric_candidate("t")
    assert ep.cypher_tier_prefilter("field") == ""
    with pytest.raises(ValueError):
        ep.cypher_tier_prefilter("national")


def test_site_name_is_normalised_once_per_row_not_once_per_pattern():
    block = ep.cypher_row_policy("main")
    assert block.count("toLower(trim(coalesce(ts.name, '')))") == 1
    assert block.count("ep_site_lc CONTAINS ep_pat") == 1
    assert "ts.name" not in block.replace("toLower(trim(coalesce(ts.name, '')))", "")
    # the standalone fragment keeps the inline name (it is used outside the row policy)
    assert "ts.name" in ep.cypher_aggregate_site("ts")


def test_row_policy_exposes_the_excluded_flag():
    assert "ep_excluded" in ep.ROW_POLICY_COLUMNS
    last_with = ep.cypher_row_policy("forage").rstrip("\n").split("\n")[-1]
    assert "ep_excluded" in last_with.split("AS ep_unconv")[0]  # carried to the caller's next clause


@pytest.mark.parametrize("tier,row_tier,in_mode,other,y,expected", [
    ("field", "field", True, False, None, True),       # in-mode rows count even without a number
    ("field", "field", False, True, None, True),       # other-purpose rows are counted (with_other)
    ("field", "field", False, False, None, False),
    ("field", "regional", True, False, 1.0, False),
    ("regional", "regional", True, False, 1.0, True),
    ("regional", "regional", True, False, None, False),  # regional: numeric rows only
    ("regional", "regional", False, True, 1.0, False),
    ("regional", "field", True, False, 1.0, False),
])
def test_tier_gate_python_twin(tier, row_tier, in_mode, other, y, expected):
    assert ep.passes_tier_gate(tier, row_tier, in_mode, other, y) is expected


def test_tier_gate_without_other_purpose_rows_and_validation():
    assert ep.passes_tier_gate("field", "field", False, True, None, with_other=False) is False
    assert "ep_other" not in ep.cypher_tier_gate_expr("field", with_other=False)
    assert "ep_other" in ep.cypher_tier_gate_expr("field")
    assert ep.cypher_tier_gate("regional").startswith("WHERE ") and ep.cypher_tier_gate("regional").endswith("\n")
    assert ep.cypher_numeric_tier_gate("field") == "WHERE ep_tier = 'field' AND ep_in_mode AND ep_y IS NOT NULL\n"
    with pytest.raises(ValueError):
        ep.passes_tier_gate("national", "field", True, False, None)
    with pytest.raises(ValueError):
        ep.cypher_tier_gate_expr("national")


def test_yield_basis_names_the_unit_of_the_reported_numbers():
    assert ep.yield_basis("forage") == ep.BASIS_DRY_MATTER
    assert ep.yield_basis("main") is None and ep.yield_basis() is None
    with pytest.raises(ValueError):
        ep.yield_basis("grain")


# ── presence-only evidence (rule 7) ──────────────────────────────────────────
def test_presence_only_applies_to_the_main_mode_only():
    assert ep.presence_only_applies("main") is True and ep.presence_only_applies("forage") is False
    with pytest.raises(ValueError):
        ep.presence_only_applies("grain")


@pytest.mark.parametrize("trial,site,mode,expected", [
    ({"source_id": "BSL", "aggregationScope": "regional", "yieldKgHa": 9500.0}, "BSL Deutschland Cfb", "main", True),
    ({"source_id": "X", "dataSource": "bsa", "aggregationScope": "national"}, "Bundesweit", "main", True),
    ({"source_id": "BSL", "aggregationScope": "regional"}, "BSL Deutschland Cfb", "forage", False),
    ({"source_id": "AHDB", "aggregationScope": "national", "yieldKgHa": 5000.0}, "UK national list", "main", False),
    ({"source_id": "BSL", "aggregationScope": "site"}, "Cadreita", "main", False),          # a field row
    ({"source_id": "BSL", "aggregationScope": "regional", "yieldMetric": "forage_dry_matter_kg_ha"},
     "BSL Deutschland Cfb", "main", False),                                                  # off-purpose
])
def test_is_presence_only_evidence(trial, site, mode, expected):
    assert ep.is_presence_only_evidence(trial, site, mode) is expected


def test_presence_gate_and_prefilter_fragments():
    assert ep.cypher_presence_gate() == "WHERE ep_tier = 'regional' AND ep_in_mode AND ep_excluded\n"
    assert ep.cypher_presence_prefilter("t") == "AND " + ep.cypher_excluded_source("t")
