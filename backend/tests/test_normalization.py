"""Tests for the normalisation registry.

Run with:  python -m pytest tests/test_normalization.py -v
"""

import json

from app.ingestion.normalization_registry import (
    TRAIT_REGISTRY,
    DISEASE_REGISTRY,
    LOCATION_NORMALIZATION,
    SCALE_NORMALIZERS,
    normalize_variety_name,
    normalize_location,
    eppo_to_scientific,
    normalize_merge_key,
    transform_traits_to_unified,
    EPPO_TO_SCIENTIFIC,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Variety name
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeVarietyName:
    def test_uppercase(self):
        assert normalize_variety_name("Hispanic") == "HISPANIC"

    def test_strip_trailing_tag(self):
        assert normalize_variety_name("HISPANIC (T)") == "HISPANIC"
        assert normalize_variety_name("Graphic (test)") == "GRAPHIC"

    def test_mas_26_t(self):
        assert normalize_variety_name("MAS 26 T") == "MAS 26 T"

    def test_none(self):
        assert normalize_variety_name(None) is None

    def test_empty(self):
        assert normalize_variety_name("") is None

    def test_paiva(self):
        assert normalize_variety_name("Paiva") == "PAIVA"


# ═══════════════════════════════════════════════════════════════════════════════
# EPPO → scientific name
# ═══════════════════════════════════════════════════════════════════════════════

class TestEppoToScientific:
    def test_zeamx(self):
        assert eppo_to_scientific("ZEAMX") == "Zea mays"

    def test_trzax(self):
        assert eppo_to_scientific("TRZAX") == "Triticum aestivum"

    def test_with_prefix(self):
        assert eppo_to_scientific("eppo:ZEAMX") == "Zea mays"

    def test_unknown(self):
        assert eppo_to_scientific("XXXXX") is None

    def test_none(self):
        assert eppo_to_scientific(None) is None

    def test_all_eppos_mapped(self):
        """Every EPPO in the data (see setup) must have a mapping."""
        # This is a structural test: all codes we know about
        known = [
            "ZEAMX", "TRZAX", "TRZAW", "TRZDU", "HORVX", "BRSNN", "BRSOX",
            "HELAN", "GLXMA", "PISSA", "VICSA", "LENCU", "CIEAR", "AVESA",
            "SECCE", "TTLSS", "SOLTU", "LYPES", "CAPAN", "VICFA", "LUPAL",
            "MALDO", "FRAAN", "PRUDU", "OLEAE", "VITVI", "ORYSA",
        ]
        for code in known:
            assert eppo_to_scientific(code) is not None, f"Missing mapping for {code}"


# ═══════════════════════════════════════════════════════════════════════════════
# Location normalisation
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeLocation:
    def test_bsl_exact(self):
        info = normalize_location("BSL Deutschland Cfb")
        assert info is not None
        assert info["name"] == "BSL Alemania Cfb"
        assert info["country"] == "Alemania"
        assert info["climateClass"] == "Cfb"

    def test_ctifl_synonyms(self):
        """All synonyms for CTIFL Balandran resolve to the same canonical name."""
        names = ["CTIFL Balandran", "Balandran (CTIFL)",
                 "Balandran (Bellegarde)", "Balandran"]
        canonical = normalize_location(names[0])
        for n in names[1:]:
            assert normalize_location(n)["name"] == canonical["name"]

    def test_hungary_average(self):
        info = normalize_location("Hungary (average)")
        assert info is not None
        assert info["country"] == "Hungría"

    def test_navarra(self):
        info = normalize_location("Imarcoain")
        assert info is not None
        assert info["country"] == "España"

    def test_unknown(self):
        info = normalize_location("Some random place")
        assert info is None

    def test_none(self):
        assert normalize_location(None) is None

    def test_case_insensitive(self):
        info = normalize_location("bsl deutschland cfb")
        assert info is not None
        assert info["name"] == "BSL Alemania Cfb"


# ═══════════════════════════════════════════════════════════════════════════════
# MergeKey normalisation
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeMergeKey:
    def test_bsl_maize(self):
        result = normalize_merge_key("BSL", "ZEAMX", "MAS 26 T", 2019,
                                     "BSL Deutschland Cfb")
        assert "BSL|eppo:ZEAMX|MAS 26 T|Alemania|2019" in result

    def test_variety_normalised(self):
        result = normalize_merge_key("GENVCE", "TRZAX", "Hispanic (T)", 2021,
                                     "Imarcoain")
        assert "HISPANIC" in result

    def test_missing_year(self):
        result = normalize_merge_key("BSL", "ZEAMX", "MAS 26 T", None,
                                     "BSL Deutschland Cfb")
        assert "NOYEAR" in result

    def test_missing_eppo(self):
        result = normalize_merge_key("BSL", None, "MAS 26 T", 2019,
                                     "BSL Deutschland Cfb")
        assert "NOEPPO" in result


# ═══════════════════════════════════════════════════════════════════════════════
# Scale normalisation
# ═══════════════════════════════════════════════════════════════════════════════

class TestScaleBSL:
    def setup_method(self):
        self.norm = SCALE_NORMALIZERS["1-9_bsl"]

    def test_better_1_is_best(self):
        assert self.norm(1, "better") == 1.0

    def test_better_9_is_worst(self):
        assert self.norm(9, "better") == 0.0

    def test_better_5_is_mid(self):
        assert self.norm(5, "better") == 0.5

    def test_worse_1_is_best(self):
        assert self.norm(1, "worse") == 0.0

    def test_worse_9_is_worst(self):
        assert self.norm(9, "worse") == 1.0

    def test_worse_5_is_mid(self):
        assert self.norm(5, "worse") == 0.5

    def test_none(self):
        assert self.norm(None, "better") is None

    def test_clamp_low(self):
        assert self.norm(-1, "better") == 1.0  # clamped to 1

    def test_clamp_high(self):
        assert self.norm(15, "better") == 0.0  # clamped to 9


# ═══════════════════════════════════════════════════════════════════════════════
# Trait transformation engine
# ═══════════════════════════════════════════════════════════════════════════════

class TestTransformTraits:
    def test_bsl_traits_normalised(self):
        raw = json.dumps({
            "kaelteempfindlichkeit": 7,
            "neigung_zu_lager": 8,
            "pflanzenlaenge": 3,
        })
        result, disease = transform_traits_to_unified(raw, None, "BSL")
        assert result is not None
        parsed = json.loads(result)

        # cold_sensitivity: higherIs=worse, value=7 → (7-1)/8 = 0.75
        assert "cold_sensitivity" in parsed
        assert parsed["cold_sensitivity"]["value"] == 0.75
        assert parsed["cold_sensitivity"]["rawValue"] == 7

        # lodging_susceptibility: 8 → (8-1)/8 = 0.875
        assert parsed["lodging_susceptibility"]["value"] == 0.875

        # plant_height: 3 → (3-1)/8 = 0.25, higherIs=worse
        assert parsed["plant_height"]["value"] == 0.25

        # AGROVOC URI present
        assert parsed["cold_sensitivity"]["agrovoc"] is not None

    def test_bsl_disease_normalised(self):
        raw = json.dumps({"staengelfaeule": 2})
        result, disease = transform_traits_to_unified(None, raw, "BSL")
        assert disease is not None
        parsed = json.loads(disease)

        assert "stem_rot_resistance" in parsed
        # higherIs=better, value=2 → 1.0 - ((2-1)/8) = 0.875
        assert parsed["stem_rot_resistance"]["value"] == 0.875

    def test_categorical_trait_preserved(self):
        raw = json.dumps({"reifegruppe": "mittelspät bis spät"})
        result, _ = transform_traits_to_unified(raw, None, "BSL")
        assert result is not None
        parsed = json.loads(result)
        assert parsed["maturity_group"]["value"] == "mittelspät bis spät"

    def test_unknown_source_no_mapping(self):
        """A source with no registered traits produces no unified output."""
        raw = json.dumps({"some_trait": 5})
        result, disease = transform_traits_to_unified(raw, None, "UNKNOWNSOURCE")
        assert result is None

    def test_none_traits(self):
        result, disease = transform_traits_to_unified(None, None, "BSL")
        assert result is None
        assert disease is None

    def test_string_no_traits(self):
        """String without registered keys → empty unified."""
        result, disease = transform_traits_to_unified(
            '{"unregistered_key": 5}', None, "BSL"
        )
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# Registry structural integrity
# ═══════════════════════════════════════════════════════════════════════════════

class TestRegistryIntegrity:
    def test_all_traits_have_canonical(self):
        for canonical, config in TRAIT_REGISTRY.items():
            assert config["canonical"] == canonical, (
                f"Key mismatch: {canonical} != {config['canonical']}"
            )

    def test_all_traits_have_scale(self):
        for canonical, config in TRAIT_REGISTRY.items():
            assert "scale" in config, f"{canonical} missing scale"
            if config["scale"] != "categorical":
                assert config["higherIs"] in ("better", "worse"), (
                    f"{canonical}: higherIs must be better/worse or None for categorical"
                )

    def test_all_traits_have_sources_dict(self):
        for canonical, config in TRAIT_REGISTRY.items():
            assert isinstance(config["sources"], dict), (
                f"{canonical}: sources must be a dict"
            )

    def test_disease_registry_structure(self):
        for canonical, config in DISEASE_REGISTRY.items():
            assert config["canonical"] == canonical
            assert "scale" in config
            assert config["higherIs"] in ("better", "worse")

    def test_location_normalization_structure(self):
        for raw_key, info in LOCATION_NORMALIZATION.items():
            assert "name" in info, f"{raw_key} missing name"
            assert "country" in info, f"{raw_key} missing country"
            # climateClass may be None (unknown)

    def test_scale_normalizers_all_registered(self):
        """Every scale referenced by a trait must have a normaliser."""
        scales_used = set()
        for config in TRAIT_REGISTRY.values():
            if config["scale"] != "categorical":
                scales_used.add(config["scale"])
        for config in DISEASE_REGISTRY.values():
            scales_used.add(config["scale"])

        for scale in scales_used:
            assert scale in SCALE_NORMALIZERS, (
                f"No normaliser registered for scale '{scale}'"
            )

    def test_eppo_to_scientific_all_mapped(self):
        """All EPPO codes that appear in the data should have a mapping."""
        # This is a sanity check — if you add a new source with new EPPO codes,
        # add them to EPPO_TO_SCIENTIFIC.
        assert len(EPPO_TO_SCIENTIFIC) >= 27  # at least the ones we know
