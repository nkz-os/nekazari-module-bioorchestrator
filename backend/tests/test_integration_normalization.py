"""Integration tests for the ingestion normalisation pipeline.

Tests that the full pipeline (JSON-LD → normalise → Neo4j MERGE-ready fields)
produces correctly unified data, using realistic JSON-LD fragments.
"""

import json

from app.ingestion.base_ingester import BaseIngester
from app.ingestion.normalization_registry import (
    TRAIT_REGISTRY,
    normalize_merge_key,
)


# ── Test ingester that simulates a real source ─────────────────────────────

class FakeTestIngester(BaseIngester):
    """Minimal ingester for integration tests."""
    SOURCE_ID = "BSL"

    def __init__(self):
        # Skip BaseIngester.__init__ to avoid source registry lookup
        # We only need normalize_nodes() and _parse_nodes()
        from abc import ABC
        ABC.__init__(self)
        self.SOURCE_ID = "BSL"
        self._driver = None

    async def _parse_nodes(self, data: dict) -> dict[str, list[dict]]:
        graph = data.get("@graph", [])
        sites = []
        articles = []
        variety_trials = []
        management_trials = []

        for node in graph:
            t = node.get("@type", "")
            if t == "TrialSite":
                sites.append({
                    "name": node.get("name"),
                    "mergeKey": f"test|{node.get('name', '').lower()}",
                })
            elif t == "ArticleSource":
                articles.append({
                    "source": node.get("source"),
                    "source_id": self.SOURCE_ID,
                    "mergeKey": f"test|{node.get('source', '')}",
                })
            elif t == "VarietyTrial":
                eppo = node.get("crop_eppo", "").replace("eppo:", "")
                vt = {
                    "mergeKey": f"test|{eppo}|{node.get('variety')}|{node.get('year')}",
                    "variety": node.get("variety"),
                    "cropEppo": eppo,
                    "cropScientific": node.get("crop_scientific"),
                    "year": node.get("year"),
                    "trialLocation": node.get("trial_location"),
                    "agronomicTraits": (
                        json.dumps(node["agronomic_traits"])
                        if node.get("agronomic_traits") else None
                    ),
                    "diseaseScores": (
                        json.dumps(node["disease_scores"])
                        if node.get("disease_scores") else None
                    ),
                    "yieldKgHa": node.get("yield_kg_ha"),
                    "yieldNoteS1": node.get("yield_note_s1"),
                    "confidence": node.get("confidence", "medium"),
                }
                variety_trials.append(vt)
            elif t == "ManagementTrial":
                management_trials.append({
                    "mergeKey": f"test|{node.get('treatment')}|{node.get('year')}",
                    "cropEppo": node.get("crop_eppo", "").replace("eppo:", ""),
                    "variety": node.get("variety"),
                    "experimentType": node.get("experiment_type"),
                    "treatment": node.get("treatment"),
                    "resultMetric": node.get("result_metric"),
                    "resultValue": node.get("result_value"),
                    "resultUnit": node.get("result_unit"),
                    "year": node.get("year"),
                    "trialLocation": node.get("trial_location"),
                    "confidence": node.get("confidence", "medium"),
                    "metadata": json.dumps(node.get("metadata", {})),
                })

        return {
            "trial_sites": sites,
            "article_sources": articles,
            "variety_trials": variety_trials,
            "management_trials": management_trials,
        }


# ── Test JSON-LD fragments ─────────────────────────────────────────────────

BSL_JSONLD = {
    "@context": "https://nkz.robotika.cloud/ngsi-ld/bioorchestrator-context.jsonld",
    "@graph": [
        {
            "@type": "TrialSite",
            "name": "BSL Deutschland Cfb",
        },
        {
            "@type": "ArticleSource",
            "source": "BSL",
        },
        {
            "@type": "VarietyTrial",
            "variety": "MAS 26 T",
            "crop_eppo": "eppo:ZEAMX",
            "crop_scientific": "Zea mays",
            "year": 2019,
            "trial_location": "BSL Deutschland Cfb",
            "agronomic_traits": {
                "kaelteempfindlichkeit": 7,
                "neigung_zu_lager": 8,
                "pflanzenlaenge": 3,
                "koernerreifezahl": 6,
                "reifegruppe": "mittelspät bis spät",
            },
            "disease_scores": {
                "staengelfaeule": 2,
            },
            "yield_kg_ha": 11200,
            "confidence": "medium",
        },
        {
            "@type": "VarietyTrial",
            "variety": "Hispanic (T)",
            "crop_eppo": "eppo:HORVX",
            "crop_scientific": "Hordeum vulgare",
            "year": 2020,
            "trial_location": "BSL Deutschland Dfb",
            "agronomic_traits": {
                "kaelteempfindlichkeit": 5,
            },
            "confidence": "high",
        },
        {
            "@type": "VarietyTrial",
            "variety": "Unknown",
            "year": 0,
            "trial_location": "somewhere",
        },
    ],
}

NAVARRA_JSONLD = {
    "@context": "https://nkz.robotika.cloud/ngsi-ld/bioorchestrator-context.jsonld",
    "@graph": [
        {
            "@type": "TrialSite",
            "name": "Imarcoain",
        },
        {
            "@type": "ArticleSource",
            "source": "NAVARRA",
        },
        {
            "@type": "VarietyTrial",
            "variety": "Soissons",
            "crop_eppo": "eppo:TRZAX",
            "crop_scientific": "Triticum aestivum",
            "year": 2001,
            "trial_location": "Imarcoain",
            "confidence": "high",
        },
        {
            "@type": "ManagementTrial",
            "crop_eppo": "eppo:TRZAX",
            "variety": "Soissons",
            "experiment_type": "FertilizationTrial",
            "treatment": "Dosis N 0",
            "result_metric": "rendimiento_kg_ha",
            "result_value": 2800.0,
            "result_unit": "kg.ha-1",
            "year": 2001,
            "trial_location": "Navarra (zonas húmedas de secano)",
            "confidence": "high",
            "metadata": {"source": "Navarra Agraria"},
        },
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestIntegrationBSL:
    """Full pipeline test with realistic BSL data."""

    def _normalise(self, jsonld):
        ingester = FakeTestIngester()
        import asyncio
        nodes = asyncio.run(ingester._parse_nodes(jsonld))
        nodes = asyncio.run(ingester.normalize_nodes(nodes))
        return nodes

    def test_bsl_traits_translated(self):
        nodes = self._normalise(BSL_JSONLD)
        vts = [v for v in nodes["variety_trials"]
               if v.get("variety") == "MAS 26 T"]
        assert len(vts) == 1
        vt = vts[0]

        # varietyNormalized
        assert vt["varietyNormalized"] == "MAS 26 T"

        # locationNormalized
        assert vt["locationNormalized"] == "BSL Alemania Cfb"
        assert vt["locationCountry"] == "Alemania"
        assert vt["climateClass"] == "Cfb"

        # mergeKeyNormalized (TEST source from FakeTestIngester)
        assert vt["mergeKeyNormalized"] is not None
        assert "eppo:ZEAMX" in vt["mergeKeyNormalized"]
        assert "MAS 26 T" in vt["mergeKeyNormalized"]
        assert "Alemania" in vt["mergeKeyNormalized"]

        # agronomicTraitsUnified exists and has AGROVOC keys
        assert vt["agronomicTraitsUnified"] is not None
        traits = json.loads(vt["agronomicTraitsUnified"])
        assert "cold_sensitivity" in traits
        assert traits["cold_sensitivity"]["value"] == 0.75  # (7-1)/8
        assert traits["cold_sensitivity"]["agrovoc"] is not None
        assert traits["cold_sensitivity"]["rawValue"] == 7

        assert "lodging_susceptibility" in traits
        assert traits["lodging_susceptibility"]["value"] == 0.875  # (8-1)/8

        # categorical trait preserved
        assert "maturity_group" in traits
        assert traits["maturity_group"]["value"] == "mittelspät bis spät"

        # diseaseScoresUnified
        assert vt["diseaseScoresUnified"] is not None
        disease = json.loads(vt["diseaseScoresUnified"])
        assert "stem_rot_resistance" in disease
        # higherIs=better, 2 → 1-((2-1)/8) = 0.875
        assert disease["stem_rot_resistance"]["value"] == 0.875

        # validation
        assert vt["_validation"]["valid"] is True

    def test_variety_name_normalised(self):
        nodes = self._normalise(BSL_JSONLD)
        vts = [v for v in nodes["variety_trials"]
               if v.get("variety") == "Hispanic (T)"]
        assert len(vts) == 1
        assert vts[0]["varietyNormalized"] == "HISPANIC"

    def test_missing_fields_invalid(self):
        nodes = self._normalise(BSL_JSONLD)
        vts = [v for v in nodes["variety_trials"]
               if v.get("variety") == "Unknown"]
        assert len(vts) == 1
        assert vts[0]["_validation"]["valid"] is False
        assert "cropEppo" in vts[0]["_validation"]["missing"]
        assert "year" in vts[0]["_validation"]["missing"]

    def test_unknown_location(self):
        nodes = self._normalise(BSL_JSONLD)
        vts = [v for v in nodes["variety_trials"]
               if v.get("trialLocation") == "somewhere"]
        assert len(vts) == 1
        assert vts[0].get("locationNormalized") == "somewhere"
        assert vts[0].get("locationCountry") is None


class TestIntegrationNavarra:
    """Full pipeline test with Navarra (management trials) data."""

    def _normalise(self, jsonld):
        import asyncio
        ingester = FakeTestIngester()
        nodes = asyncio.run(ingester._parse_nodes(jsonld))
        nodes = asyncio.run(ingester.normalize_nodes(nodes))
        return nodes

    def test_variety_normalised(self):
        nodes = self._normalise(NAVARRA_JSONLD)
        vts = [v for v in nodes["variety_trials"]
               if v.get("variety") == "Soissons"]
        assert len(vts) == 1
        assert vts[0]["varietyNormalized"] == "SOISSONS"

    def test_location_resolved(self):
        nodes = self._normalise(NAVARRA_JSONLD)
        vts = [v for v in nodes["variety_trials"]
               if v.get("trialLocation") == "Imarcoain"]
        assert len(vts) == 1
        assert vts[0]["locationNormalized"] == "Imarcoain"
        assert vts[0]["locationCountry"] == "España"

    def test_management_trial_normalised(self):
        nodes = self._normalise(NAVARRA_JSONLD)
        mts = nodes["management_trials"]
        assert len(mts) == 1
        mt = mts[0]

        assert mt["varietyNormalized"] == "SOISSONS"
        assert mt.get("locationNormalized") is not None
        # Navarra (zonas húmedas de secano) should be resolved
        # It's not in the exact list but the partial matcher might pick it up

    def test_eppo_fills_scientific_name(self):
        """If cropScientific is missing, it's filled from EPPO."""
        jsonld = dict(NAVARRA_JSONLD)
        # Remove crop_scientific from one trial
        for node in jsonld["@graph"]:
            if node.get("@type") == "VarietyTrial":
                node.pop("crop_scientific", None)
                break

        nodes = self._normalise(jsonld)
        vts = nodes["variety_trials"]
        assert len(vts) >= 1
        # cropScientific should be filled from EPPO
        assert vts[0]["cropScientific"] == "Triticum aestivum"


class TestRegistryWithRealData:
    """Verify the registry covers known real-world patterns."""

    def test_bsl_all_traits_mapped(self):
        """All BSL trait keys should have a canonical mapping."""
        bsl_keys_in_registry = set()
        for canonical, config in TRAIT_REGISTRY.items():
            src_val = config["sources"].get("BSL")
            if src_val:
                if isinstance(src_val, list):
                    bsl_keys_in_registry.update(src_val)
                else:
                    bsl_keys_in_registry.add(src_val)

        # These are all the BSL trait keys observed in the real data
        known_bsl_keys = {
            "kaelteempfindlichkeit",
            "neigung_zu_lager",
            "pflanzenlaenge",
            "koernerreifezahl",
            "zeitpunkt_weibliche_bluete",
            "neigung_zu_bestockung",
            "abreifegrad_der_blaetter",
            "siloreifezahl",
            "reifegruppe",
        }
        for key in known_bsl_keys:
            assert key in bsl_keys_in_registry, f"BSL key {key} not mapped"

    def test_bsl_disease_key_mapped(self):
        """Disease score key from BSL should be mapped."""
        from app.ingestion.normalization_registry import DISEASE_REGISTRY
        bsl_keys = set()
        for canonical, config in DISEASE_REGISTRY.items():
            src_val = config["sources"].get("BSL")
            if src_val:
                if isinstance(src_val, list):
                    bsl_keys.update(src_val)
                else:
                    bsl_keys.add(src_val)
        assert "staengelfaeule" in bsl_keys

    def test_normalize_merge_key_unified_format(self):
        """All sources should produce the same mergeKey format."""
        formats = [
            normalize_merge_key("BSL", "ZEAMX", "MAS 26 T", 2019, "BSL Deutschland Cfb"),
            normalize_merge_key("GENVCE", "TRZAX", "Soissons", 2020, "Lleida"),
            normalize_merge_key("NAVARRA", "HORVX", "Hispanic", 2021, "Imarcoain"),
            normalize_merge_key("NEBIH", "BRSNN", "DK Exquisite", 2020, "Bóly"),
        ]
        for f in formats:
            parts = f.split("|")
            assert len(parts) == 5, f"Expected 5 parts in {f}"
            # Part 0: source (uppercase)
            assert parts[0].isupper() or parts[0] == "NAVARRA"
            # Part 1: eppo:XXXXX
            assert parts[1].startswith("eppo:")
            # Part 2: variety
            assert parts[2].isupper()
