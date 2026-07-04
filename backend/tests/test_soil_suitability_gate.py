"""Soil-suitability gate (C.5, soil-first).

Gate input = crop/seed STANDARD tolerance (EcoCrop / CropSoilSuitability)
× the parcel's REAL soil (Soil module). NOT trial-site soil.

The pure `assess_soil_suitability(crop_tolerance, parcel_soil)` verdict is
validated here against an agronomically-defensible truth-set. The
`extrapolate_varieties` wiring (drop unsuitable, flag marginal, EPPO→slug
resolution) is validated with a real graph (testcontainers) and a mocked
Soil module — no live Soil module in tests.
"""
from __future__ import annotations

import asyncio
import shutil
from unittest.mock import AsyncMock, patch

import pytest

from app.services.soil_client import assess_soil_suitability

# ── Truth-set: (crop_tolerance, parcel_soil) → expected verdict ──────────────
# Tolerance basis: EcoCrop/CropSoilSuitability pH ranges + USDA texture prefs.

# Acidophile whose pH band is 4.0–5.5 (blueberry-like); calcifuge.
BLUEBERRY = {"ph_min": 4.0, "ph_max": 5.5, "textures": ["sandy_loam", "loam"]}
# Wide-tolerance calcicole (olive-like), pH up to 8.5.
OLIVE = {"ph_min": 5.5, "ph_max": 8.5, "textures": ["sandy_loam", "loam"]}
# Paddy crop wanting heavy clay + poor drainage (rice-like).
RICE = {"ph_min": 5.0, "ph_max": 7.5, "textures": ["clay"], "drainage": ["poor"]}
WHEAT = {"ph_min": 5.5, "ph_max": 8.5, "textures": ["loam"]}
ALFALFA = {"ph_min": 6.5, "ph_max": 8.0, "textures": ["loam"]}
POTATO = {"ph_min": 5.0, "ph_max": 7.0, "textures": ["sandy_loam"]}


def _soil(ph, texture, available=True, source="soilgrids"):
    return {
        "ph": ph, "texture": texture, "awc_mm": 120.0,
        "data_available": available, "source": source,
    }


TRUTH_SET = [
    # blueberry on calcareous high-pH parcel → pH way above max → unsuitable
    ("blueberry/calcareous", BLUEBERRY, _soil(8.1, "Clay loam"), "unsuitable"),
    # olive on the same calcareous parcel (pH within its 8.5 max) → suitable
    ("olive/calcareous", OLIVE, _soil(8.0, "Loam"), "suitable"),
    # rice on free-draining sand → texture sand vs clay (extreme) → unsuitable
    ("rice/sand", RICE, _soil(6.5, "Sand"), "unsuitable"),
    # rice on heavy clay in its pH band → suitable
    ("rice/clay", RICE, _soil(6.5, "Clay"), "suitable"),
    # wheat on loam, mid-range pH → suitable
    ("wheat/loam", WHEAT, _soil(7.0, "Loam"), "suitable"),
    # blueberry only 0.2 pH above its max → marginal (within band)
    ("blueberry/slightly-alkaline", BLUEBERRY, _soil(5.7, "Sandy loam"), "marginal"),
    # potato 0.4 pH above max, texture near → marginal
    ("potato/slightly-alkaline", POTATO, _soil(7.4, "Loam"), "marginal"),
    # alfalfa on strongly acidic parcel (1.7 below min) → unsuitable
    ("alfalfa/acidic", ALFALFA, _soil(4.8, "Sandy loam"), "unsuitable"),
    # potato on its ideal soil → suitable
    ("potato/ideal", POTATO, _soil(6.0, "Sandy loam"), "suitable"),
]


@pytest.mark.parametrize(
    "case_id,tolerance,parcel,expected",
    TRUTH_SET, ids=[c[0] for c in TRUTH_SET],
)
def test_assess_soil_suitability_truth_set(case_id, tolerance, parcel, expected):
    result = assess_soil_suitability(tolerance, parcel)
    assert result["verdict"] == expected, (
        f"{case_id}: got {result['verdict']} ({result['reason']})"
    )
    # A real verdict is never silently unlabelled.
    assert result["reason"]
    assert result["source"]


def test_verdict_carries_ph_and_texture_detail():
    r = assess_soil_suitability(BLUEBERRY, _soil(8.1, "Clay loam"))
    assert r["ph"]["value"] == 8.1
    assert r["ph"]["min"] == 4.0 and r["ph"]["max"] == 5.5
    assert r["ph"]["verdict"] == "unsuitable"
    assert r["texture"]["value"] == "Clay loam"


# ── Honest `unknown` paths — never silently treat unknown as suitable ────────

def test_no_crop_tolerance_is_unknown():
    r = assess_soil_suitability(None, _soil(6.5, "Loam"))
    assert r["verdict"] == "unknown"
    assert r["confidence"] == "low"
    assert "toleran" in r["reason"].lower()


def test_empty_crop_tolerance_is_unknown():
    r = assess_soil_suitability({}, _soil(6.5, "Loam"))
    assert r["verdict"] == "unknown"


def test_parcel_soil_unavailable_is_unknown():
    r = assess_soil_suitability(WHEAT, {"data_available": False, "source": "unavailable"})
    assert r["verdict"] == "unknown"
    assert r["confidence"] == "low"
    assert "soil" in r["reason"].lower()


def test_no_assessable_dimension_is_unknown():
    # Tolerance without pH bounds or textures; parcel with only awc → nothing to compare.
    r = assess_soil_suitability({"drainage": ["poor"]}, _soil(6.5, None))
    assert r["verdict"] == "unknown"


def test_ph_only_verdict_has_lower_confidence_than_two_dimensions():
    # pH assessable, texture not (parcel texture missing) → still a verdict, lower conf.
    ph_only = assess_soil_suitability(WHEAT, _soil(7.0, None))
    two_dim = assess_soil_suitability(WHEAT, _soil(7.0, "Loam"))
    assert ph_only["verdict"] == "suitable"
    assert two_dim["confidence"] == "medium"
    assert ph_only["confidence"] == "low"


# ── extrapolate_varieties wiring (real graph + mocked Soil module) ───────────

pytestmark_docker = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker unavailable for testcontainers"
)

_loop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


@pytestmark_docker
class TestExtrapolateSoilGate:
    @pytest.fixture(scope="class")
    def dao(self):
        from neo4j import AsyncGraphDatabase
        from testcontainers.neo4j import Neo4jContainer

        from app.graph.dao import GraphDAO
        with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
            driver = AsyncGraphDatabase.driver(
                n.get_connection_url(), auth=(n.username, n.password),
            )
            # One Csa site + two varieties of an acidophile crop (blueberry-like).
            # Species name = canonical slug; EPPO 'VACCX' resolves to it via registry.
            _run(self._seed(driver))
            yield GraphDAO(driver)
            _run(driver.close())

    @staticmethod
    async def _seed(driver):
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
            await s.run(
                """
                CREATE (site:TrialSite {name:'SiteA', climateClass:'Csa', annualRainfallMm:600})
                CREATE (t1:VarietyTrial {cropEppo:'HORVX', varietyNormalized:'V1', variety:'V1', year:2020, yieldKgHa:8000.0})
                CREATE (t2:VarietyTrial {cropEppo:'HORVX', varietyNormalized:'V2', variety:'V2', year:2020, yieldKgHa:7000.0})
                CREATE (t1)-[:TRIAL_AT]->(site)
                CREATE (t2)-[:TRIAL_AT]->(site)
                CREATE (sp:Species {name:'barley'})
                CREATE (ss:CropSoilSuitability {species:'barley', phMin:6.0, phMax:8.5, textures:['loam']})
                CREATE (sp)-[:HAS_SOIL_SUITABILITY]->(ss)
                """
            )

    def test_unsuitable_crop_varieties_dropped(self, dao):
        # Parcel is strongly acidic (pH 4.5) → barley (min 6.0) unsuitable → drop all.
        acid = _soil(4.5, "Sandy loam")
        with patch(
            "app.graph.dao.get_parcel_soil_properties",
            new=AsyncMock(return_value=acid),
        ):
            res = _run(dao.extrapolate_varieties(
                crop="HORVX", climate_class="Csa",
                rainfall_min=400, rainfall_max=800,
                filter_soil_suitability=True,
                parcel_id="urn:ngsi-ld:AgriParcel:test:1", tenant_id="t",
            ))
        assert res["ranked_varieties"] == []
        assert res["soil_gate"]["verdict"] == "unsuitable"
        assert len(res["excluded_by_soil"]) == 2

    def test_soil_suitability_coverage(self, dao):
        # Seed has 1 species (barley) with tolerance → 100% of species covered.
        cov = _run(dao.soil_suitability_coverage())
        assert cov["species_total"] == 1
        assert cov["species_with_tolerance"] == 1
        assert cov["coverage_pct"] == 100.0

    def test_suitable_crop_varieties_kept_and_annotated(self, dao):
        ok = _soil(7.0, "Loam")
        with patch(
            "app.graph.dao.get_parcel_soil_properties",
            new=AsyncMock(return_value=ok),
        ):
            res = _run(dao.extrapolate_varieties(
                crop="HORVX", climate_class="Csa",
                rainfall_min=400, rainfall_max=800,
                filter_soil_suitability=True,
                parcel_id="urn:ngsi-ld:AgriParcel:test:1", tenant_id="t",
            ))
        assert len(res["ranked_varieties"]) == 2
        assert res["soil_gate"]["verdict"] == "suitable"
        # AgronomicValue envelope present on the gate result.
        assert res["soil_gate"]["agronomic"]["value"] == "suitable"
        assert res["soil_gate"]["agronomic"]["confidence"] in ("high", "medium", "low")
