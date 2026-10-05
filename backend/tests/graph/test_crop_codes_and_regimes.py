"""Crop codes and irrigation literals on real Neo4j.

* Maize is stored under ZEAMX in the trials and the graph, while the registry's code is ZEAMA:
  the recommendation must still read the species' frost and soil tolerance, and the crop catalog
  lists the exact sibling codes once (no other code is merged).
* The irrigation regime is stored as an AGROVOC URI, but one source stores the literals
  "secano"/"regadío": every regime filter must count both spellings.
"""
from __future__ import annotations

import asyncio
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph import dao as dao_mod
from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

pytestmark = [
    pytest.mark.real_batch,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable for testcontainers"),
]

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
_PW = "testpassword"


def _run(coro):
    return _loop.run_until_complete(coro)


_SITES = [{"name": "field-a", "climateClass": "Cfb"}, {"name": "field-b", "climateClass": "Cfb"}]


def _t(crop, variety, site, kg, **props):
    base = {"cropEppo": crop, "varietyNormalized": variety, "variety": variety, "year": 2020,
            "aggregationScope": "site", "source_id": "SRC", "yieldKgHa": kg, "confidence": "high",
            "mergeKey": f"{crop}|{variety}|{site}|{kg}|{sorted(props.items())}", **props}
    return {"props": base, "sites": [site]}


_TRIALS = [
    # maize: the trials and the graph use ZEAMX; two ZEAMA twins of ZEAMX trials plus one of its own
    _t("ZEAMX", "M1", "field-a", 12000.0), _t("ZEAMX", "M2", "field-a", 13000.0),
    _t("ZEAMA", "M1", "field-a", 12000.0, mergeKey="twin-1"),
    _t("ZEAMA", "M3", "field-b", 9000.0, mergeKey="twin-2"),
    # codes that stay separate in the listing (owner decision)
    _t("BRSNN", "R1", "field-a", 3000.0), _t("BRSNW", "R2", "field-a", 3200.0),
    _t("PIBSX", "P1", "field-a", 2500.0), _t("PIBAR", "P2", "field-a", 2600.0),
    _t("CIEAR", "C1", "field-a", 1500.0), _t("CIEAS", "C2", "field-a", 1600.0),
]


@pytest.fixture(scope="module")
def dao():
    with Neo4jContainer("neo4j:5.26-community", password=_PW) as n:
        driver = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        d = GraphDAO(driver)
        _run(_seed(d))
        yield d
        _run(driver.close())


async def _seed(dao):
    async with dao._driver.session() as s:
        await s.run("CREATE INDEX trial_site_name IF NOT EXISTS FOR (ts:TrialSite) ON (ts.name)")
        await s.run("UNWIND $sites AS s CREATE (t:TrialSite) SET t = s", sites=_SITES)
        await s.run(
            """
            UNWIND $trials AS t
            CREATE (vt:VarietyTrial) SET vt = t.props
            WITH vt, t
            UNWIND t.sites AS n
            MATCH (ts:TrialSite {name: n})
            CREATE (vt)-[:TRIAL_AT]->(ts)
            """,
            trials=_TRIALS,
        )
        # The graph's own Species node of maize carries the code the trials use.
        await s.run(
            """
            CREATE (sp:Species {name: 'maize', eppoCode: 'ZEAMX'})
            CREATE (sp)-[:HAS_HEAT_TOLERANCE]->(:CropHeatTolerance
                {frostDamageThresholdC: -2.0, heatDamageThresholdC: 35.0})
            CREATE (sp)-[:HAS_SOIL_SUITABILITY]->(:CropSoilSuitability
                {phMin: 5.5, phMax: 7.5, textures: ['loam'], drainage: ['well_drained']})
            """
        )


@pytest.fixture(autouse=True)
def _clear_caches():
    dao_mod._RECOMMEND_CACHE.clear()
    yield
    dao_mod._RECOMMEND_CACHE.clear()


# ── crop codes ───────────────────────────────────────────────────────────────

def test_catalog_lists_exact_sibling_codes_once_and_merges_nothing_else(dao):
    by = {c["eppo_code"]: c for c in _run(dao.get_available_crops())}
    assert "ZEAMA" not in by
    maize = by["ZEAMX"]
    # distinct varieties over both codes (M1 is shared by the twin), every trial node counted
    assert (maize["variety_count"], maize["trial_count"]) == (3, 4)
    assert maize["first_year"] == maize["last_year"] == 2020
    for code in ("BRSNN", "BRSNW", "PIBSX", "PIBAR", "CIEAR", "CIEAS"):
        assert by[code]["trial_count"] == 1


def test_maize_regains_frost_and_soil_verdicts_in_recommend(dao):
    cond = {"climate_class": "Cfb", "soil_type": None, "irrigation_regime": None, "management": "any",
            "season": "all", "top_n": 30, "coldest_month_min_c": -6.0, "soil_ph": 7.0,
            "soil_texture": "loam", "crops": ["ZEAMX"]}
    out = _run(dao.recommend_for_conditions(cond))
    rec = next(r for r in out["recommendations"] if r["crop"]["eppo"] == "ZEAMX")
    assert rec["suitability"]["frost"]["level"] == "risk"  # -6 C - margin <= -2 C
    assert rec["suitability"]["soil"]["level"] == "suitable"
    gaps = rec["trust"]["data_gaps"]
    assert "frost_tolerance_unavailable" not in gaps
    assert not any("soil_tolerance" in g for g in gaps)
