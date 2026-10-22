"""Parcel zone matching of the regional tier (T11c): a Spanish parcel is placed in GENVCE's own climatic
zones from its cached CHELSA cell; zone-matched units come first, the country level (units whose zone cannot
be told) only for crops with no zone-matched evidence, never a unit decidably in another zone. Real Neo4j;
the parcel point is synthetic."""
from __future__ import annotations

import asyncio
import json
import re
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph import dao as dao_mod
from app.graph import evidence_policy as ep
from app.graph import zone_match
from app.graph.dao import GraphDAO
from app.kg.zone_definitions import ZONE_MATCH_BASIS, zone_key
from app.services.chelsa_climate import cell_key
from neo4j import AsyncGraphDatabase

pytestmark = [
    pytest.mark.real_batch,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable for testcontainers"),
]

_loop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


COLD_PARCEL = (41.0, -3.0)   # synthetic point
WARM_PARCEL = (37.0, -5.0)   # synthetic point
NO_CELL_PARCEL = (40.0, -4.0)
DEF = "genvce-winter-cereals-2023-24"
_SITES = [
    {"name": "Zona Fría", "siteKey": "ES-Z1", "siteKind": "aggregate", "country": "ES"},
    {"name": "Zona Cálida", "siteKey": "ES-Z2", "siteKind": "aggregate", "country": "ES"},
    {"name": "General", "siteKey": "ES-G", "siteKind": "aggregate", "country": "ES"},
    # no country property (the legacy production shape): admitted by its climate class
    {"name": "Sin Pais", "siteKey": "XX-N", "siteKind": "aggregate", "climateClass": "Csb"},
]
# crop, variety, kg, site, zoneKey
_TRIALS = [
    ("HORVX", "A", 6000.0, "Zona Fría", zone_key(DEF, "Zona Fría")),
    ("HORVX", "B", 9000.0, "Zona Cálida", zone_key(DEF, "Zona Cálida")),
    ("HORVX", "C", 7000.0, "General", None),
    ("TRZAX", "D", 4000.0, "General", None),
    ("TRZAX", "E", 8000.0, "Zona Cálida", zone_key(DEF, "Zona Cálida")),
    ("ZEAMX", "F", 11000.0, "Sin Pais", None),
]


def _tas(april: float) -> list[float]:
    t = [8.0, 9.0, 11.0, april, 16.0, 20.0, 23.0, 23.0, 19.0, 14.0, 10.0, 8.0]
    return t


def _cell(lat_lon, april, rain):
    return {"key": cell_key(*lat_lon), "monthlyTasC": _tas(april), "annualRainfallMm": rain,
            "koppen": "Csb", "annualTempC": 13.0, "source": "test"}


@pytest.fixture(scope="module")
def dao():
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        driver = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        d = GraphDAO(driver)

        async def seed():
            async with driver.session() as s:
                await s.run("CREATE INDEX trial_site_name IF NOT EXISTS FOR (ts:TrialSite) ON (ts.name)")
                await s.run("UNWIND $sites AS s CREATE (t:TrialSite) SET t = s", sites=_SITES)
                await s.run(
                    """UNWIND $rows AS r MATCH (ts:TrialSite {name: r.site})
                    CREATE (vt:VarietyTrial {cropEppo: r.crop, varietyNormalized: r.variety, yieldKgHa: r.kg,
                                             year: 2024, aggregationScope: 'site', source_id: 'GENVCE',
                                             mergeKey: r.crop + r.variety, zoneKey: r.zk})
                    CREATE (vt)-[:TRIAL_AT]->(ts)""",
                    rows=[{"crop": c, "variety": v, "kg": k, "site": s_, "zk": z} for c, v, k, s_, z in _TRIALS])
                # units the answer never reads: an excluded source (BSL) and a forage record in main mode
                await s.run(
                    """MATCH (ts:TrialSite {name: 'Zona Cálida'})
                    CREATE (:VarietyTrial {cropEppo: 'BSLCR', varietyNormalized: 'X', yieldKgHa: 5000.0, year: 2024,
                                           aggregationScope: 'site', source_id: 'BSL', mergeKey: 'bsl',
                                           zoneKey: $zk})-[:TRIAL_AT]->(ts)
                    CREATE (:VarietyTrial {cropEppo: 'FORCR', varietyNormalized: 'Y', yieldKgHa: 5000.0, year: 2024,
                                           aggregationScope: 'site', source_id: 'GENVCE', mergeKey: 'forage',
                                           qualityParams: '{"ndf_pct": 40}', yieldBasis: 'dry_matter', zoneKey: $zk})-[:TRIAL_AT]->(ts)""",
                    zk=zone_key(DEF, "Zona Cálida"))
                for ll, april, rain in ((COLD_PARCEL, 9.0, 450.0), (WARM_PARCEL, 14.5, 400.0)):
                    await s.run("CREATE (c:ClimateCell) SET c = $c", c=_cell(ll, april, rain))
        _run(seed())
        yield d
        _run(driver.close())


def _recommend(dao, point, crops=("HORVX", "TRZAX"), country="ES", regime=None):
    dao_mod._RECOMMEND_CACHE.clear()
    cond = {"climate_class": "Csb", "country": country, "crops": list(crops), "top_n": 10, "management": "any",
            "season": "all", "purpose": "main", "irrigation_regime": regime}
    if point:
        cond["lat"], cond["lon"] = point
    out = _run(dao.recommend_for_conditions(cond))
    return {r["crop"]["eppo"]: r for r in out["recommendations"]}


def test_a_cold_parcel_gets_its_own_zone_and_the_label_the_citation_and_the_basis(dao):
    rec = _recommend(dao, COLD_PARCEL)["HORVX"]
    assert rec["yield"]["expected_kg_ha"] == 6000.0  # only the cold zone; not 9000, not the country level 7000
    zm = rec["evidence"]["zone_match"]
    assert zm["status"] == "matched" and zm["basis"] == ZONE_MATCH_BASIS and "climatology" in zm["caveat"]
    assert zm["matched_zones"] == [{
        "definition_id": DEF, "zone_label": "Zona Fría",
        "citation": "GENVCE report, winter cereals, campaign 2023-2024, section 2.1.3 Zonas de experimentación, PDF page 3"}]
    assert zm["parcel"] == {"april_mean_temp_c": 9.0, "annual_rainfall_mm": 450}
    gaps = rec["trust"]["data_gaps"]
    assert "regional_zone_matched" in gaps and "zone_match_climatology_basis" in gaps
    assert "regional_country_level" not in gaps and rec["trust"]["level"] == "low"


def test_a_warm_parcel_gets_the_warm_zone(dao):
    assert _recommend(dao, WARM_PARCEL)["HORVX"]["yield"]["expected_kg_ha"] == 9000.0


def test_a_crop_without_zone_matched_evidence_falls_back_to_the_country_level_without_the_other_zone(dao):
    # TRZAX has a warm-zone unit (decidably another zone for the cold parcel: excluded) and a country-level one
    rec = _recommend(dao, COLD_PARCEL)["TRZAX"]
    assert rec["yield"]["expected_kg_ha"] == 4000.0
    zm = rec["evidence"]["zone_match"]
    assert zm["status"] == "country_level" and zm["matched_zones"] == [] and zm["basis"] == ZONE_MATCH_BASIS
    gaps = rec["trust"]["data_gaps"]
    assert "regional_country_level" in gaps and "regional_zone_matched" not in gaps


def test_sites_without_a_country_are_matched_by_climate_and_get_no_country_gap(dao):
    rec = _recommend(dao, COLD_PARCEL, crops=("ZEAMX",))["ZEAMX"]
    assert rec["yield"]["expected_kg_ha"] == 11000.0
    assert "regional_country_level" not in rec["trust"]["data_gaps"]
    # the same request does get the gap where the evidence sits at a country-admitted site
    assert "regional_country_level" in _recommend(dao, COLD_PARCEL)["TRZAX"]["trust"]["data_gaps"]


def test_zone_keys_follow_the_row_policy_and_purpose_gate_of_the_answer(dao):
    ctx = _run(dao.resolve_zone_context("ES", *WARM_PARCEL, None))
    names = [s["name"] for s in _SITES]
    keys = _run(dao.regional_zone_keys(["HORVX", "BSLCR", "FORCR"], names, ctx))
    assert keys.get("HORVX") == [zone_key(DEF, "Zona Cálida")]
    assert "BSLCR" not in keys     # excluded source: never named as a matched zone
    assert "FORCR" not in keys     # forage record under the main purpose
    forage = _run(dao.regional_zone_keys(["FORCR", "HORVX"], names, ctx, purpose="forage"))
    assert "FORCR" in forage and "HORVX" not in forage


def test_no_parcel_climate_means_country_level_and_says_why(dao):
    rec = _recommend(dao, NO_CELL_PARCEL)["HORVX"]
    assert rec["yield"]["expected_kg_ha"] == 9000.0  # no zone filter: the best variety of all three units
    zm = rec["evidence"]["zone_match"]
    assert zm["status"] == "unavailable" and zm["reason"] == "parcel_climate_unavailable"
    assert "regional_country_level" in rec["trust"]["data_gaps"]


def test_without_a_point_or_outside_spain_zone_matching_is_not_asked(dao):
    rec = _recommend(dao, None)["HORVX"]
    assert "zone_match" not in rec["evidence"]
    assert rec["yield"]["expected_kg_ha"] == 9000.0  # no zone filter: the best variety of all three units


def test_the_zone_id_carries_the_classification_and_no_coordinate(dao):
    zm = _recommend(dao, COLD_PARCEL)["HORVX"]["evidence"]["zone_match"]
    ctx = zone_match.context_from_zone_id(zm["zone_id"], None)
    direct = _run(dao.resolve_zone_context("ES", *COLD_PARCEL, None))
    assert ctx.allow == direct.allow and ctx.deny == direct.deny
    for text in (json.dumps(zm), zm["zone_id"]):
        assert str(COLD_PARCEL[0]) not in text and str(abs(COLD_PARCEL[1])) not in text
    assert zone_match.context_from_zone_id("41.0,-3.0", None) is None


def test_the_zone_id_holds_threshold_classes_never_climate_values():
    ctx = zone_match.context_from_values(9.0, 450.0, None)
    zid = zone_match.zone_id(ctx)
    assert not re.search(r"\d+\.\d|(?<![a-z0-9-])\d", zid.replace(DEF, ""))  # no number besides the definition id
    # a different parcel in the same classes has the same id; one in another class does not
    assert zone_match.zone_id(zone_match.context_from_values(9.3, 470.0, None)) == zid
    assert zone_match.zone_id(zone_match.context_from_values(14.5, 400.0, None)) != zid
    back = zone_match.context_from_zone_id(zid, None)
    assert back.allow == ctx.allow and back.deny == ctx.deny
    assert zone_match.context_from_zone_id("9.0_450", None) is None  # the former value-carrying id


def test_zone_matching_reads_the_regime_with_the_shared_normaliser():
    # request spellings only the evidence filter's normaliser handles (the old private parser did not)
    for spelling, regime in (("rainfed", "secano"), ("Irrigated", "regadio"), ("regadío", "regadio")):
        assert zone_match.normalise_regime(spelling) == regime == ep.irrigation_regime(ep.irrigation_uri(spelling))
        assert zone_match.context_from_values(9.0, 450.0, spelling).zones.regime == regime
    assert zone_match.normalise_regime("whatever") is None


def test_the_evidence_list_follows_the_same_pool_rule(dao):
    def ctx(point):
        # what the route does: the recommendation's opaque zone id, never the point
        zm = _recommend(dao, point)["HORVX"]["evidence"].get("zone_match") or {}
        return zone_match.context_from_zone_id(zm["zone_id"], None) if zm.get("zone_id") else None

    sites = [s["name"] for s in _SITES]

    def listing(crop, point):
        return _run(dao.list_trial_evidence(
            crop=crop, similar_sites=sites, variety=None, irrigation_uri=None, page=1, page_size=50,
            purpose="main", tier="regional", zone_ctx=ctx(point)))
    cold = listing("HORVX", COLD_PARCEL)
    assert [i["variety"] for i in cold["items"]] == ["A"] and cold["zone_pool"] == "matched"
    fall = listing("TRZAX", COLD_PARCEL)
    assert [i["variety"] for i in fall["items"]] == ["D"] and fall["zone_pool"] == "fallback"
    none = listing("HORVX", NO_CELL_PARCEL)
    assert sorted(i["variety"] for i in none["items"]) == ["A", "B", "C"] and "zone_pool" not in none
