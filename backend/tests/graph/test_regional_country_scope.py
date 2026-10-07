"""Aggregate sites of the regional tier (task 11): declared by ``TrialSite.siteKind`` (not only by name) and
matched to a parcel by COUNTRY when they have one (a national network's zone means have no coordinates, so no
climate); an aggregate with no country keeps the climate-class match. Real Neo4j."""
from __future__ import annotations

import asyncio
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph import dao as dao_mod
from app.graph import evidence_policy as ep
from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

pytestmark = [
    pytest.mark.real_batch,
    pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable for testcontainers"),
]

_loop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


# 'Zona Fría' / 'Zona Cálida' read as field sites by name; only the declared kind makes them aggregates.
_SITES = [
    {"name": "Zona Fría", "siteKey": "ES-Z1", "siteKind": "aggregate", "country": "ES"},
    {"name": "Zona Cálida", "siteKey": "ES-Z2", "siteKind": "aggregate", "country": "ES"},
    {"name": "Zona Italiana", "siteKey": "IT-Z1", "siteKind": "aggregate", "country": "IT"},
    {"name": "Legacy declared aggregate", "siteKey": "LEG", "siteKind": "aggregate", "climateClass": "Csa"},
    {"name": "Zona sin tipo", "climateClass": "Csa"},  # an old graph: no siteKind, no country: a field site
    {"name": "Campo Csa", "siteKey": "F1", "siteKind": "field", "country": "ES", "climateClass": "Csa",
     "annualRainfallMm": 500.0, "annualET0Mm": 1000.0, "frostDaysPerYear": 10.0, "elevationM": 300.0},
]
_TRIALS = [
    ("HORVX", "A", 5000.0, "Zona Fría"), ("HORVX", "B", 6000.0, "Zona Cálida"),
    ("HORVX", "C", 7000.0, "Zona Italiana"), ("HORVX", "D", 8000.0, "Legacy declared aggregate"),
    ("HORVX", "E", 9000.0, "Campo Csa"), ("HORVX", "F", 4000.0, "Zona sin tipo"),
]


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
                                             year: 2020, aggregationScope: 'site', source_id: 'S',
                                             mergeKey: r.crop + r.variety})
                    CREATE (vt)-[:TRIAL_AT]->(ts)""",
                    rows=[{"crop": c, "variety": v, "kg": k, "site": s_} for c, v, k, s_ in _TRIALS])
        _run(seed())
        yield d
        _run(driver.close())


def _names(sites):
    return sorted(s["name"] for s in sites)


def test_a_declared_aggregate_is_an_aggregate_whatever_its_name(dao):
    sites = _run(dao.get_similar_sites(climate_class="Csa", limit=None, include_aggregate=True, country="ES"))
    kinds = {s["name"]: s["site_kind"] for s in sites}
    assert kinds["Zona Fría"] == "aggregate" and kinds["Legacy declared aggregate"] == "aggregate"
    assert kinds["Zona sin tipo"] == "field" and kinds["Campo Csa"] == "field"


def test_aggregates_with_a_country_match_by_country_the_others_by_climate(dao):
    es = _run(dao.get_similar_sites(climate_class="Csa", limit=None, include_aggregate=True, country="ES"))
    agg = [s["name"] for s in es if s["site_kind"] == "aggregate"]
    assert sorted(agg) == ["Legacy declared aggregate", "Zona Cálida", "Zona Fría"]  # not the Italian one
    it = _run(dao.get_similar_sites(climate_class="Csa", limit=None, include_aggregate=True, country="it"))
    assert sorted(s["name"] for s in it if s["site_kind"] == "aggregate") == [
        "Legacy declared aggregate", "Zona Italiana"]
    # the zone means have no climate: another Köppen class of the same country still gets them
    cfb = _run(dao.get_similar_sites(climate_class="Cfb", limit=None, include_aggregate=True, country="ES"))
    assert sorted(s["name"] for s in cfb if s["site_kind"] == "aggregate") == ["Zona Cálida", "Zona Fría"]


def test_without_a_country_only_climate_aggregates_match(dao):
    sites = _run(dao.get_similar_sites(climate_class="Csa", limit=None, include_aggregate=True))
    assert sorted(s["name"] for s in sites if s["site_kind"] == "aggregate") == ["Legacy declared aggregate"]


def test_aggregates_stay_out_of_the_default_field_lookup(dao):
    sites = _run(dao.get_similar_sites(climate_class="Csa", limit=None, country="ES"))
    assert _names(sites) == ["Campo Csa", "Zona sin tipo"]


def test_the_distance_path_keeps_the_country_match_after_real_analogs(dao):
    target = {"rainfall": 500.0, "et0": 1000.0, "frost": 10.0, "elevation": 300.0}
    sites = _run(dao.get_similar_sites(climate_class="Csa", limit=None, include_aggregate=True, country="ES",
                                       target_features=target))
    by_name = {s["name"]: s for s in sites}
    assert "Zona Italiana" not in by_name
    assert by_name["Zona Fría"]["distance"] == by_name["Zona Cálida"]["distance"] > by_name["Campo Csa"]["distance"]


def test_the_tier_follows_the_declared_kind(dao):
    regional = _run(dao.list_trial_evidence(
        crop="HORVX", similar_sites=["Zona Fría", "Zona Cálida", "Campo Csa", "Zona sin tipo"], variety=None,
        irrigation_uri=None, page=1, page_size=50, purpose="main", tier="regional"))
    assert sorted(i["variety"] for i in regional["items"]) == ["A", "B"]
    assert {i["tier"] for i in regional["items"]} == {"regional"}
    field = _run(dao.list_trial_evidence(
        crop="HORVX", similar_sites=["Zona Fría", "Zona Cálida", "Campo Csa", "Zona sin tipo"], variety=None,
        irrigation_uri=None, page=1, page_size=50, purpose="main", tier="field"))
    assert sorted(i["variety"] for i in field["items"]) == ["E", "F"]  # E is the field site; F has no declared kind


def test_recommend_uses_the_parcel_country_for_the_regional_tier(dao):
    def run(country):
        dao_mod._RECOMMEND_CACHE.clear()
        return _run(dao.recommend_for_conditions({
            "climate_class": "Cfb", "country": country, "crops": ["HORVX"], "top_n": 10, "management": "any",
            "season": "all", "purpose": "main"}))
    es, fr = run("ES"), run(None)
    assert [r["evidence"]["tier"] for r in es["recommendations"]] == ["regional"]
    assert sorted(es["recommendations"][0]["evidence"]["sites"]) == ["Zona Cálida", "Zona Fría"]
    assert fr["recommendations"] == []  # no country, no climate on the zone means: nothing to match, nothing invented


def test_policy_python_twin_reads_the_declared_kind():
    assert ep.is_aggregate_site("Zona Fría", "aggregate") and not ep.is_aggregate_site("Zona Fría")
    assert ep.is_aggregate_site("Media 14 Località", "field")  # the property only ever adds aggregates
    assert ep.evidence_tier("site", "Zona Fría", "Aggregate ") == ep.EVIDENCE_TIER_REGIONAL
    assert ep.evidence_tier("site", "Zona Fría", None) == ep.EVIDENCE_TIER_FIELD
