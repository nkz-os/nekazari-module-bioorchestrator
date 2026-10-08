"""Organic and conventional units are never pooled (evidence policy rule 10), on real Neo4j:
a request with no production system, or a conventional one, leaves organic units out; an organic
request reads ONLY organic units. Every evidence path honours it."""
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

_loop = asyncio.new_event_loop()
_FIELD = ["pf01", "pf02", "pf03"]
_AGG = ["Agg P"]
_SITES = (
    [{"name": n, "climateClass": "Cfb", "siteKind": "field"} for n in _FIELD]
    + [{"name": "Agg P", "climateClass": "Cfb", "siteKind": "aggregate", "country": "ES"}]
)
_NON_ORGANIC = {"V1", "V2"}
_ORGANIC = {"VO1", "VO2", "VO3"}


def _t(crop, variety, site, kg, system, **props):
    base = {"cropEppo": crop, "varietyNormalized": variety, "variety": variety, "year": 2020, "aggregationScope": "site",
            "source_id": "SRC", "mergeKey": f"{crop}|{variety}|{site}", "yieldKgHa": kg,
            "productionSystem": system, **props}
    return {"props": {k: v for k, v in base.items() if v is not None}, "site": site}


_TRIALS = [
    # TRZAX at field sites: two non-organic (one states conventional, one states nothing) and
    # three organic under different spellings.
    _t("TRZAX", "V1", "pf01", 6000.0, "conventional"),
    _t("TRZAX", "V2", "pf02", 7000.0, None),
    _t("TRZAX", "VO1", "pf01", 3000.0, "organic"),
    _t("TRZAX", "VO2", "pf02", 3200.0, "ecológico"),
    _t("TRZAX", "VO3", "pf03", 2900.0, " Ecologico "),
    # ZEAMX: organic only.
    _t("ZEAMX", "MO1", "pf01", 4000.0, "organic"),
    # HORVX at the aggregate site (regional tier): one each.
    _t("HORVX", "H1", "Agg P", 5000.0, "conventional", source_id="AHDB"),
    _t("HORVX", "HO1", "Agg P", 2000.0, "organic", source_id="AHDB"),
    # SECCE: an excluded-source (BSL) organic trial at the aggregate site: presence for organic only.
    _t("SECCE", "R1", "Agg P", 3000.0, "organic", source_id="BSL", aggregationScope="regional"),
]


def _run(coro):
    return _loop.run_until_complete(coro)


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
                    """UNWIND $trials AS t CREATE (vt:VarietyTrial) SET vt = t.props
                    WITH vt, t MATCH (ts:TrialSite {name: t.site}) CREATE (vt)-[:TRIAL_AT]->(ts)""",
                    trials=_TRIALS)
        _run(seed())
        yield d
        _run(driver.close())


@pytest.fixture(autouse=True)
def _clear_caches():
    dao_mod._RECOMMEND_CACHE.clear()
    yield
    dao_mod._RECOMMEND_CACHE.clear()


def _sites(names):
    return [{"name": n, "distance": None} for n in names]


def _ranked(dao, crop, management, **kw):
    out = _run(dao.extrapolate_varieties(
        crop, similar_sites_override=_sites(_FIELD), management=management, top_n=10, **kw))
    return {v["variety"]: v for v in out["ranked_varieties"]}


@pytest.mark.parametrize("management", [None, "any", "conventional"])
def test_unspecified_or_conventional_request_leaves_organic_out(dao, management):
    assert set(_ranked(dao, "TRZAX", management)) == _NON_ORGANIC
    assert _ranked(dao, "ZEAMX", management) == {}  # organic-only crop: no evidence, no fallback


def test_organic_request_reads_only_organic_units(dao):
    got = _ranked(dao, "TRZAX", "organic")
    assert set(got) == _ORGANIC
    assert _ranked(dao, "ZEAMX", "organic").keys() == {"MO1"}


def test_means_are_never_pooled(dao):
    def mean(management):
        out = _run(dao.extrapolate_varieties(
            "TRZAX", similar_sites_override=_sites(_FIELD), management=management, top_n=10))
        ranked = out["ranked_varieties"]
        return sum(v["mean_yield_kg_ha"] for v in ranked) / len(ranked)
    assert mean(None) == pytest.approx(6500.0)
    assert mean("organic") == pytest.approx((3000.0 + 3200.0 + 2900.0) / 3)


def test_batch_equals_single_for_each_request(dao):
    for management in (None, "organic"):
        batch = _run(dao.extrapolate_varieties_batch(
            ["TRZAX", "ZEAMX"], _sites(_FIELD), management=management, top_n=10))
        for crop in ("TRZAX", "ZEAMX"):
            single = _ranked(dao, crop, management)
            assert {v["variety"]: v["mean_yield_kg_ha"] for v in batch[crop]} == \
                {k: v["mean_yield_kg_ha"] for k, v in single.items()}


def test_regional_tier_honours_it(dao):
    def regional(management):
        out = _run(dao.extrapolate_varieties_batch(
            ["HORVX"], _sites(_AGG), management=management, top_n=10, tier="regional"))
        return {v["variety"] for v in out["HORVX"]}
    assert regional(None) == {"H1"} and regional("conventional") == {"H1"}
    assert regional("organic") == {"HO1"}


def test_variety_trials_listing(dao):
    def listed(management):
        rows = _run(dao.get_variety_trials(crop="TRZAX", limit=50, management=management))
        return {r["variety"] for r in rows}
    assert listed(None) == _NON_ORGANIC and listed("any") == _NON_ORGANIC
    assert listed("organic") == _ORGANIC


def test_evidence_page(dao):
    def page(management):
        res = _run(dao.list_trial_evidence(
            crop="TRZAX", similar_sites=_FIELD, variety=None, irrigation_uri=None,
            page=1, page_size=50, management=management))
        return res["total"], {i["variety"] for i in res["items"]}
    assert page(None) == (2, _NON_ORGANIC)
    assert page("organic") == (3, _ORGANIC)


def test_prefilter_follows_the_request(dao):
    eppos = ["TRZAX", "ZEAMX"]
    assert _run(dao._crops_with_analog_trials(eppos, _FIELD)) == {"TRZAX"}
    assert _run(dao._crops_with_analog_trials(eppos, _FIELD, management="organic")) == {"TRZAX", "ZEAMX"}


def test_presence_scan_follows_the_request(dao):
    assert _run(dao.regional_presence_trials(["SECCE"], _AGG)) == {}
    assert set(_run(dao.regional_presence_trials(["SECCE"], _AGG, management="organic"))) == {"SECCE"}


def test_excluded_organic_units_are_counted(dao):
    counts = _run(dao.organic_units_excluded(["TRZAX", "ZEAMX", "HORVX", "LYPES"], _FIELD + _AGG))
    assert counts == {"TRZAX": 3, "ZEAMX": 1, "HORVX": 1}


def _recommend(dao, management):
    dao_mod._RECOMMEND_CACHE.clear()
    cond = {"climate_class": "Cfb", "soil_type": None, "irrigation_regime": None,
            "management": management, "season": "all", "top_n": 30}
    out = _run(dao.recommend_for_conditions(cond))
    return {r["crop"]["eppo"]: r for r in out["recommendations"]}


def test_recommend_unspecified_excludes_organic_and_says_so(dao):
    recs = _recommend(dao, "any")
    assert "ZEAMX" not in recs  # organic-only crop
    wheat = recs["TRZAX"]
    assert wheat["yield"]["expected_kg_ha"] == pytest.approx(7000.0)  # best non-organic variety
    assert "organic_units_excluded" in wheat["trust"]["data_gaps"]
    assert recs["HORVX"]["yield"]["expected_kg_ha"] == pytest.approx(5000.0)
    assert "SECCE" not in recs  # the organic BSL trial is no presence for a non-organic request


def test_recommend_organic_reads_organic_only_with_no_yield_factor(dao):
    recs = _recommend(dao, "organic")
    assert set(recs) == {"TRZAX", "ZEAMX", "HORVX", "SECCE"}
    wheat = recs["TRZAX"]
    assert wheat["yield"]["expected_kg_ha"] == pytest.approx(3200.0)  # best organic variety, unscaled
    assert "organic_units_excluded" not in wheat["trust"]["data_gaps"]
    assert "organic_yield_factor" not in {a["id"] for a in wheat["assumptions"]}
    assert recs["HORVX"]["yield"]["expected_kg_ha"] == pytest.approx(2000.0)
