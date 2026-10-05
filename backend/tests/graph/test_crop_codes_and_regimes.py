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


_SECANO = "http://aims.fao.org/aos/agrovoc/c_6436"
_REGADIO = "http://aims.fao.org/aos/agrovoc/c_3954"
_SITES = [
    {"name": "field-a", "climateClass": "Cfb"}, {"name": "field-b", "climateClass": "Cfb"},
    # irrigation regimes: two field sites and an aggregate container of one climate
    {"name": "reg-a", "climateClass": "Csa"}, {"name": "reg-b", "climateClass": "Csa"},
    {"name": "UK national list", "climateClass": "Csa"},
]
_REG_SITES = [{"name": "reg-a", "distance": None}, {"name": "reg-b", "distance": None}]


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
    # regimes of HORVX: the AGROVOC URI, the literals of the one source that stores them, none
    _t("HORVX", "L1", "reg-a", 4000.0, irrigationRegime="secano"),
    _t("HORVX", "U1", "reg-a", 5000.0, irrigationRegime=_SECANO),
    _t("HORVX", "U2", "reg-b", 6000.0, irrigationRegime=_SECANO),
    _t("HORVX", "L2", "reg-b", 8000.0, irrigationRegime="regadío"),
    _t("HORVX", "U3", "reg-a", 9000.0, irrigationRegime=_REGADIO),
    _t("HORVX", "N1", "reg-b", 7000.0),
    # PU (URIs) and PL (literals): the same two trials, spelled differently
    _t("HORVX", "PU", "reg-a", 4000.0, irrigationRegime=_SECANO),
    _t("HORVX", "PU", "reg-b", 8000.0, irrigationRegime=_REGADIO),
    _t("HORVX", "PL", "reg-a", 4000.0, irrigationRegime="secano"),
    _t("HORVX", "PL", "reg-b", 8000.0, irrigationRegime="regadío"),
    # a crop whose only evidence is one literal trial (any case), and a BSL trial of the aggregate site
    _t("LITONLY", "T1", "reg-a", 3000.0, irrigationRegime="Secano"),
    _t("SECCE", "B1", "UK national list", 3500.0, source_id="BSL", aggregationScope="regional",
       irrigationRegime="secano"),
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


# ── irrigation literals ──────────────────────────────────────────────────────

_SECANO_VARIETIES = {"L1", "U1", "U2", "PU", "PL"}   # a regime-less trial (N1) never matches
_REGADIO_VARIETIES = {"L2", "U3", "PU", "PL"}


def _batch(dao, crops, regime):
    return _run(dao.extrapolate_varieties_batch(crops, _REG_SITES, irrigation_regime=regime, top_n=20))


def _by_variety(rows):
    return {v["variety"]: v for v in rows}


def test_extrapolation_keeps_a_variety_with_a_literal_regime(dao):
    for regime, expected in (("secano", _SECANO_VARIETIES), ("regadío", _REGADIO_VARIETIES),
                             ("rainfed", _SECANO_VARIETIES)):
        rows = _batch(dao, ["HORVX"], regime)["HORVX"]
        assert set(_by_variety(rows)) == expected, regime
    assert set(_by_variety(_batch(dao, ["HORVX"], None)["HORVX"])) == \
        _SECANO_VARIETIES | _REGADIO_VARIETIES | {"N1"}


def test_per_crop_extrapolation_equals_the_batch(dao):
    rows = _batch(dao, ["HORVX"], "secano")["HORVX"]
    single = _run(dao.extrapolate_varieties("HORVX", similar_sites_override=_REG_SITES,
                                            irrigation_regime="secano", top_n=20))
    assert single["ranked_varieties"] == rows


def test_reference_counts_literal_and_uri_trials_of_the_regime(dao):
    def ref(regime):
        rows = _batch(dao, ["HORVX"], regime)["HORVX"]
        assert len({(v["crop_reference_median_kg_ha"], v["crop_reference_n"]) for v in rows}) == 1
        return rows[0]["crop_reference_median_kg_ha"], rows[0]["crop_reference_n"]

    assert ref("secano") == (4000.0, 5)    # 4000 (literal) 4000 4000 5000 6000
    assert ref("regadío") == (8000.0, 4)   # 8000 (literal) 8000 8000 9000
    assert ref(None) == (6500.0, 10)       # both regimes and the trial without one


def test_water_regime_weight_is_the_same_for_a_literal_and_its_uri(dao):
    v = _by_variety(_batch(dao, ["HORVX"], "secano")["HORVX"])
    # one matching trial (4000) and one of the other regime (8000, weight 0.4): the weighted mean
    # is (4000 + 0.4 * 8000) / 1.4, whichever way the two regimes are spelled
    expected = round((4000 + 0.4 * 8000) / 1.4, 1)
    assert v["PU"]["mean_yield_kg_ha"] == v["PL"]["mean_yield_kg_ha"] == pytest.approx(expected, abs=0.1)


def test_prefilter_counts_a_crop_whose_only_trial_is_a_literal(dao):
    names = [s["name"] for s in _REG_SITES]
    crops = ["HORVX", "LITONLY", "TRZAX"]
    assert _run(dao._crops_with_analog_trials(crops, names, irrigation_uri=_SECANO)) == {"HORVX", "LITONLY"}
    assert _run(dao._crops_with_analog_trials(crops, names, irrigation_uri=_REGADIO)) == {"HORVX"}
    assert _run(dao._crops_with_analog_trials(crops, names)) == {"HORVX", "LITONLY"}


def test_evidence_page_lists_literal_and_uri_trials(dao):
    def total(uri):
        page = _run(dao.list_trial_evidence(crop="HORVX", similar_sites=["reg-a", "reg-b"], variety=None,
                                            irrigation_uri=uri, page=1, page_size=50))
        return page["total"], {i["irrigation_regime"] for i in page["items"]}

    assert total(_SECANO) == (5, {"secano", _SECANO})
    assert total(_REGADIO) == (4, {"regadío", _REGADIO})
    assert total(None)[0] == 10


def test_presence_scan_counts_a_literal_regime(dao):
    agg = ["UK national list"]
    assert _run(dao.regional_presence_trials(["SECCE"], agg, irrigation_uri=_SECANO))["SECCE"]["trial_count"] == 1
    assert _run(dao.regional_presence_trials(["SECCE"], agg, irrigation_uri=_REGADIO)) == {}


def test_variety_trials_endpoint_filter_finds_both_spellings(dao):
    def n(regime):
        rows = _run(dao.get_variety_trials(crop="HORVX", irrigation_regime=regime, limit=50))
        return len(rows), {r["irrigation_regime"] for r in rows}

    assert n("secano") == (5, {"secano", _SECANO})
    assert n("regadío") == (4, {"regadío", _REGADIO})
    assert n(None)[0] == 10


def test_recommend_with_a_regime_includes_a_crop_with_only_a_literal_trial(dao):
    cond = {"climate_class": "Csa", "soil_type": None, "management": "any", "season": "all", "top_n": 30,
            "irrigation_regime": "secano"}
    out = _run(dao.recommend_for_conditions(cond))
    recs = {r["crop"]["eppo"]: r for r in out["recommendations"]}
    assert {"HORVX", "LITONLY"} <= set(recs)
    assert recs["HORVX"]["fit"]["reference"]["scope"] == "analog_sites:Csa:secano"
    assert recs["HORVX"]["fit"]["reference"]["n_trials"] == 5
    assert recs["HORVX"]["fit"]["relative_yield_pct"] is not None  # five trials: the floor itself counts
    dao_mod._RECOMMEND_CACHE.clear()
    out = _run(dao.recommend_for_conditions({**cond, "irrigation_regime": "regadío"}))
    assert {r["crop"]["eppo"] for r in out["recommendations"]} >= {"HORVX"}
    assert "LITONLY" not in {r["crop"]["eppo"] for r in out["recommendations"]}
