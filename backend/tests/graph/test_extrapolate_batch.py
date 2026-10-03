"""Batched Köppen extrapolation == per-crop extrapolate_varieties, on real Neo4j."""
from __future__ import annotations

import asyncio
import json
import random
import shutil
from unittest.mock import AsyncMock, patch

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph import dao as dao_mod
from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker unavailable for testcontainers"
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
_PW = "testpassword"
_SECANO = "http://aims.fao.org/aos/agrovoc/c_6436"
_REGADIO = "http://aims.fao.org/aos/agrovoc/c_3954"

# (eppo, scientific name) per crop; AVESA is note-only, LUPAL has ineligible trials only.
_CROPS = [("TRZAX", "Triticum aestivum"), ("TRZDU", "Triticum durum"),
          ("HORVX", "Hordeum vulgare"), ("ZEAMA", "Zea mays"),
          ("AVESA", "Avena sativa"), ("LUPAL", "Lupinus albus")]
_SITES = [
    {"name": "site-a", "climateClass": "Cfb", "soilType": "Loam"},
    {"name": "site-b", "climateClass": "Cfb", "soilType": "Clay"},
    {"name": "site-c", "climateClass": "Cfb", "soilType": "Loam"},
    {"name": "site-d", "climateClass": "Csa", "soilType": "Loam"},
    {"name": "site-a", "climateClass": "Cfb", "soilType": "Loam"},  # same-name duplicate
]


def _run(coro):
    return _loop.run_until_complete(coro)


def _trials(seed: int = 7) -> list[dict]:
    rnd = random.Random(seed)
    out = []
    for i in range(900):
        eppo, sci = rnd.choice(_CROPS)
        ranking = eppo != "LUPAL" and rnd.random() > 0.05
        numeric = eppo != "AVESA" and rnd.random() > 0.25
        # few distinct yields so equal means (ties) occur
        yld = rnd.choice([4000.0, 5000.0, 5000.0, 6000.0, 7250.5]) if numeric else None
        t = {
            "id": i,
            "cropEppo": eppo if rnd.random() > 0.15 else None,
            "cropScientific": rnd.choice([sci, sci.upper(), f"{sci} L.", None]),
            "varietyNormalized": f"{eppo}-V{rnd.randint(1, 12)}",
            "yieldKgHa": yld,
            "yieldNoteS1": None if numeric else rnd.choice(["5", "7"]),
            "rankingEligible": rnd.choice([True, None]) if ranking else False,
            "irrigationRegime": rnd.choice([_SECANO, _REGADIO, None]),
            "year": rnd.choice([None, 2015, 2019, 2022, 2025]),
            "productionSystem": rnd.choice([None, "conventional", "organic"]),
            "diseaseScoresUnified": rnd.choice([None, json.dumps({"rust": {"value": rnd.randint(1, 9)}}),
                                                json.dumps({"rust": {"value": 5}, "mildew": {"value": 3}})]),
            "agronomicTraitsUnified": rnd.choice([None, json.dumps({"height": {"value": rnd.randint(60, 120)}})]),
            "confidence": rnd.choice([None, "high", "medium", "low"]),
            "source_id": rnd.choice([None, "SRC1", "SRC2"]),
            "yieldDerivationMethod": rnd.choice([None, None, "bsl_note"]),
            "sites": sorted({rnd.randrange(len(_SITES)) for _ in range(rnd.choice([1, 1, 2]))}),
        }
        if t["cropEppo"] is None and t["cropScientific"] is None:
            t["cropEppo"] = eppo
        out.append(t)
    # Eight HORVX varieties with the exact same top mean: top_n=5 must cut inside the tie.
    for k in range(8):
        out.append({**out[0], "id": 1000 + k, "cropEppo": "HORVX", "cropScientific": None,
                    "varietyNormalized": f"HORVX-TIE{k}", "yieldKgHa": 99999.0, "yieldNoteS1": None,
                    "rankingEligible": True, "irrigationRegime": None, "year": None, "sites": [k % 3]})
    # PISSA: exact ties spread over sites; their rank depends on the order the sites'
    # trials are scanned, so the batch must walk them like the per-crop query does.
    for k in range(24):
        out.append({**out[0], "id": 3000 + k, "cropEppo": "PISSA", "cropScientific": None,
                    "varietyNormalized": f"PISSA-V{k:02d}", "yieldKgHa": 2000.0, "yieldNoteS1": None,
                    "rankingEligible": True, "irrigationRegime": None, "year": None,
                    "sites": [(k * 7) % 4]})
    # SECCE: two numeric varieties + seven note-only ones (null means tie at the top_n cut).
    for k in range(9):
        numeric = k < 2
        out.append({**out[0], "id": 2000 + k, "cropEppo": "SECCE", "cropScientific": "Secale cereale",
                    "varietyNormalized": f"SECCE-V{k}", "yieldKgHa": 3000.0 + k if numeric else None,
                    "yieldNoteS1": None if numeric else "6", "rankingEligible": True,
                    "sites": [k % 3]})
    return out


@pytest.fixture(scope="module")
def dao():
    with Neo4jContainer("neo4j:5.26-community", password=_PW) as n:
        driver = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        d = GraphDAO(driver)
        _run(_seed(d))
        yield d
        _run(driver.close())


async def _seed(dao):
    sites = [{**s, "idx": i} for i, s in enumerate(_SITES)]
    async with dao._driver.session() as s:
        # Production schema (cypher_migrations 001/009): the per-crop plan, and so the
        # order in which tied means rank, depends on the TrialSite(name) index.
        for stmt in (
            "CREATE INDEX trial_site_name IF NOT EXISTS FOR (ts:TrialSite) ON (ts.name)",
            "CREATE INDEX trial_site_soil IF NOT EXISTS FOR (ts:TrialSite) ON (ts.soilType)",
            "CREATE INDEX variety_trial_yield IF NOT EXISTS FOR (vt:VarietyTrial) ON (vt.yieldKgHa)",
            ("CREATE INDEX variety_trial_source_crop IF NOT EXISTS "
             "FOR (vt:VarietyTrial) ON (vt.source_id, vt.cropEppo)"),
        ):
            await s.run(stmt)
        await s.run("CALL db.awaitIndexes(300)")
        await s.run("MATCH (n) DETACH DELETE n")
        await s.run("UNWIND $sites AS s CREATE (:TrialSite {name: s.name, climateClass: s.climateClass,"
                    " soilType: s.soilType, idx: s.idx})", sites=sites)
        await s.run(
            """
            UNWIND $trials AS t
            CREATE (vt:VarietyTrial {tid: t.id, cropEppo: t.cropEppo, cropScientific: t.cropScientific,
                    varietyNormalized: t.varietyNormalized, yieldKgHa: t.yieldKgHa,
                    yieldNoteS1: t.yieldNoteS1, rankingEligible: t.rankingEligible,
                    irrigationRegime: t.irrigationRegime, year: t.year,
                    productionSystem: t.productionSystem,
                    diseaseScoresUnified: t.diseaseScoresUnified,
                    agronomicTraitsUnified: t.agronomicTraitsUnified, confidence: t.confidence,
                    source_id: t.source_id, yieldDerivationMethod: t.yieldDerivationMethod})
            WITH vt, t
            UNWIND t.sites AS si
            MATCH (ts:TrialSite {idx: si})
            CREATE (vt)-[:TRIAL_AT]->(ts)
            """,
            trials=_trials(),
        )


_REQ = ["TRZAX", "PISSA", "SECCE", "TRZDU", "HORVX", "ZEAMA", "AVESA", "LUPAL", "ZZZZZ",
        "Triticum", "triticum aestivum", "TRZAX"]
_SITE_SETS = {
    "flat": [{"name": "site-a", "distance": None}, {"name": "site-b", "distance": None},
             {"name": "site-c", "distance": None}],
    "weighted": [{"name": "site-c", "distance": 0.2}, {"name": "site-a", "distance": 1.5},
                 {"name": "site-d", "distance": 0.7}],
    "unsorted": [{"name": "site-d", "distance": None}, {"name": "site-c", "distance": None},
                 {"name": "site-a", "distance": None}, {"name": "site-b", "distance": None}],
    "empty": [],
}


async def _per_crop(dao, crops, sites, **kw):
    return {c: (await dao.extrapolate_varieties(c, similar_sites_override=sites, **kw))["ranked_varieties"]
            for c in dict.fromkeys(crops)}


@pytest.mark.parametrize("site_set", sorted(_SITE_SETS))
@pytest.mark.parametrize("irrigation", [None, "secano", "regadío"])
@pytest.mark.parametrize("top_n", [1, 5, 50])
def test_batch_equals_per_crop(dao, site_set, irrigation, top_n):
    sites = _SITE_SETS[site_set]
    kw = {"irrigation_regime": irrigation, "top_n": top_n}
    batch = _run(dao.extrapolate_varieties_batch(_REQ, sites, **kw))
    assert batch == _run(_per_crop(dao, _REQ, sites, **kw))
    assert list(batch) == list(dict.fromkeys(_REQ))


def test_fixture_exercises_ties_nulls_and_empty(dao):
    """Guard that the equivalence above covers the risky cases."""
    full = _run(dao.extrapolate_varieties_batch(_REQ, _SITE_SETS["flat"], top_n=500))
    assert full["ZZZZZ"] == [] and full["LUPAL"] == []
    assert full["AVESA"] and all(v["mean_yield_kg_ha"] is None for v in full["AVESA"])
    assert [v["mean_yield_kg_ha"] for v in full["HORVX"][:8]] == [99999.0] * 8
    secce = [v["mean_yield_kg_ha"] for v in full["SECCE"]]
    assert secce[:2] == [3001.0, 3000.0] and secce[2:] == [None] * 7


def test_batch_exclude_sites_equals_per_crop(dao):
    sites = _SITE_SETS["flat"]
    kw = {"top_n": 5, "exclude_sites": ["SITE-B"]}
    assert _run(dao.extrapolate_varieties_batch(_REQ, sites, **kw)) == _run(_per_crop(dao, _REQ, sites, **kw))


def test_batch_rejects_non_list_sites(dao):
    with pytest.raises(TypeError):
        _run(dao.extrapolate_varieties_batch(["TRZAX"], "site-a"))


# ── recommend_for_conditions: batched Köppen path == per-crop path ────────────
_VEC = {"annual_rainfall_mm": 700.0, "annual_et0_mm": 650.0, "coldest_month_min_c": -2.0,
        "annual_temp_c": 10.0}


async def _recommend(dao, conds, batched: bool):
    dao_mod._RECOMMEND_CACHE.clear()
    dao_mod._MEDIAN_CACHE.clear()
    calls = []
    real = GraphDAO.extrapolate_varieties

    async def counting(self_, crop, **kw):
        calls.append(kw.get("vector_version", "v1"))
        return await real(self_, crop, **kw)

    patches = [patch.object(GraphDAO, "extrapolate_varieties", counting)]
    if not batched:
        patches.append(patch.object(GraphDAO, "extrapolate_varieties_batch",
                                    AsyncMock(side_effect=RuntimeError("forced per-crop path"))))
    for p in patches:
        p.start()
    try:
        return await dao.recommend_for_conditions(conds), calls
    finally:
        for p in patches:
            p.stop()
        dao_mod._RECOMMEND_CACHE.clear()


@pytest.mark.parametrize("management", ["any", "organic"])
@pytest.mark.parametrize("irrigation", [None, "secano"])
@pytest.mark.parametrize("soil_type", [None, "Loam"])
@pytest.mark.parametrize("numerics", [False, True])
def test_recommend_batched_equals_per_crop(dao, monkeypatch, management, irrigation, soil_type, numerics):
    monkeypatch.setenv("AGROCLIMATIC_VECTOR", "hybrid")
    conds = {"climate_class": "Cfb", "soil_type": soil_type, "irrigation_regime": irrigation,
             "management": management, "season": "all", "top_n": 10,
             **(_VEC if numerics else {})}
    new, new_calls = _run(_recommend(dao, conds, batched=True))
    old, old_calls = _run(_recommend(dao, conds, batched=False))
    assert new == old
    assert new["recommendations"], "fixture yields no recommendation"
    assert "v1" not in new_calls  # Köppen path ran batched
    assert "v1" in old_calls
