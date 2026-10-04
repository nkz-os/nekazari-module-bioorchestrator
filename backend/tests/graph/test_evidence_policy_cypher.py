"""Evidence-policy Cypher fragments on real Neo4j: they must agree with the Python
classifiers, and an aggregate built from them counts a duplicated trial once and
takes no kg/ha from BSL, forage or aggregate-site rows."""
from __future__ import annotations

import asyncio
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph import evidence_policy as ep
from neo4j import AsyncGraphDatabase

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker unavailable for testcontainers"
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
_PW = "testpassword"
_REGADIO = "http://aims.fao.org/aos/agrovoc/c_3954"


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def driver():
    with Neo4jContainer("neo4j:5.26-community", password=_PW) as n:
        drv = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        yield drv
        _run(drv.close())


async def _query(driver, cypher: str, **params) -> list[dict]:
    async with driver.session() as s:
        res = await s.run(cypher, **params)
        return [dict(r) async for r in res]


def _reset(driver, cypher: str = "", **params) -> None:
    async def _s():
        async with driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
            if cypher:
                await s.run(cypher, **params)
    _run(_s())


# Real-shaped trial property sets (source/dataSource/crop/year/metric/qualityParams/scope).
_TRIALS = [
    {"source_id": "BSL", "cropEppo": "TRZAX", "aggregationScope": "regional", "yieldKgHa": 12600.0},
    {"source_id": "BSL", "dataSource": "bsa bundessortenamt", "cropEppo": "HORVX", "aggregationScope": "national"},
    {"source_id": "BSL", "dataSource": "bsa", "cropEppo": "TTLSS", "aggregationScope": "regional"},
    {"source_id": "GENVCE", "cropEppo": "TRZAX", "qualityParams": '{"humedad_grano_pct": 16.5}',
     "aggregationScope": "site", "yieldKgHa": 6200.0},
    {"source_id": "LEGACY", "dataSource": "navarra_agraria", "cropEppo": "", "year": 2014,
     "aggregationScope": "unlocated", "qualityParams": '{"ms_pct": 17.1, "fnd_pct": 66.6}', "yieldKgHa": 9604.0},
    {"source_id": "LFL-BAYERN", "cropEppo": "AVESA", "aggregationScope": "regional"},
    {"source_id": "NAVARRA-AGRARIA", "cropEppo": "ZEAMX", "year": 2019, "aggregationScope": "site",
     "qualityParams": '{"dry_matter_pct": 34.8, "protein_pct": 9.1, "starch_pct": 31.0, "ndf_pct": 40.1}',
     "yieldKgHa": 26176.0},
    {"source_id": "NAVARRA-AGRARIA", "cropEppo": "ZEAMX", "year": 2018, "aggregationScope": "site",
     "qualityParams": '{"aporte_mazorca_pct": 64.1, "almidon_pct": 27.0}'},
    {"source_id": "NAVARRA-AGRARIA", "cropEppo": "MEDSA", "year": 2023, "aggregationScope": "site",
     "qualityParams": '{"dry_matter_pct": 22.03, "NDF_pct": 41.28, "digestibility_pct": 66.72}',
     "yieldKgHa": 15352.0},
    {"source_id": "NAVARRA-AGRARIA", "cropEppo": "OLVEU", "aggregationScope": "site",
     "yieldMetric": "fruit_weight_kg_ha", "yieldKgHa": 12150.0},
    {"source_id": "CTIFL", "dataSource": "ctifl", "cropEppo": "LYPES", "aggregationScope": "site",
     "yieldKgHa": 289000.0},
    {"source_id": "X", "cropEppo": "VITVI", "yieldMetric": "fresh_grape_kg_ha"},
    {"source_id": "X", "cropEppo": "ZEAMX", "yieldMetric": "grain_kg_ha"},
    {"source_id": "X", "cropEppo": "ZEAMX", "yieldBasis": "fresh_matter", "yieldKgHa": 60000.0,
     "qualityParams": '{"dry_matter_pct": 33.0, "ndf_pct": 40.0}'},
    {"source_id": "X", "cropEppo": "ZEAMX", "yieldMetric": "forage_fresh_matter_kg_ha", "yieldKgHa": 50000.0},
    {"source_id": "X", "cropEppo": "ZEAMX", "yieldMetric": "forage_dry_matter_kg_ha", "yieldKgHa": 18000.0,
     "qualityParams": '{"dry_matter_pct": null, "ms_pct": 21.0}'},
    {"source_id": "NAVARRA-AGRARIA", "cropEppo": "SETIT", "year": 2014.0, "yieldKgHa": 3821.0,
     "qualityParams": '{"ms_pct": 134.0, "fnd_pct": 66.6}'},
    {"source_id": "CREA", "cropEppo": " zeama ", "aggregationScope": "site"},
    {},
]


def test_purpose_basis_and_eligibility_fragments_match_python(driver):
    _reset(driver, "UNWIND $rows AS r CREATE (v:VarietyTrial) SET v = r",
           rows=[{**t, "i": i} for i, t in enumerate(_TRIALS)])
    rows = _run(_query(
        driver,
        f"""
        MATCH (vt:VarietyTrial)
        RETURN vt.i AS i,
               {ep.cypher_excluded_source('vt')} AS excluded_source,
               {ep.cypher_yield_purpose('vt')} AS purpose,
               {ep.cypher_numeric_yield_eligible('vt')} AS main,
               {ep.cypher_numeric_yield_eligible('vt', 'forage')} AS forage,
               {ep.cypher_crop_family('vt')} AS family,
               {ep.cypher_grain_yield('vt')} AS grain_yield,
               {ep.cypher_forage_basis('vt')} AS basis,
               {ep.cypher_dry_matter_pct('vt')} AS dm_pct,
               {ep.cypher_forage_dm_yield('vt')} AS dm_yield,
               {ep.cypher_field_scope('vt')} AS field_scope
        ORDER BY i
        """,
    ))
    assert len(rows) == len(_TRIALS)
    for row in rows:
        t = _TRIALS[row["i"]]
        assert row["excluded_source"] is ep.is_excluded_source(t.get("source_id"), t.get("dataSource")), t
        assert row["purpose"] == ep.yield_purpose(t.get("yieldMetric"), t.get("qualityParams")), t
        assert row["main"] is ep.is_numeric_yield_eligible(t), t
        assert row["forage"] is ep.is_numeric_yield_eligible(t, "forage"), t
        assert row["family"] == ep.crop_family(t.get("cropEppo")), t
        assert row["grain_yield"] is ep.is_grain_yield(t), t
        assert row["basis"] == ep.forage_basis(t), t
        assert row["dm_pct"] == ep.dry_matter_pct(t.get("qualityParams")), t
        expected_dm = ep.forage_dm_yield(t)
        if expected_dm is None:
            assert row["dm_yield"] is None, t
        else:
            assert row["dm_yield"] == pytest.approx(expected_dm), t
        assert row["field_scope"] is ep.is_field_scope(t.get("aggregationScope")), t
    # The fixtures exercise every purpose and basis value.
    assert {r["purpose"] for r in rows} == {"grain", "forage", "fresh", "unknown"}
    assert {r["basis"] for r in rows} == {"dry_matter", "fresh_matter", "unknown"}


_SITE_NAMES = [
    "Media 14 Località", "Országos átlag", "Átlag (9 helyszín)", "Average 10 locations",
    "Hungary (multiple locations)", "UK national list", "Poland (national average)",
    "BSL Deutschland Cfb", "BSL Deutschland Uebergang", "Bundesweit (Deutschland)", "Navarra",
    "Secanos frescos de Navarra", "Red GENVCE", "Zones Bour favorable", "Non spécifié",
    "Skåne, Östergötland, Uppland", "unknown", "  ",
    "Valladolid", "Zamadueñas (Valladolid)", "Cadreita", "Würzburg", "Mosonmagyaróvár",
    "Tekirdağ İnanlı", "İzmir", "Rípodas (Navarra)", "ITGA Navarra", "Juansenea (Doneztebe) - C1N0",
]


def test_aggregate_site_fragment_matches_python(driver):
    _reset(driver, "UNWIND $names AS n CREATE (:TrialSite {name: n})", names=_SITE_NAMES)
    _run(_query(driver, "CREATE (:TrialSite)"))  # nameless site
    rows = _run(_query(
        driver,
        f"MATCH (ts:TrialSite) RETURN ts.name AS name, {ep.cypher_aggregate_site('ts')} AS agg",
    ))
    assert len(rows) == len(_SITE_NAMES) + 1
    for row in rows:
        assert row["agg"] is ep.is_aggregate_site(row["name"]), row["name"]


def test_field_evidence_fragment_matches_python(driver):
    _reset(
        driver,
        """
        CREATE (f:TrialSite {name:'Cadreita'}), (a:TrialSite {name:'Media 8 Località'})
        CREATE (:VarietyTrial {i:0, aggregationScope:'site'})-[:TRIAL_AT]->(f)
        CREATE (:VarietyTrial {i:1, aggregationScope:'regional'})-[:TRIAL_AT]->(f)
        CREATE (:VarietyTrial {i:2, aggregationScope:'site'})-[:TRIAL_AT]->(a)
        CREATE (:VarietyTrial {i:3})-[:TRIAL_AT]->(f)
        """,
    )
    rows = _run(_query(
        driver,
        f"""
        MATCH (v:VarietyTrial)-[:TRIAL_AT]->(t:TrialSite)
        RETURN v.i AS i, v.aggregationScope AS scope, t.name AS site,
               {ep.cypher_field_evidence('v', 't')} AS field
        ORDER BY i
        """,
    ))
    assert [r["field"] for r in rows] == [True, False, False, True]
    for r in rows:
        assert r["field"] is ep.is_field_evidence(r["scope"], r["site"])


def test_content_key_fragment_groups_like_python(driver):
    _reset(
        driver,
        """
        CREATE (d:TrialSite {name:'Doneztebe'}), (s:TrialSite {name:'Santesteban'}),
               (d2:TrialSite {name:'Doneztebe'})
        // Re-ingest twins: same content, different mergeKey / normalisation fields,
        // sites linked in a different order (and one through a same-name duplicate node).
        CREATE (a:VarietyTrial {i:0, mergeKey:'k|1', trialLocationKey:'x', cropEppo:'ZEAMX',
                varietyNormalized:'CODIWAY', year:2019, yieldKgHa:12300.0, irrigationRegime:$reg})
        CREATE (b:VarietyTrial {i:1, mergeKey:'k|2', trialLocationKey:'y', cropEppo:'ZEAMX',
                varietyNormalized:'CODIWAY', year:2019, yieldKgHa:12300.0, irrigationRegime:$reg})
        CREATE (a)-[:TRIAL_AT]->(d), (a)-[:TRIAL_AT]->(s)
        CREATE (b)-[:TRIAL_AT]->(s), (b)-[:TRIAL_AT]->(d2)
        // Different year → a distinct observation.
        CREATE (c:VarietyTrial {i:2, mergeKey:'k|3', cropEppo:'ZEAMX', varietyNormalized:'CODIWAY',
                year:2020, yieldKgHa:12300.0, irrigationRegime:$reg})
        CREATE (c)-[:TRIAL_AT]->(d)
        // Null irrigation / production system / note on both twins still groups.
        CREATE (e:VarietyTrial {i:3, mergeKey:'k|4', cropEppo:'HORVX', varietyNormalized:'V', year:2018})
        CREATE (f:VarietyTrial {i:4, mergeKey:'k|5', cropEppo:'HORVX', varietyNormalized:'V', year:2018})
        CREATE (e)-[:TRIAL_AT]->(d), (f)-[:TRIAL_AT]->(d)
        """,
        reg=_REGADIO,
    )
    rows = _run(_query(
        driver,
        f"""
        MATCH (vt:VarietyTrial)
        WITH {ep.cypher_content_key('vt')} AS ck, vt
        RETURN ck, collect(vt.i) AS members ORDER BY members[0]
        """,
    ))
    groups = sorted(sorted(r["members"]) for r in rows)
    assert groups == [[0, 1], [2], [3, 4]]


def test_policy_aggregate_counts_duplicate_once_and_takes_no_excluded_kg(driver):
    """Per-variety mean built from the fragments: a re-ingested twin counts once; BSL
    (every dataSource variant), forage and aggregate-site rows contribute no kg."""
    _reset(
        driver,
        """
        CREATE (f:TrialSite {name:'Cadreita'}), (agg:TrialSite {name:'BSL Deutschland Cfb'}),
               (crea:TrialSite {name:'Media 14 Località'})
        CREATE (:VarietyTrial {mergeKey:'g|1', source_id:'GENVCE', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V', year:2020, yieldKgHa:6000.0})-[:TRIAL_AT]->(f)
        CREATE (:VarietyTrial {mergeKey:'g|2', source_id:'GENVCE', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V', year:2020, yieldKgHa:6000.0})-[:TRIAL_AT]->(f)
        CREATE (:VarietyTrial {mergeKey:'g|3', source_id:'GENVCE', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V', year:2021, yieldKgHa:8000.0})-[:TRIAL_AT]->(f)
        CREATE (:VarietyTrial {mergeKey:'b|1', source_id:'BSL', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V', year:2021, yieldKgHa:12600.0})-[:TRIAL_AT]->(f)
        CREATE (:VarietyTrial {mergeKey:'b|2', source_id:'BSL', dataSource:'bsa', aggregationScope:'regional',
                cropEppo:'TRZAX', varietyNormalized:'V', year:2022, yieldKgHa:11200.0})-[:TRIAL_AT]->(agg)
        CREATE (:VarietyTrial {mergeKey:'x|1', source_id:'X', dataSource:'bsa bundessortenamt',
                aggregationScope:'site', cropEppo:'TRZAX', varietyNormalized:'V', year:2019,
                yieldKgHa:9800.0})-[:TRIAL_AT]->(f)
        CREATE (:VarietyTrial {mergeKey:'n|1', source_id:'NAVARRA-AGRARIA', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V', year:2019, yieldKgHa:26176.0,
                qualityParams:'{"dry_matter_pct": 34.8, "ndf_pct": 40.1}'})-[:TRIAL_AT]->(f)
        CREATE (:VarietyTrial {mergeKey:'c|1', source_id:'CREA', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V', year:2019, yieldKgHa:4000.0})-[:TRIAL_AT]->(crea)
        """,
    )
    rows = _run(_query(
        driver,
        f"""
        MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite)
        WHERE vt.yieldKgHa IS NOT NULL
          AND {ep.cypher_numeric_yield_eligible('vt')}
          AND {ep.cypher_field_evidence('vt', 'ts')}
        WITH vt.varietyNormalized AS variety, {ep.cypher_content_key('vt')} AS ck,
             min(vt.yieldKgHa) AS kg
        RETURN variety, avg(kg) AS mean_kg, count(*) AS n_trials
        """,
    ))
    assert rows == [{"variety": "V", "mean_kg": 7000.0, "n_trials": 2}]
