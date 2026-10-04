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
               {ep.cypher_forage_numeric_evidence('vt')} AS forage_numeric,
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
        assert row["forage_numeric"] is ep.is_forage_numeric_evidence(t), t
        assert row["field_scope"] is ep.is_field_scope(t.get("aggregationScope")), t
    # The fixtures exercise every purpose and basis value.
    assert {r["purpose"] for r in rows} == {"grain", "forage", "fresh", "unknown"}
    assert {r["basis"] for r in rows} == {"dry_matter", "fresh_matter", "unknown"}
    # ... and the numeric-forage predicate is true and false among forage-eligible rows.
    assert {r["forage_numeric"] for r in rows if r["forage"]} == {True, False}


@pytest.mark.parametrize("mode", ["main", "forage"])
def test_row_policy_columns_match_python(driver, mode):
    """One WITH block classifies each (trial, site) row: tier, mode gate, policy yield, counts."""
    sites = ["Cadreita", "Media 8 Località"]
    _reset(
        driver,
        """
        UNWIND $sites AS n CREATE (:TrialSite {name: n})
        WITH count(*) AS made
        UNWIND $rows AS r
        MATCH (ts:TrialSite {name: $sites[r.i % 2]})
        CREATE (v:VarietyTrial) SET v = r CREATE (v)-[:TRIAL_AT]->(ts)
        """,
        rows=[{**t, "i": i} for i, t in enumerate(_TRIALS)], sites=sites,
    )
    rows = _run(_query(
        driver,
        f"""
        MATCH (vt:VarietyTrial)-[:TRIAL_AT]->(ts:TrialSite)
        {ep.cypher_row_policy(mode)}
        RETURN vt.i AS i, ep_tier, ep_in_mode, ep_other, ep_y, ep_unconv, ep_excluded,
               {ep.cypher_numeric_candidate("vt")} AS candidate
        ORDER BY i
        """,
    ))
    assert len(rows) == len(_TRIALS)
    for row in rows:
        t = _TRIALS[row["i"]]
        site = sites[row["i"] % 2]
        assert row["ep_tier"] == ep.evidence_tier(t.get("aggregationScope"), site), t
        assert row["ep_in_mode"] is ep.in_purpose_mode(
            ep.yield_purpose(t.get("yieldMetric"), t.get("qualityParams")), mode), t
        assert row["ep_other"] is ep.is_other_purpose_evidence(t, mode), t
        assert row["ep_unconv"] is ep.has_unconverted_kg(t, mode), t
        assert row["ep_excluded"] is ep.is_excluded_source(t.get("source_id"), t.get("dataSource")), t
        # the cheap WHERE predicate is a necessary condition of a policy yield (a query that
        # applies it first can never drop a number) and equals it in the main mode
        assert row["candidate"] is (t.get("yieldKgHa") is not None and not row["ep_excluded"]), t
        if row["ep_y"] is not None:
            assert row["candidate"], t
        if mode == "main":
            assert row["ep_y"] is not None or not (row["candidate"] and row["ep_in_mode"]), t
        expected = ep.policy_yield(t, mode)
        if expected is None:
            assert row["ep_y"] is None, t
        else:
            assert row["ep_y"] == pytest.approx(expected), t
    # both tiers, and yields / non-yields / unconverted kg occur among the rows
    assert {r["ep_tier"] for r in rows} == {"field", "regional"}
    assert {r["ep_y"] is None for r in rows} == {True, False}
    assert any(r["ep_unconv"] for r in rows) or mode == "main"


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


_REGULAR = {"cropEppo": "ZEAMX", "varietyNormalized": "CODIWAY", "year": 2019, "yieldKgHa": 12300.0,
            "irrigationRegime": _REGADIO}
_ALMOND = {"cropEppo": "PRNDU", "varietyNormalized": "GUARA", "year": 2015, "yieldKgHa": 1800.0,
           "rootstock": "GF-677", "scion": "Guara", "trainingSystem": "open vase",
           "plantingYear": 2008, "plantingDensityTreesHa": 400, "cropCycle": "perennial"}
# (trial properties, linked site names); the same fixtures feed the Python and Cypher keys.
_DEDUP_FIXTURES: list[tuple[dict, list[str]]] = [
    # 0, 1: re-ingest twins (other mergeKey / normalisation fields), sites linked in another
    # order, and one of them through a same-name duplicate site node.
    ({**_REGULAR, "mergeKey": "k|1", "trialLocationKey": "x"}, ["Doneztebe", "Santesteban"]),
    ({**_REGULAR, "mergeKey": "k|2", "trialLocationKey": "y"}, ["Santesteban", "Doneztebe", "Doneztebe"]),
    # 2: different year; 3: different sites
    ({**_REGULAR, "mergeKey": "k|3", "year": 2020}, ["Doneztebe", "Santesteban"]),
    ({**_REGULAR, "mergeKey": "k|4"}, ["Oskotz"]),
    # 4, 5: nothing optional set on either twin
    ({"mergeKey": "k|5", "cropEppo": "HORVX", "varietyNormalized": "V", "year": 2018}, ["Doneztebe"]),
    ({"mergeKey": "k|6", "cropEppo": "HORVX", "varietyNormalized": "V", "year": 2018}, ["Doneztebe"]),
    # 6, 7: almond twins
    ({**_ALMOND, "mergeKey": "a|1"}, ["Sesma"]),
    ({**_ALMOND, "mergeKey": "a|2"}, ["Sesma"]),
    # 8-13: almond trials differing in exactly one orchard field
    ({**_ALMOND, "mergeKey": "a|3", "rootstock": "Garnem"}, ["Sesma"]),
    ({**_ALMOND, "mergeKey": "a|4", "scion": "Lauranne"}, ["Sesma"]),
    ({**_ALMOND, "mergeKey": "a|5", "trainingSystem": "hedgerow"}, ["Sesma"]),
    ({**_ALMOND, "mergeKey": "a|6", "plantingYear": 2010}, ["Sesma"]),
    ({**_ALMOND, "mergeKey": "a|7", "plantingDensityTreesHa": 625}, ["Sesma"]),
    ({**_ALMOND, "mergeKey": "a|8", "cropCycle": "annual"}, ["Sesma"]),
    # 14: a field missing altogether is not the same as the value it differs from
    ({k: v for k, v in {**_ALMOND, "mergeKey": "a|9"}.items() if k != "rootstock"}, ["Sesma"]),
    # 15: empty string equals missing (same group as 14)
    ({**_ALMOND, "mergeKey": "a|10", "rootstock": ""}, ["Sesma"]),
]


def test_content_key_fragment_groups_like_python(driver):
    _reset(driver, "UNWIND $sites AS n CREATE (:TrialSite {name: n, dup: false})",
           sites=["Doneztebe", "Santesteban", "Oskotz", "Sesma"])
    # A same-name duplicate site node, linked in place of the first one for twin 1.
    _run(_query(driver, "CREATE (:TrialSite {name: 'Doneztebe', dup: true})"))

    async def _create():
        async with driver.session() as s:
            for i, (props, sites) in enumerate(_DEDUP_FIXTURES):
                await s.run(
                    "CREATE (v:VarietyTrial) SET v = $props, v.i = $i "
                    "WITH v UNWIND $sites AS n "
                    "MATCH (t:TrialSite {name: n}) WHERE t.dup = ($i = 1 AND n = 'Doneztebe') "
                    "CREATE (v)-[:TRIAL_AT]->(t)",
                    props=props, i=i, sites=sorted(set(sites)),
                )
    _run(_create())
    rows = _run(_query(
        driver,
        f"""
        MATCH (vt:VarietyTrial)
        WITH {ep.cypher_content_key('vt')} AS ck, vt
        RETURN ck, collect(vt.i) AS members
        """,
    ))
    cypher_groups = sorted(sorted(r["members"]) for r in rows)
    by_key: dict[tuple, list[int]] = {}
    for i, (props, sites) in enumerate(_DEDUP_FIXTURES):
        by_key.setdefault(ep.content_key(props, sites), []).append(i)
    python_groups = sorted(sorted(m) for m in by_key.values())
    assert cypher_groups == python_groups
    assert [0, 1] in cypher_groups and [4, 5] in cypher_groups and [6, 7] in cypher_groups
    assert [14, 15] in cypher_groups
    # Each single-field orchard difference (8-13) is its own observation.
    assert all([i] in cypher_groups for i in range(8, 14))
    assert [2] in cypher_groups and [3] in cypher_groups


def test_forage_numeric_evidence_counts_all_but_averages_only_convertible(driver):
    """Mixed forage set: counts use the eligibility predicate, numbers only the convertible rows."""
    rows = [
        {"i": 0, "source_id": "X", "yieldMetric": "forage_dry_matter_kg_ha", "yieldKgHa": 18000.0,
         "qualityParams": '{"ms_pct": 21.0, "fnd_pct": 50.0}'},                       # dry matter
        {"i": 1, "source_id": "X", "yieldKgHa": 9604.0,
         "qualityParams": '{"ms_pct": 17.1, "fnd_pct": 66.6}'},                       # unknown basis
        {"i": 2, "source_id": "X", "yieldBasis": "fresh_matter", "yieldKgHa": 60000.0,
         "qualityParams": '{"dry_matter_pct": 33.0, "ndf_pct": 40.0}'},               # fresh, DM %
        {"i": 3, "source_id": "X", "yieldMetric": "forage_fresh_matter_kg_ha",
         "yieldKgHa": 50000.0},                                                       # fresh, no DM %
        {"i": 4, "source_id": "BSL", "yieldMetric": "forage_dry_matter_kg_ha",
         "yieldKgHa": 99999.0},                                                       # excluded source
        {"i": 5, "source_id": "X", "yieldKgHa": 7000.0},                              # not forage
    ]
    _reset(driver, "UNWIND $rows AS r CREATE (v:VarietyTrial) SET v = r", rows=rows)
    out = _run(_query(
        driver,
        f"""
        MATCH (vt:VarietyTrial)
        WHERE vt.yieldKgHa IS NOT NULL AND {ep.cypher_numeric_yield_eligible('vt', 'forage')}
        WITH vt, {ep.cypher_forage_numeric_evidence('vt')} AS numeric, {ep.cypher_forage_dm_yield('vt')} AS dm
        RETURN count(vt) AS n_forage,
               sum(CASE WHEN numeric THEN 1 ELSE 0 END) AS n_numeric,
               avg(CASE WHEN numeric THEN dm END) AS mean_dm,
               collect(CASE WHEN numeric THEN vt.i END) AS numeric_ids
        """,
    ))
    assert len(out) == 1
    assert out[0]["n_forage"] == 4
    assert out[0]["n_numeric"] == 2
    assert out[0]["mean_dm"] == pytest.approx((18000.0 + 19800.0) / 2)
    assert sorted(out[0]["numeric_ids"]) == [0, 2]


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


def test_tier_gate_fragments_match_python(driver):
    """The Cypher gate expression equals ``passes_tier_gate`` on every combination of the policy columns."""
    import itertools

    combos = [
        {"t": t, "rt": rt, "m": m, "o": o, "y": y}
        for t, rt, m, o, y in itertools.product(
            ["field", "regional"], ["field", "regional"], [True, False], [True, False], [None, 7.5])
    ]
    for with_other in (True, False):
        for tier in ("field", "regional"):
            rows = _run(_query(
                driver,
                f"""
                UNWIND $combos AS c
                WITH c WHERE c.t = $tier
                WITH c.rt AS ep_tier, c.m AS ep_in_mode, c.o AS ep_other, c.y AS ep_y
                RETURN ep_tier, ep_in_mode, ep_other, ep_y,
                       ({ep.cypher_tier_gate_expr(tier, with_other)}) AS gate,
                       ({ep.cypher_numeric_gate_expr(tier)}) AS numeric_gate
                """,
                combos=combos, tier=tier,
            ))
            assert len(rows) == 16
            for r in rows:
                assert r["gate"] is ep.passes_tier_gate(
                    tier, r["ep_tier"], r["ep_in_mode"], r["ep_other"], r["ep_y"], with_other), r
                assert r["numeric_gate"] is (r["ep_tier"] == tier and r["ep_in_mode"] and r["ep_y"] is not None), r
