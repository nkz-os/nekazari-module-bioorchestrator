"""``get_variety_trials`` and ``get_yield_potential`` follow the evidence policy, on real Neo4j.

The expected yield a farmer sees for an assigned variety is the mean of its FIELD trials of the
main purpose: no BSL or note-derived kg/ha, no forage, no national or regional record, duplicates
once, and the variety is filtered in the query (not out of an arbitrary top-N by kg). With no such
evidence the number is null with a data gap, never 0."""
from __future__ import annotations

import asyncio
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO
from neo4j import AsyncGraphDatabase

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker unavailable for testcontainers"
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
_PW = "testpassword"


def _run(coro):
    return _loop.run_until_complete(coro)


@pytest.fixture(scope="module")
def dao():
    with Neo4jContainer("neo4j:5.26-community", password=_PW) as n:
        driver = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        d = GraphDAO(driver)
        _run(_seed(d))
        yield d
        _run(driver.close())


_SITES = [
    {"name": "A", "climateClass": "Cfb"}, {"name": "B", "climateClass": "Cfb"},
    {"name": "UK national list", "climateClass": "Cfb"},
    {"name": "BSL Deutschland Cfb", "climateClass": "Cfb"},
]


def _t(crop, variety, sites, kg=None, key=None, **props):
    base = {"cropEppo": crop, "variety": variety, "varietyNormalized": variety.upper(), "year": 2020,
            "aggregationScope": "site", "source_id": "SRC", "yieldKgHa": kg,
            "mergeKey": key or f"{crop}|{variety}|{'+'.join(sites)}|{kg}|{props.get('year')}", **props}
    return {"props": {k: v for k, v in base.items() if v is not None}, "sites": sites}


_TRIALS = [
    # LG AURUS: two measured field trials (a re-ingest twin of the first is ONE observation), plus
    # everything the policy keeps out of the mean
    _t("TRZAX", "LG AURUS", ["A"], 8000.0), _t("TRZAX", "LG AURUS", ["A"], 8000.0, key="twin"),
    _t("TRZAX", "LG AURUS", ["B"], 6000.0),
    _t("TRZAX", "LG AURUS", ["A"], 12600.0, source_id="BSL", year=2021, yieldNoteS1="7"),
    _t("TRZAX", "LG AURUS", ["B"], 9999.0, year=2022, yieldNoteS1="7",
       yieldDerivationMethod="bsl_note_empirical_factor"),
    _t("TRZAX", "LG AURUS", ["UK national list"], 5000.0, source_id="AHDB", aggregationScope="national"),
    _t("TRZAX", "LG AURUS", ["A"], 30000.0, year=2019, qualityParams='{"ndf_pct": 40.0}'),
    # only a BSL note at a field site
    _t("TRZAX", "NOTE ONLY", ["A"], 11000.0, source_id="BSL", yieldNoteS1="6"),
    # only a national record
    _t("TRZAX", "REG ONLY", ["UK national list"], 4000.0, source_id="AHDB", aggregationScope="national"),
    # only a GENVCE row (a zone average, regional evidence) at a field-named site
    _t("TRZAX", "GENVCE ONLY", ["A"], 9000.0, source_id="GENVCE"),
    # a measured variety of a crop with many higher yields of another variety (top-N bias)
    _t("BIAS", "LOW", ["A"], 1000.0),
    *[_t("BIAS", "OTHER", ["A"], 9000.0 + i, year=2000 + i % 20, key=f"o{i}") for i in range(210)],
]


async def _seed(dao):
    async with dao._driver.session() as s:
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


def test_variety_trials_listing_is_field_tier_policy_rows(dao):
    rows = _run(dao.get_variety_trials(crop="TRZAX", variety="lg aurus", limit=50))
    assert {r["evidence_tier"] for r in rows} == {"field"}
    assert "UK national list" not in {n for r in rows for n in r["site_names"]}
    by_kg = sorted(r["yield_kg_ha"] for r in rows if r["yield_kg_ha"] is not None)
    assert by_kg == [6000.0, 8000.0]  # twin collapsed; BSL, derived and forage kg are not numbers
    # the trials with a note and no policy number stay listed, with a null yield
    assert sum(r["yield_kg_ha"] is None for r in rows) == 2  # BSL note, derived note (forage is another purpose)
    assert all(r["yield_note_s1"] for r in rows if r["yield_kg_ha"] is None)
    # numbers first, best first
    assert [r["yield_kg_ha"] for r in rows][:2] == [8000.0, 6000.0]


def test_variety_trials_min_yield_reads_the_policy_yield(dao):
    rows = _run(dao.get_variety_trials(crop="TRZAX", min_yield_kg_ha=7000.0, limit=50))
    assert {r["yield_kg_ha"] for r in rows} == {8000.0}  # not the BSL 12600 / 11000 or the derived 9999


def test_variety_trials_every_tier_when_asked_and_labelled(dao):
    rows = _run(dao.get_variety_trials(crop="TRZAX", variety="REG ONLY", tier=None, limit=50))
    assert [(r["evidence_tier"], r["yield_kg_ha"]) for r in rows] == [("regional", 4000.0)]
    assert _run(dao.get_variety_trials(crop="TRZAX", variety="REG ONLY", limit=50)) == []
    with pytest.raises(ValueError):
        _run(dao.get_variety_trials(crop="TRZAX", tier="national"))


def test_variety_trials_rows_carry_their_source_id(dao):
    rows = _run(dao.get_variety_trials(crop="TRZAX", variety="lg aurus", limit=50))
    assert {r["source_id"] for r in rows} == {"SRC", "BSL"}  # measured trials + the BSL/derived note rows


def test_trial_sites_summary_lists_the_sources_of_each_site(dao):
    rows = {r["name"]: r for r in _run(dao.get_trial_sites_summary())}
    assert rows["A"]["source_ids"] == ["BSL", "GENVCE", "SRC"]
    assert rows["UK national list"]["source_ids"] == ["AHDB"]
    assert rows["BSL Deutschland Cfb"]["source_ids"] == []  # a site without trials has no source


def test_available_crops_list_the_sources_of_each_crop(dao):
    rows = {r["eppo_code"]: r for r in _run(dao.get_available_crops())}
    assert rows["TRZAX"]["source_ids"] == ["AHDB", "BSL", "GENVCE", "SRC"]
    assert rows["BIAS"]["source_ids"] == ["SRC"]


def test_extrapolate_credits_the_ranked_varieties_not_the_sites_of_other_crops(dao):
    """Analog sites are chosen by climate: a maize-only site must not bring its source into a wheat ranking."""
    async def seed():
        async with dao._driver.session() as s:
            await s.run("""
                CREATE (w:TrialSite {name: 'WHEAT SITE', climateClass: 'Zzz'})
                CREATE (m:TrialSite {name: 'MAIZE SITE', climateClass: 'Zzz'})
                CREATE (:VarietyTrial {cropEppo: 'WHEATX', variety: 'W1', varietyNormalized: 'W1', year: 2020,
                        aggregationScope: 'site', source_id: 'ITACYL', yieldKgHa: 6000.0, mergeKey: 'w1'})-[:TRIAL_AT]->(w)
                CREATE (:VarietyTrial {cropEppo: 'MAIZEX', variety: 'M1', varietyNormalized: 'M1', year: 2020,
                        aggregationScope: 'site', source_id: 'CREA', yieldKgHa: 14000.0, mergeKey: 'm1'})-[:TRIAL_AT]->(m)
            """)
    _run(seed())
    res = _run(dao.extrapolate_varieties(crop="WHEATX", climate_class="Zzz", top_n=5))
    assert {"WHEAT SITE", "MAIZE SITE"} <= set(res["similar_sites"])  # the CREA maize site IS an analog site
    assert [v["variety"] for v in res["ranked_varieties"]] == ["W1"]
    assert {sid for v in res["ranked_varieties"] for sid in v["source_ids"]} == {"ITACYL"}

