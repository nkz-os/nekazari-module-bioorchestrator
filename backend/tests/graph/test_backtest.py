"""C.3 — Accuracy backtest (leave-one-site-out CV) over MEASURED trials.

Makes "SOTA advisor" falsifiable: hold out a site, predict its variety ranking
from the rest via `extrapolate_varieties`, compare to what was observed there.
Only trials with a real `yieldKgHa` (yieldDerivationMethod IS NULL) enter the
eval set — never note-derived / fabricated yields.
"""
from __future__ import annotations

import asyncio
import shutil

import pytest
from neo4j import AsyncGraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.graph.dao import GraphDAO

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker unavailable for testcontainers"
)

_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


_PW = "testpassword"


@pytest.fixture(scope="module")
def neo4j_container():
    with Neo4jContainer("neo4j:5.26-community", password=_PW) as n:
        yield n


@pytest.fixture(scope="module")
def dao(neo4j_container):
    driver = AsyncGraphDatabase.driver(
        neo4j_container.get_connection_url(),
        auth=(neo4j_container.username, neo4j_container.password),
    )
    yield GraphDAO(driver)
    _run(driver.close())


def _reset_and_seed(dao, cypher: str):
    async def _s():
        async with dao._driver.session() as s:
            await s.run("MATCH (n) DETACH DELETE n")
            await s.run(cypher)
    _run(_s())


# ── exclude_sites: prerequisite for holding a site out of the training pool ──

def test_extrapolate_exclude_sites_removes_that_sites_trials(dao):
    # Two Csa sites, same variety: A yields 9000, B yields 5000.
    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Csa', annualRainfallMm:500})
        CREATE (b:TrialSite {name:'SiteB', climateClass:'Csa', annualRainfallMm:500})
        CREATE (t1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2020, yieldKgHa:9000.0})
        CREATE (t2:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2020, yieldKgHa:5000.0})
        CREATE (t1)-[:TRIAL_AT]->(a)
        CREATE (t2)-[:TRIAL_AT]->(b)
        """,
    )
    both = _run(dao.extrapolate_varieties(crop="TRZAX", climate_class="Csa", top_n=5))
    v_both = next(x for x in both["ranked_varieties"] if x["variety"] == "V")
    assert v_both["mean_yield_kg_ha"] == 7000.0  # (9000+5000)/2

    excl = _run(
        dao.extrapolate_varieties(
            crop="TRZAX", climate_class="Csa", top_n=5, exclude_sites=["SiteA"]
        )
    )
    v_excl = next(x for x in excl["ranked_varieties"] if x["variety"] == "V")
    assert v_excl["mean_yield_kg_ha"] == 5000.0  # SiteA's 9000 held out


def test_exclude_sites_drops_multilinked_trials_observed_there(dao):
    """Holding out a site must exclude EVERY trial observed there, even one also
    linked to an analog site — otherwise leave-one-site-out leaks (the same yield
    ends up in both the held-out observation and the training prediction)."""
    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Csa', annualRainfallMm:500})
        CREATE (b:TrialSite {name:'SiteB', climateClass:'Csa', annualRainfallMm:500})
        CREATE (t2:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2020, yieldKgHa:5000.0})
        // t3 is observed at BOTH SiteA and SiteB (multi-linked).
        CREATE (t3:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2021, yieldKgHa:1000.0})
        CREATE (t2)-[:TRIAL_AT]->(b)
        CREATE (t3)-[:TRIAL_AT]->(a)
        CREATE (t3)-[:TRIAL_AT]->(b)
        """,
    )
    excl = _run(
        dao.extrapolate_varieties(
            crop="TRZAX", climate_class="Csa", top_n=5, exclude_sites=["SiteA"]
        )
    )
    v = next(x for x in excl["ranked_varieties"] if x["variety"] == "V")
    # t3 touches held-out SiteA → excluded entirely; only t2=5000 remains (not 3000).
    assert v["mean_yield_kg_ha"] == 5000.0


# ── Backtester: leave-one-site-out cross-validation ──────────────────────────

_TWO_SITE_SEED = """
CREATE (a:TrialSite {name:'SiteA', climateClass:'Csa', annualRainfallMm:500})
CREATE (b:TrialSite {name:'SiteB', climateClass:'Csa', annualRainfallMm:500})
// SiteA: V1=9000, V2=7000
CREATE (a1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V1', variety:'V1', year:2020, yieldKgHa:9000.0})
CREATE (a2:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V2', variety:'V2', year:2020, yieldKgHa:7000.0})
// SiteB: V1=8800, V2=7200
CREATE (b1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V1', variety:'V1', year:2020, yieldKgHa:8800.0})
CREATE (b2:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V2', variety:'V2', year:2020, yieldKgHa:7200.0})
CREATE (a1)-[:TRIAL_AT]->(a)
CREATE (a2)-[:TRIAL_AT]->(a)
CREATE (b1)-[:TRIAL_AT]->(b)
CREATE (b2)-[:TRIAL_AT]->(b)
"""


def test_backtest_leave_one_site_out_metrics(dao):
    from app.eval.backtest import Backtester

    _reset_and_seed(dao, _TWO_SITE_SEED)
    report = _run(Backtester(dao).run())

    assert report["strategy"] == "leave_one_site_out"
    ov = report["overall"]
    # Hold-out A predicts from B (V1=8800,V2=7200) vs observed A (9000,7000):
    #   |8800-9000|=200, |7200-7000|=200.  Symmetric for hold-out B → all errors 200.
    assert ov["median_abs_error_kg_ha"] == 200.0
    assert ov["error_pairs"] == 4
    # Both folds predict [V1, V2]; observed order is [V1, V2] at both sites.
    assert ov["top3_overlap"] == 1.0
    # Both (site,crop) folds produced a non-empty ranking.
    assert ov["coverage"] == 1.0
    assert ov["folds"] == 2
    # Breakdowns present and keyed by crop / climate.
    assert report["by_crop"]["TRZAX"]["error_pairs"] == 4
    assert report["by_climate"]["Csa"]["folds"] == 2


def test_backtest_coverage_miss_when_no_analog_site(dao):
    """A held-out site whose (crop,climate) cell has no other site → coverage miss."""
    from app.eval.backtest import Backtester

    _reset_and_seed(
        dao,
        _TWO_SITE_SEED
        + """
        // A lone BSk site: holding it out leaves no BSk analog to predict from.
        CREATE (c:TrialSite {name:'SiteC', climateClass:'BSk', annualRainfallMm:350})
        CREATE (c1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V1', variety:'V1', year:2020, yieldKgHa:6000.0})
        CREATE (c1)-[:TRIAL_AT]->(c)
        """,
    )
    report = _run(Backtester(dao).run())

    assert report["overall"]["folds"] == 3
    assert report["overall"]["coverage"] == round(2 / 3, 3)  # Csa×2 covered, BSk missed
    assert report["by_climate"]["BSk"]["coverage"] == 0.0
    assert report["by_climate"]["Csa"]["coverage"] == 1.0


def test_backtest_top3_overlap_partial(dao):
    """Predicted top-3 set differs from observed top-3 set → overlap < 1."""
    from app.eval.backtest import Backtester

    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Csa', annualRainfallMm:500})
        CREATE (b:TrialSite {name:'SiteB', climateClass:'Csa', annualRainfallMm:500})
        // Observed at A: top-3 = {V1,V2,V3}
        CREATE (a1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V1', variety:'V1', year:2020, yieldKgHa:9000.0})
        CREATE (a2:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V2', variety:'V2', year:2020, yieldKgHa:8000.0})
        CREATE (a3:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V3', variety:'V3', year:2020, yieldKgHa:7000.0})
        CREATE (a4:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V4', variety:'V4', year:2020, yieldKgHa:1000.0})
        // Trained-on B predicts top-3 = {V4,V3,V2} → intersection with A = {V2,V3}
        CREATE (b1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V1', variety:'V1', year:2020, yieldKgHa:1000.0})
        CREATE (b2:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V2', variety:'V2', year:2020, yieldKgHa:1100.0})
        CREATE (b3:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V3', variety:'V3', year:2020, yieldKgHa:1200.0})
        CREATE (b4:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V4', variety:'V4', year:2020, yieldKgHa:9000.0})
        CREATE (a1)-[:TRIAL_AT]->(a) CREATE (a2)-[:TRIAL_AT]->(a)
        CREATE (a3)-[:TRIAL_AT]->(a) CREATE (a4)-[:TRIAL_AT]->(a)
        CREATE (b1)-[:TRIAL_AT]->(b) CREATE (b2)-[:TRIAL_AT]->(b)
        CREATE (b3)-[:TRIAL_AT]->(b) CREATE (b4)-[:TRIAL_AT]->(b)
        """,
    )
    # Hold out A: obs top3 {V1,V2,V3} vs pred top3 {V4,V3,V2} → 2/3.
    # Hold out B: obs top3 {V4,V3,V2} vs pred top3 {V1,V2,V3} → 2/3.
    report = _run(Backtester(dao).run())
    assert report["by_climate"]["Csa"]["top3_overlap"] == round(2 / 3, 3)


def test_backtest_report_route(dao):
    """The /agriculture/backtest-report route wires DAO → Backtester → report."""
    from app.api.v1.graph import agriculture_backtest_report

    _reset_and_seed(dao, _TWO_SITE_SEED)
    report = _run(agriculture_backtest_report(dao._driver))
    assert report["strategy"] == "leave_one_site_out"
    assert report["overall"]["folds"] == 2
