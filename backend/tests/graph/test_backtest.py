"""C.3 — Accuracy backtest (leave-one-site-out CV) over MEASURED trials.

Makes "SOTA advisor" falsifiable: hold out a site, predict its variety ranking
from the rest via `extrapolate_varieties`, compare to what was observed there.
Only trials with a real `yieldKgHa` enter the eval set (the evidence policy drops
note-derived / fabricated yields) — never note-derived / fabricated yields.
"""
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


def test_extrapolate_excludes_ranking_ineligible(dao):
    """Catalogue/zonal trials (rankingEligible=false) must not pollute extrapolation."""
    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Csa', annualRainfallMm:500})
        CREATE (t1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'Good', variety:'Good', year:2020, yieldKgHa:5000.0, rankingEligible:true})
        CREATE (t2:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'Catalog', variety:'Catalog', year:2020, yieldKgHa:9000.0, rankingEligible:false})
        CREATE (t1)-[:TRIAL_AT]->(a)
        CREATE (t2)-[:TRIAL_AT]->(a)
        """,
    )
    res = _run(dao.extrapolate_varieties(crop="TRZAX", climate_class="Csa", top_n=5))
    varieties = [x["variety"] for x in res["ranked_varieties"]]
    assert "Catalog" not in varieties
    good = next(x for x in res["ranked_varieties"] if x["variety"] == "Good")
    assert good["mean_yield_kg_ha"] == 5000.0


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


def test_backtest_fold_carries_site_agroclimatic_features(dao):
    """Each fold must expose the held-out site's agro-climatic vector so the
    backtest can pass it as the target for C.1 distance weighting."""
    from app.eval.backtest import Backtester

    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Csa', annualRainfallMm:520, annualET0Mm:1000, frostDaysPerYear:38, elevationM:320})
        CREATE (b:TrialSite {name:'SiteB', climateClass:'Csa', annualRainfallMm:900, annualET0Mm:1000, frostDaysPerYear:6, elevationM:60})
        CREATE (a1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2020, yieldKgHa:8000.0})
        CREATE (b1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2020, yieldKgHa:8000.0})
        CREATE (a1)-[:TRIAL_AT]->(a)
        CREATE (b1)-[:TRIAL_AT]->(b)
        """,
    )
    folds = _run(Backtester(dao)._folds())
    site_a = next(f for f in folds if f["site"] == "SiteA")
    assert site_a["rainfall"] == 520 and site_a["et0"] == 1000
    assert site_a["frost"] == 38 and site_a["elevation"] == 320
    # And the full v1-vector backtest runs over enriched sites without error.
    report = _run(Backtester(dao).run(strategy="v1"))
    assert report["overall"]["coverage"] == 1.0


def test_backtest_hybrid_falls_back_to_v2_when_koppen_misses(dao):
    """Köppen has no analog (lone class per fold) but CHELSA analogs exist → v2 covers it."""
    from app.eval.backtest import Backtester

    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Csa', annualRainfallMm:500, annualET0Mm:1000,
                             coldestMonthMinCChelsa:-3.0, annualTempCChelsa:13.0})
        CREATE (b:TrialSite {name:'SiteB', climateClass:'Cfb', annualRainfallMm:520, annualET0Mm:1000,
                             coldestMonthMinCChelsa:-2.0, annualTempCChelsa:13.5})
        CREATE (a1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2020, yieldKgHa:8000.0})
        CREATE (b1:VarietyTrial {cropEppo:'TRZAX', varietyNormalized:'V', variety:'V', year:2020, yieldKgHa:7000.0})
        CREATE (a1)-[:TRIAL_AT]->(a)
        CREATE (b1)-[:TRIAL_AT]->(b)
        """,
    )
    koppen = _run(Backtester(dao).run(strategy="koppen"))
    assert koppen["overall"]["coverage"] == 0.0
    hybrid = _run(Backtester(dao).run(strategy="hybrid"))
    assert hybrid["overall"]["coverage"] == 1.0
    assert hybrid["similarity"] == "hybrid"


def test_backtest_report_route(dao):
    """The /agriculture/backtest-report route wires DAO → Backtester → report."""
    from app.api.v1.graph import agriculture_backtest_report

    _reset_and_seed(dao, _TWO_SITE_SEED)
    report = _run(agriculture_backtest_report(dao._driver))
    assert report["strategy"] == "leave_one_site_out"
    assert report["overall"]["folds"] == 2


# ── Evidence policy on the ground truth (honest baseline) ────────────────────

def _fold_obs(dao, site: str) -> dict[str, float]:
    from app.eval.backtest import Backtester

    folds = _run(Backtester(dao)._folds())
    fold = next((f for f in folds if f["site"] == site), None)
    return {} if fold is None else {o["variety"]: o["obs"] for o in fold["observed"]}


def test_backtest_folds_count_a_duplicated_trial_once(dao):
    """Re-ingest twins (same content, different mergeKey) are ONE observation."""
    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Csa', annualRainfallMm:500})
        CREATE (t1:VarietyTrial {mergeKey:'g|1', source_id:'ITACYL', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V1', variety:'V1', year:2020, yieldKgHa:9000.0})
        CREATE (t2:VarietyTrial {mergeKey:'g|2', source_id:'ITACYL', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V1', variety:'V1', year:2020, yieldKgHa:9000.0})
        CREATE (t3:VarietyTrial {mergeKey:'g|3', source_id:'ITACYL', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V1', variety:'V1', year:2021, yieldKgHa:6000.0})
        CREATE (t4:VarietyTrial {mergeKey:'g|4', source_id:'GENVCE', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'V1', variety:'V1', year:2022, yieldKgHa:1000.0})
        CREATE (t1)-[:TRIAL_AT]->(a) CREATE (t2)-[:TRIAL_AT]->(a) CREATE (t3)-[:TRIAL_AT]->(a)
        CREATE (t4)-[:TRIAL_AT]->(a)
        """,
    )
    # not (9000+9000+6000)/3; the GENVCE row (a zone average, regional evidence) is no ground truth
    assert _fold_obs(dao, "SiteA") == {"V1": 7500.0}


def test_backtest_folds_take_no_kg_from_bsl(dao):
    """BSL kg/ha (note × constant) never act as observed yield, whatever the variant."""
    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Cfb', annualRainfallMm:700})
        CREATE (m:VarietyTrial {source_id:'LFL-BAYERN', aggregationScope:'site', cropEppo:'TRZAX',
                varietyNormalized:'M', variety:'M', year:2020, yieldKgHa:8000.0})
        // Unflagged BSL kg (no yieldDerivationMethod) and dataSource-only variants.
        CREATE (b1:VarietyTrial {source_id:'BSL', aggregationScope:'site', cropEppo:'TRZAX',
                varietyNormalized:'B1', variety:'B1', year:2020, yieldKgHa:12600.0})
        CREATE (b2:VarietyTrial {source_id:'X', dataSource:'bsa', aggregationScope:'site', cropEppo:'TRZAX',
                varietyNormalized:'B2', variety:'B2', year:2020, yieldKgHa:11200.0})
        CREATE (b3:VarietyTrial {source_id:'X', dataSource:'bsa bundessortenamt', aggregationScope:'site',
                cropEppo:'TRZAX', varietyNormalized:'B3', variety:'B3', year:2020, yieldKgHa:9800.0})
        CREATE (m)-[:TRIAL_AT]->(a) CREATE (b1)-[:TRIAL_AT]->(a)
        CREATE (b2)-[:TRIAL_AT]->(a) CREATE (b3)-[:TRIAL_AT]->(a)
        """,
    )
    assert _fold_obs(dao, "SiteA") == {"M": 8000.0}


def test_backtest_folds_take_no_note_derived_kg_from_any_source(dao):
    """A kg/ha derived from a note is not observed yield, even from a source the policy keeps."""
    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Cfb', annualRainfallMm:700})
        CREATE (m:VarietyTrial {source_id:'LFL-BAYERN', aggregationScope:'site', cropEppo:'TRZAX',
                varietyNormalized:'M', variety:'M', year:2020, yieldKgHa:8000.0})
        CREATE (d:VarietyTrial {source_id:'LFL-BAYERN', aggregationScope:'site', cropEppo:'TRZAX',
                varietyNormalized:'D', variety:'D', year:2020, yieldKgHa:12600.0,
                yieldDerivationMethod:'bsl_note_empirical_factor'})
        CREATE (m)-[:TRIAL_AT]->(a) CREATE (d)-[:TRIAL_AT]->(a)
        """,
    )
    assert _fold_obs(dao, "SiteA") == {"M": 8000.0}


def test_backtest_folds_are_grain_yields_of_grain_crops(dao):
    """Forage and fresh records, and crops outside the grain family, are not ground truth."""
    from app.eval.backtest import Backtester

    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Cfb', annualRainfallMm:900})
        CREATE (g:VarietyTrial {source_id:'NAVARRA-AGRARIA', aggregationScope:'site', cropEppo:'ZEAMX',
                varietyNormalized:'GRAIN', variety:'GRAIN', year:2019, yieldKgHa:12300.0,
                qualityParams:'{"humidity_pct": 22.0, "thousand_grain_weight_g": 350.0}'})
        CREATE (s:VarietyTrial {source_id:'NAVARRA-AGRARIA', aggregationScope:'site', cropEppo:'ZEAMX',
                varietyNormalized:'SILAGE', variety:'SILAGE', year:2019, yieldKgHa:26176.0,
                qualityParams:'{"dry_matter_pct": 34.8, "ndf_pct": 40.1, "starch_pct": 31.0}'})
        CREATE (f:VarietyTrial {source_id:'NAVARRA-AGRARIA', aggregationScope:'site', cropEppo:'ZEAMX',
                varietyNormalized:'FRESH', variety:'FRESH', year:2019, yieldKgHa:40000.0,
                yieldMetric:'fresh_fruit_kg_ha'})
        CREATE (tom:VarietyTrial {source_id:'CTIFL', dataSource:'ctifl', aggregationScope:'site',
                cropEppo:'LYPES', varietyNormalized:'T', variety:'T', year:2019, yieldKgHa:289000.0})
        CREATE (g)-[:TRIAL_AT]->(a) CREATE (s)-[:TRIAL_AT]->(a) CREATE (f)-[:TRIAL_AT]->(a)
        CREATE (tom)-[:TRIAL_AT]->(a)
        """,
    )
    folds = _run(Backtester(dao)._folds())
    assert [(f["site"], f["crop"]) for f in folds] == [("SiteA", "ZEAMX")]
    assert _fold_obs(dao, "SiteA") == {"GRAIN": 12300.0}


def test_backtest_folds_skip_aggregate_sites_and_non_site_scopes(dao):
    """Pseudo-sites are never held-out folds; regional/national rows are not observations."""
    from app.eval.backtest import Backtester

    _reset_and_seed(
        dao,
        """
        CREATE (a:TrialSite {name:'SiteA', climateClass:'Cfb', annualRainfallMm:700})
        CREATE (uk:TrialSite {name:'UK national list', climateClass:'Cfb'})
        CREATE (k:VarietyTrial {source_id:'AHDB', aggregationScope:'site', cropEppo:'TRZAX',
                varietyNormalized:'K', variety:'K', year:2025, yieldKgHa:10500.0})
        CREATE (f:VarietyTrial {source_id:'LFL-BAYERN', aggregationScope:'site', cropEppo:'TRZAX',
                varietyNormalized:'F', variety:'F', year:2020, yieldKgHa:8000.0})
        CREATE (r:VarietyTrial {source_id:'LFL-BAYERN', aggregationScope:'regional', cropEppo:'TRZAX',
                varietyNormalized:'R', variety:'R', year:2020, yieldKgHa:7700.0})
        CREATE (k)-[:TRIAL_AT]->(uk) CREATE (f)-[:TRIAL_AT]->(a) CREATE (r)-[:TRIAL_AT]->(a)
        """,
    )
    folds = _run(Backtester(dao)._folds())
    assert [f["site"] for f in folds] == ["SiteA"]
    assert _fold_obs(dao, "SiteA") == {"F": 8000.0}


def test_backtest_report_names_the_evidence_policy(dao):
    from app.eval.backtest import Backtester
    from app.graph import evidence_policy

    _reset_and_seed(dao, _TWO_SITE_SEED)
    report = _run(Backtester(dao).run())
    assert report["evidence_policy"] == evidence_policy.POLICY_VERSION
