"""Integration tests for TrialSite canonicalization execution (apoc mergeNodes).

Uses an apoc-enabled Neo4j testcontainer (prod has apoc; the 0.4 canonicalization
used apoc.refactor.mergeNodes). Verifies: same-name dups merge into the richest
survivor with TRIAL_AT reattached (municipality disagreement no longer blocks the
merge — identity is the name key, not municipality), re-run is a no-op.
"""

from __future__ import annotations

import shutil

import pytest
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer

from app.graph.site_canonicalization import (
    apply_site_canonicalization,
    fetch_trial_sites,
    plan_site_canonicalization,
)

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None,
    reason="docker unavailable for testcontainers",
)


@pytest.fixture(scope="module")
def driver():
    container = (
        Neo4jContainer("neo4j:5.26-community", password="testpassword")
        .with_env("NEO4J_PLUGINS", '["apoc"]')
        .with_env("NEO4J_dbms_security_procedures_unrestricted", "apoc.*")
    )
    with container as n:
        d = GraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        with d.session() as s:
            has_apoc = s.run(
                "SHOW PROCEDURES YIELD name WHERE name='apoc.refactor.mergeNodes' RETURN count(*) AS c"
            ).single()["c"]
        if not has_apoc:
            d.close()
            pytest.skip("apoc plugin not available in test container")
        yield d
        d.close()


def _wipe(driver):
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")


def _count_sites(driver, name):
    with driver.session() as s:
        return s.run(
            "MATCH (t:TrialSite) WHERE toLower(trim(t.name))=toLower(trim($n)) RETURN count(t) AS c",
            n=name,
        ).single()["c"]


def _trials_at(driver, name):
    with driver.session() as s:
        return s.run(
            "MATCH (v:VarietyTrial)-[:TRIAL_AT]->(t:TrialSite) "
            "WHERE toLower(trim(t.name))=toLower(trim($n)) RETURN count(v) AS c",
            n=name,
        ).single()["c"]


def test_merge_collapses_dups_and_reattaches_trials(driver):
    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (a:TrialSite {name:'Sartaguda', municipality:'Sartaguda',
                    climateClass:'BSk', latitude:42.3, soilTexture:'loam', annualRainfallMm:400})
            CREATE (b:TrialSite {name:'Sartaguda', municipality:'Sartaguda', source_id:'NAVARRA-AGRARIA'})
            CREATE (c:TrialSite {name:'Sartaguda', municipality:'Sartaguda', source_id:'NAVARRA-AGRARIA'})
            CREATE (v1:VarietyTrial {mergeKey:'v1'})-[:TRIAL_AT]->(a)
            CREATE (v2:VarietyTrial {mergeKey:'v2'})-[:TRIAL_AT]->(b)
            CREATE (v3:VarietyTrial {mergeKey:'v3'})-[:TRIAL_AT]->(c)
            """
        )
    plans = plan_site_canonicalization(fetch_trial_sites(driver))
    summary = apply_site_canonicalization(driver, plans, dry_run=False)

    assert _count_sites(driver, "Sartaguda") == 1
    assert _trials_at(driver, "Sartaguda") == 3
    assert summary["merged_groups"] == 1
    assert summary["removed_nodes"] == 2
    # survivor kept its rich climateClass
    with driver.session() as s:
        cc = s.run("MATCH (t:TrialSite {name:'Sartaguda'}) RETURN t.climateClass AS cc").single()["cc"]
    assert cc == "BSk"


def test_rerun_is_idempotent_noop(driver):
    # graph already canonical from previous test -> nothing to do
    plans = plan_site_canonicalization(fetch_trial_sites(driver))
    summary = apply_site_canonicalization(driver, plans, dry_run=False)
    assert summary["merged_groups"] == 0
    assert _count_sites(driver, "Sartaguda") == 1


def test_conflicting_municipalities_now_merge_not_flag(driver):
    # Regression: municipality disagreement alone (no geo) must merge, not flag.
    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (:TrialSite {name:'Córdoba (Alameda del Obispo)', municipality:'Córdoba'})
            CREATE (:TrialSite {name:'Córdoba (Alameda del Obispo)', municipality:'Alameda'})
            """
        )
    plans = plan_site_canonicalization(fetch_trial_sites(driver))
    assert plans[0]["action"] == "merge"
    summary = apply_site_canonicalization(driver, plans, dry_run=False)

    assert _count_sites(driver, "Córdoba (Alameda del Obispo)") == 1  # merged
    assert summary["merged_groups"] == 1


def test_cli_run_execute_collapses_dups(driver):
    # the CLI run() (reused by the post-ingest hook) canonicalizes end-to-end
    from scripts.canonicalize_trial_sites import run as canon_run

    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (a:TrialSite {name:'Larraga', municipality:'Larraga', climateClass:'BSk'})
            CREATE (:TrialSite {name:'Larraga', municipality:'Larraga', source_id:'NAVARRA-AGRARIA'})
            CREATE (:VarietyTrial {mergeKey:'x'})-[:TRIAL_AT]->(a)
            """
        )
    summary = canon_run(execute=True, driver=driver)
    assert summary["merged_groups"] == 1
    assert _count_sites(driver, "Larraga") == 1
    # idempotent second run
    assert canon_run(execute=True, driver=driver)["merged_groups"] == 0


def test_dry_run_mutates_nothing(driver):
    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (:TrialSite {name:'Tulebras', municipality:'Tulebras', climateClass:'BSk'})
            CREATE (:TrialSite {name:'Tulebras', municipality:'Tulebras', source_id:'NAVARRA-AGRARIA'})
            """
        )
    plans = plan_site_canonicalization(fetch_trial_sites(driver))
    summary = apply_site_canonicalization(driver, plans, dry_run=True)
    assert _count_sites(driver, "Tulebras") == 2  # untouched
    assert summary["merged_groups"] == 1  # reports what it WOULD do


def test_merge_sets_sitekey_and_sourceids(driver):
    """Merge writes siteKey from plan and accumulates sourceIds from all members."""
    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (:TrialSite {name:'Sartaguda', source_id:'NAVARRA-AGRARIA', latitude:42.38, longitude:-2.05})
            CREATE (:TrialSite {name:'Sartaguda', source_id:'GENVCE', latitude:42.39, longitude:-2.06})
            """
        )
    plans = plan_site_canonicalization(fetch_trial_sites(driver))
    apply_site_canonicalization(driver, plans, dry_run=False)
    with driver.session() as s:
        rec = s.run(
            "MATCH (t:TrialSite {name:'Sartaguda'}) RETURN t.siteKey AS k, t.sourceIds AS src"
        ).single()
    assert rec["k"] == "sartaguda"
    assert sorted(rec["src"]) == ["GENVCE", "NAVARRA-AGRARIA"]


def test_fetch_trial_sites_loads_longitude_and_source_ids(driver):
    """fetch_trial_sites returns longitude, source_id, and sourceIds for every site."""
    _wipe(driver)
    with driver.session() as s:
        s.run(
            "CREATE (:TrialSite {name:'Lleida', source_id:'GENVCE', "
            "sourceIds:['GENVCE','ITACYL'], latitude:41.6, longitude:0.62})"
        )
    sites = fetch_trial_sites(driver)
    assert len(sites) == 1
    s0 = sites[0]
    assert s0["name"] == "Lleida"
    assert s0["longitude"] == 0.62
    assert s0["source_id"] == "GENVCE"
    assert s0["sourceIds"] == ["GENVCE", "ITACYL"]
