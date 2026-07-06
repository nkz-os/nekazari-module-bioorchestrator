"""Integration test for in-place TrialSite identity migration.

Uses an apoc-enabled Neo4j testcontainer (prod has apoc; the migration
calls apply_site_canonicalization which uses apoc.refactor.mergeNodes).
Verifies the migration's trial-count invariant (VarietyTrial + ManagementTrial),
source-agnostic collapse, split-path handling, and dry-run safety.
"""

from __future__ import annotations

import shutil

import pytest
from neo4j import GraphDatabase
from testcontainers.neo4j import Neo4jContainer

from scripts.migrate_site_identity import run

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
        d = GraphDatabase.driver(
            n.get_connection_url(), auth=(n.username, n.password)
        )
        with d.session() as s:
            has_apoc = s.run(
                "SHOW PROCEDURES YIELD name "
                "WHERE name='apoc.refactor.mergeNodes' RETURN count(*) AS c"
            ).single()["c"]
        if not has_apoc:
            d.close()
            pytest.skip("apoc plugin not available in test container")
        yield d
        d.close()


def _wipe(driver):
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")


def test_migration_preserves_trial_counts_and_collapses_sites(driver):
    """Migration must not lose VarietyTrial or ManagementTrial nodes,
    must collapse same-name sites into one shared siteKey node, and
    must reattach all TRIAL_AT relationships."""
    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (a:TrialSite {mergeKey:'a', name:'Sartaguda',
                   source_id:'NAVARRA-AGRARIA', latitude:42.38, longitude:-2.05})
            CREATE (b:TrialSite {mergeKey:'b', name:'Sartaguda',
                   source_id:'GENVCE', latitude:42.39, longitude:-2.06})
            CREATE (v1:VarietyTrial {mergeKey:'v1', source_id:'NAVARRA-AGRARIA',
                    cropEppo:'TRZAX', variety:'V1', year:2021})
            CREATE (v2:VarietyTrial {mergeKey:'v2', source_id:'GENVCE',
                    cropEppo:'HORVX', variety:'V2', year:2021})
            CREATE (m1:ManagementTrial {mergeKey:'m1', source_id:'NAVARRA-AGRARIA',
                    experimentType:'fertilization', year:2021})
            CREATE (v1)-[:TRIAL_AT]->(a)
            CREATE (v2)-[:TRIAL_AT]->(b)
            CREATE (m1)-[:TRIAL_AT]->(a)
            """
        )

    summary = run(execute=True, driver=driver)

    # Invariant: zero trial loss (VT + MT)
    assert summary["vt_after"] == summary["vt_before"] == 3
    assert summary["sites_before"] == 2
    assert summary["sites_after"] == 1
    assert summary["merged_groups"] == 1
    assert summary["orphans_after"] <= summary["orphans_before"]

    with driver.session() as s:
        rec = s.run(
            "MATCH (t:TrialSite {siteKey:'sartaguda'}) "
            "RETURN t.siteKey AS k, t.sourceIds AS src"
        ).single()
        assert rec["k"] == "sartaguda"
        assert sorted(rec["src"]) == ["GENVCE", "NAVARRA-AGRARIA"]

        # All 3 trials (2 VT + 1 MT) linked to the shared survivor
        linked = s.run(
            "MATCH (v)-[:TRIAL_AT]->(t:TrialSite {siteKey:'sartaguda'}) "
            "WHERE v:VarietyTrial OR v:ManagementTrial "
            "RETURN count(*) AS c"
        ).single()["c"]
        assert linked == 3


def test_migration_handles_split_without_orphaning(driver):
    """When two same-name sites are > 15 km apart, the geo-guard splits
    them into disambiguated siteKeys. Trial counts must stay invariant."""
    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (a:TrialSite {mergeKey:'a', name:'Springfield',
                   source_id:'SRC-A', latitude:39.80, longitude:-89.64})
            CREATE (b:TrialSite {mergeKey:'b', name:'Springfield',
                   source_id:'SRC-B', latitude:42.10, longitude:-72.59})
            CREATE (v1:VarietyTrial {mergeKey:'v1', source_id:'SRC-A',
                    cropEppo:'TRZAX', variety:'V1', year:2021})
            CREATE (v2:VarietyTrial {mergeKey:'v2', source_id:'SRC-B',
                    cropEppo:'HORVX', variety:'V2', year:2021})
            CREATE (v1)-[:TRIAL_AT]->(a)
            CREATE (v2)-[:TRIAL_AT]->(b)
            """
        )

    summary = run(execute=True, driver=driver)

    # Invariant holds: no trials lost
    assert summary["vt_after"] == summary["vt_before"] == 2
    # Split: 2 sites stay as 2 (not merged), flagged
    assert summary["sites_after"] == 2
    assert summary["split_groups"] == 1
    assert summary["merged_groups"] == 0
    assert summary["orphans_after"] <= summary["orphans_before"]

    with driver.session() as s:
        # Both survivors have disambiguated siteKeys
        recs = list(s.run(
            "MATCH (t:TrialSite) WHERE t.siteKey STARTS WITH 'springfield#' "
            "RETURN t.siteKey AS k, t.needsHumanReview AS hr"
        ))
        assert len(recs) == 2
        for r in recs:
            assert r["k"].startswith("springfield#")
            assert r["hr"] is True

        # Each survivor has its trial still linked
        linked = s.run(
            "MATCH (v)-[:TRIAL_AT]->(t:TrialSite) "
            "WHERE t.siteKey STARTS WITH 'springfield#' "
            "RETURN count(*) AS c"
        ).single()["c"]
        assert linked == 2


def test_migration_dry_run_mutates_nothing(driver):
    """Dry-run reports what it would do but leaves the graph untouched."""
    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (a:TrialSite {mergeKey:'a', name:'Tulebras',
                   source_id:'NAVARRA-AGRARIA', latitude:42.0, longitude:-1.6})
            CREATE (b:TrialSite {mergeKey:'b', name:'Tulebras',
                   source_id:'GENVCE', latitude:42.0, longitude:-1.6})
            CREATE (v1:VarietyTrial {mergeKey:'v1', source_id:'NAVARRA-AGRARIA',
                    cropEppo:'TRZAX', variety:'V1', year:2021})
            CREATE (m1:ManagementTrial {mergeKey:'m1', source_id:'GENVCE',
                    experimentType:'irrigation', year:2021})
            CREATE (v1)-[:TRIAL_AT]->(a)
            CREATE (m1)-[:TRIAL_AT]->(b)
            """
        )

    summary = run(execute=False, driver=driver)

    assert summary["merged_groups"] == 1
    assert summary["vt_after"] == summary["vt_before"]
    assert summary["sites_after"] == summary["sites_before"] == 2

    with driver.session() as s:
        c = s.run("MATCH (t:TrialSite) RETURN count(t) AS c").single()["c"]
    assert c == 2  # still two sites


def test_migration_sets_sitekey_on_every_node(driver):
    """After migration, every TrialSite has a non-null siteKey."""
    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (:TrialSite {mergeKey:'x', name:'Lleida', source_id:'GENVCE'})
            CREATE (:TrialSite {mergeKey:'y', name:'UniquePlace', source_id:'BSL'})
            """
        )

    run(execute=True, driver=driver)

    with driver.session() as s:
        null_keys = s.run(
            "MATCH (t:TrialSite) WHERE t.siteKey IS NULL RETURN count(t) AS c"
        ).single()["c"]
    assert null_keys == 0
