"""Integration test for in-place TrialSite identity migration.

Uses an apoc-enabled Neo4j testcontainer (prod has apoc; the migration
calls apply_site_canonicalization which uses apoc.refactor.mergeNodes).
Verifies the migration's VT-count invariant and source-agnostic collapse.
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


def test_migration_preserves_vt_count_and_collapses_sites(driver):
    """Migration must not lose a single VarietyTrial and must collapse
    same-name duplicate sites into one shared siteKey node."""
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
            CREATE (v1)-[:TRIAL_AT]->(a)
            CREATE (v2)-[:TRIAL_AT]->(b)
            """
        )

    summary = run(execute=True, driver=driver)

    # Invariant: zero VarietyTrial loss
    assert summary["vt_after"] == summary["vt_before"] == 2
    # Two same-name sites collapsed into one
    assert summary["sites_before"] == 2
    assert summary["sites_after"] == 1
    assert summary["merged_groups"] == 1
    # Orphans must not increase
    assert summary["orphans_after"] <= summary["orphans_before"]

    with driver.session() as s:
        # Survivor carries siteKey and accumulated sourceIds
        rec = s.run(
            "MATCH (t:TrialSite {siteKey:'sartaguda'}) "
            "RETURN t.siteKey AS k, t.sourceIds AS src"
        ).single()
        assert rec["k"] == "sartaguda"
        assert sorted(rec["src"]) == ["GENVCE", "NAVARRA-AGRARIA"]

        # Both trials linked to the shared survivor
        linked = s.run(
            "MATCH (:VarietyTrial)-[:TRIAL_AT]->(t:TrialSite {siteKey:'sartaguda'}) "
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
            CREATE (v1)-[:TRIAL_AT]->(a)
            """
        )

    summary = run(execute=False, driver=driver)

    # Reports intents but mutates nothing
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
