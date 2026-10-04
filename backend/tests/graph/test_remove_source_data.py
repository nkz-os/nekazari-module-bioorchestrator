"""Licence takedown script: selection, orphan handling, idempotency, dry-run safety.

Integration part uses a plain Neo4j testcontainer (no APOC needed).
"""

from __future__ import annotations

import json
import shutil

import pytest
from testcontainers.neo4j import Neo4jContainer

from neo4j import READ_ACCESS, GraphDatabase
from scripts.remove_source_data import (
    LegacyIndex,
    resolve_variants,
    run,
    site_keys,
    variety_key,
    yield_key,
)

# ── pure helpers (no docker) ──────────────────────────────────────────────────


def test_resolve_variants_uses_alias_table_and_reports_near_misses():
    rows = [
        {"sid": "CTIFL", "ds": "ctifl", "n": 5},
        {"sid": "LEGACY", "ds": "ctifl", "n": 2},
        {"sid": "LEGACY", "ds": "legacy", "n": 9},
        {"sid": "GENVCE", "ds": None, "n": 7},
        {"sid": "CTIFL-HORTI", "ds": None, "n": 1},
    ]
    out = resolve_variants(rows, "CTIFL")
    assert {(v["source_id"], v["dataSource"]) for v in out["selected"]} == {("CTIFL", "ctifl"), ("LEGACY", "ctifl")}
    assert [(v["source_id"], v["dataSource"]) for v in out["legacy"]] == [("LEGACY", "legacy")]
    assert [v["source_id"] for v in out["nearMiss"]] == ["CTIFL-HORTI"]


def test_key_normalization():
    assert variety_key("  Chloé  (Test) ") == "chloe (test)"
    assert yield_key(329000) == yield_key("329000.04") == 329000.0
    assert yield_key(None) is None
    assert site_keys(["CTIFL Balandran", "Balandran (Bellegarde)"], "CTIFL") == frozenset(
        {"ctifl balandran", "balandran"}
    )


# ── integration ───────────────────────────────────────────────────────────────

docker_required = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")


@pytest.fixture(scope="module")
def driver():
    if shutil.which("docker") is None:
        pytest.skip("docker unavailable")
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = GraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        yield d
        d.close()


@pytest.fixture
def raw_file(tmp_path):
    path = tmp_path / "raw.jsonld"
    path.write_text(json.dumps({"@graph": [
        {"@id": "raw:1", "@type": "VarietyTrial", "crop_eppo": "eppo:FRAAN", "crop_scientific": "Fragaria x ananassa",
         "variety": "Dream (Production)", "trial_location": "CTIFL Balandran", "year": 2025, "yield_kg_ha": 495000.0},
        {"@id": "raw:2", "@type": "VarietyTrial", "crop_eppo": "eppo:LYPES", "variety": "Cygnary",
         "trial_location": "Avignon", "year": 2025, "yield_kg_ha": 329000.0},
        {"@id": "raw:3", "@type": "TrialSite", "name": "Avignon"},
        {"@id": "raw:4", "@type": "VarietyTrial", "crop_scientific": "Fragaria \u00d7 ananassa",
         "variety": "Gariguette (Stress)", "trial_location": "Balandran", "year": 2024, "yield_kg_ha": 467000.0},
        {"@id": "raw:5", "@type": "VarietyTrial", "crop_scientific": "Fragaria \u00d7 ananassa",
         "variety": "Magnum", "trial_location": "Balandran", "year": 2025, "yield_kg_ha": None},
    ]}), encoding="utf-8")
    return path


def _wipe(driver):
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")


def _seed(driver):
    _wipe(driver)
    with driver.session() as s:
        s.run(
            """
            CREATE (shared:TrialSite {mergeKey:'site-shared', name:'Shared', siteKey:'shared'})
            CREATE (only:TrialSite {mergeKey:'site-only', name:'Only Ctifl', siteKey:'only ctifl', source_id:'CTIFL'})
            CREATE (balan:TrialSite {mergeKey:'site-balan', name:'Balandran (Bellegarde)', siteKey:'balandran',
                                     source_id:'CTIFL'})
            CREATE (ownedSite:TrialSite {mergeKey:'site-owned', name:'Owned Lonely', source_id:'CTIFL'})
            CREATE (otherSite:TrialSite {mergeKey:'site-other', name:'Elsewhere'})
            CREATE (ownedShared:TrialSite {mergeKey:'site-owned-shared', name:'Tag Only', source_id:'CTIFL'})
            CREATE (artOnly:ArticleSource {mergeKey:'art-only', source:'CTIFL'})
            CREATE (artShared:ArticleSource {mergeKey:'art-shared', source:'CTIFL'})
            CREATE (artOwned:ArticleSource {mergeKey:'art-owned', source:'CTIFL'})
            CREATE (artOther:ArticleSource {mergeKey:'art-other', source:'GENVCE'})

            // source trials (3 spellings of the tag)
            CREATE (c1:VarietyTrial {mergeKey:'c1', source_id:'CTIFL', dataSource:'ctifl', cropEppo:'FRAAN', cropScientific:'Fragaria \u00d7 ananassa',
                    variety:'A', year:2025, yieldKgHa:1.0, trialLocation:'Shared'})
            CREATE (c2:VarietyTrial {mergeKey:'c2', source_id:'CTIFL', dataSource:'ctifl', cropEppo:'LYPES',
                    variety:'B', year:2025, yieldKgHa:2.0, trialLocation:'Only Ctifl'})
            CREATE (c3:VarietyTrial {mergeKey:'c3', source_id:'LEGACY', dataSource:'ctifl', cropEppo:'',
                    variety:'C', year:2024, yieldKgHa:3.0, trialLocation:'Balandran'})
            CREATE (c1)-[:TRIAL_AT]->(shared)
            CREATE (c2)-[:TRIAL_AT]->(only)
            CREATE (c3)-[:TRIAL_AT]->(balan)
            CREATE (c1)-[:SOURCED_FROM]->(artShared)
            CREATE (c2)-[:SOURCED_FROM]->(artOnly)

            // legacy rows: one matches raw (crop empty, scientific only; site spelled differently), one has
            // a different yield (kept), one is an unrelated site
            CREATE (l1:VarietyTrial {mergeKey:'l1', source_id:'LEGACY', dataSource:'legacy', cropEppo:'',
                    cropScientific:'Fragaria × ananassa', variety:'dream (production)', year:2025,
                    yieldKgHa:495000.0, trialLocation:'balandran'})
            CREATE (l2:VarietyTrial {mergeKey:'l2', source_id:'LEGACY', dataSource:'legacy', cropEppo:'LYPES',
                    variety:'Cygnary', year:2025, yieldKgHa:111.0, trialLocation:'Avignon'})
            CREATE (l3:VarietyTrial {mergeKey:'l3', source_id:'LEGACY', dataSource:'legacy', cropEppo:'HORVX',
                    variety:'Cygnary', year:2025, yieldKgHa:329000.0, trialLocation:'Elsewhere'})
            // legacy rows with no crop field at all and no site link: yield carries the match;
            // a null yield is too weak and must stay
            CREATE (l4:VarietyTrial {mergeKey:'l4', source_id:'LEGACY', dataSource:'legacy', cropEppo:'',
                    variety:'gariguette (stress)', year:2024, yieldKgHa:467000.0, trialLocation:'balandran'})
            CREATE (l5:VarietyTrial {mergeKey:'l5', source_id:'LEGACY', dataSource:'legacy', cropEppo:'',
                    variety:'magnum', year:2025, trialLocation:'balandran'})
            CREATE (l1)-[:TRIAL_AT]->(balan)
            CREATE (l2)-[:TRIAL_AT]->(otherSite)
            CREATE (l3)-[:TRIAL_AT]->(otherSite)

            // another source sharing a site and an article with a source trial
            CREATE (g1:VarietyTrial {mergeKey:'g1', source_id:'GENVCE', dataSource:'genvce', cropEppo:'FRAAN', cropScientific:'Fragaria \u00d7 ananassa',
                    variety:'A', year:2025, yieldKgHa:1.0, trialLocation:'Shared'})
            CREATE (g1)-[:TRIAL_AT]->(shared)
            CREATE (g1)-[:SOURCED_FROM]->(artShared)
            CREATE (g2:VarietyTrial {mergeKey:'g2', source_id:'GENVCE', dataSource:'genvce', cropEppo:'FRAAN', cropScientific:'Fragaria \u00d7 ananassa',
                    variety:'Z', year:2025, yieldKgHa:9.0, trialLocation:'Elsewhere'})
            CREATE (g2)-[:TRIAL_AT]->(otherSite)
            CREATE (g2)-[:TRIAL_AT]->(ownedShared)
            CREATE (g2)-[:SOURCED_FROM]->(artOther)
            """
        )


def _snapshot(driver):
    with driver.session() as s:
        nodes = s.run(
            "MATCH (n) RETURN labels(n) AS l, properties(n) AS p ORDER BY coalesce(n.mergeKey,'')"
        ).data()
        rels = s.run(
            "MATCH (a)-[r]->(b) RETURN coalesce(a.mergeKey,'') AS a, type(r) AS t, coalesce(b.mergeKey,'') AS b "
            "ORDER BY a, t, b"
        ).data()
    return json.dumps([nodes, rels], sort_keys=True, default=str)


def _keys(driver, label):
    with driver.session() as s:
        return {r["k"] for r in s.run(f"MATCH (n:{label}) RETURN n.mergeKey AS k")}


class _ReadOnlyDriver:
    """Fails the test if the dry-run ever opens a session that is not READ_ACCESS."""

    def __init__(self, inner):
        self._inner = inner

    def session(self, **kwargs):
        assert kwargs.get("default_access_mode") == READ_ACCESS, "dry-run opened a non-read session"
        return self._inner.session(**kwargs)


@docker_required
def test_dry_run_writes_nothing_and_reports_plan(driver, raw_file, tmp_path):
    _seed(driver)
    before = _snapshot(driver)
    report_path = tmp_path / "dry.json"

    report = run(driver=_ReadOnlyDriver(driver), source="ctifl", legacy_paths=[raw_file], report_path=report_path)

    assert _snapshot(driver) == before
    assert report["mode"] == "dry-run"
    assert report["counts"]["VarietyTrial"] == {"total": 5, "bySelection": {
        "source_id+dataSource": 2, "dataSource": 1, "legacy_match": 2}}
    assert {t["mergeKey"] for t in report["trials"]} == {"c1", "c2", "c3", "l1", "l4"}
    removed = {(e["label"], e["key"]) for e in report["dependentsRemoved"]}
    assert removed == {("TrialSite", "site-only"), ("TrialSite", "site-balan"), ("ArticleSource", "art-only")}
    kept = {e["key"]: e for e in report["dependentsKept"]}
    assert set(kept) == {"site-shared", "art-shared"}
    assert kept["site-shared"]["remainingReferencesBy"] == {"VarietyTrial:GENVCE": 1}
    assert {e["key"] for e in report["ownedUnlinked"]["nodes"]} == {"site-owned", "art-owned"}
    # tagged with the source but only another source points at it: untouched, reported
    assert [(e["key"], e["remainingReferencesBy"]) for e in report["ownedKept"]] == [
        ("site-owned-shared", {"VarietyTrial:GENVCE": 1})]
    assert report["legacyMatching"]["matched"] == 2
    assert report["legacyMatching"]["matchedCropUnknown"] == 1
    # near misses at a known site: l2 has another yield, l5 has no crop and no yield
    assert {u["mergeKey"] for u in report["legacyMatching"]["unmatched"]} == {"l2", "l5"}
    assert json.loads(report_path.read_text())["counts"] == report["counts"]


@docker_required
def test_apply_removes_source_legacy_match_and_orphans_only(driver, raw_file, tmp_path):
    _seed(driver)
    report_path = tmp_path / "apply.json"

    report = run(driver=driver, source="CTIFL", legacy_paths=[raw_file], apply=True, batch_size=2,
                 expect_trials=5, report_path=report_path)

    # trials: source (3 incl. alias-tagged LEGACY/ctifl) + the matching legacy rows; non-matching legacy kept
    assert _keys(driver, "VarietyTrial") == {"l2", "l3", "l5", "g1", "g2"}
    # shared site and shared article survive; orphaned site/article go; already-unlinked owned nodes stay (default)
    assert _keys(driver, "TrialSite") == {"site-shared", "site-owned", "site-other", "site-owned-shared"}
    assert _keys(driver, "ArticleSource") == {"art-shared", "art-owned", "art-other"}
    # shared nodes keep their remaining relationships
    with driver.session() as s:
        assert s.run("MATCH (:VarietyTrial {mergeKey:'g1'})-[r]->() RETURN count(r) AS c").single()["c"] == 2

    assert report["applied"]["batches"] == 3
    assert report["applied"]["counts"] == {"VarietyTrial": 5, "TrialSite": 2, "ArticleSource": 1}
    assert {t["mergeKey"] for t in report["applied"]["deletedTrials"]} == {"c1", "c2", "c3", "l1", "l4"}
    assert report["postCheck"] == {"remainingTrials": 0, "remainingDependents": 0}
    assert json.loads(report_path.read_text())["applied"]["counts"] == report["applied"]["counts"]


@docker_required
def test_apply_is_idempotent(driver, raw_file):
    _seed(driver)
    run(driver=driver, source="ctifl", legacy_paths=[raw_file], apply=True, batch_size=2)
    after_first = _snapshot(driver)

    second = run(driver=driver, source="ctifl", legacy_paths=[raw_file], apply=True, batch_size=2)

    assert second["applied"]["counts"] == {"VarietyTrial": 0}
    assert second["applied"]["deletedDependents"] == []
    assert _snapshot(driver) == after_first


@docker_required
def test_include_owned_unlinked_also_removes_pre_orphaned_tagged_nodes(driver, raw_file):
    _seed(driver)

    report = run(driver=driver, source="ctifl", legacy_paths=[raw_file], apply=True, include_owned_unlinked=True)

    assert _keys(driver, "TrialSite") == {"site-shared", "site-other", "site-owned-shared"}
    assert _keys(driver, "ArticleSource") == {"art-shared", "art-other"}
    assert report["applied"]["counts"] == {"VarietyTrial": 5, "TrialSite": 3, "ArticleSource": 2}


@docker_required
def test_without_legacy_files_legacy_rows_are_untouched(driver):
    _seed(driver)

    run(driver=driver, source="ctifl", apply=True)

    assert _keys(driver, "VarietyTrial") == {"l1", "l2", "l3", "l4", "l5", "g1", "g2"}
    # Balandran site is still referenced by the kept legacy row l1
    assert "site-balan" in _keys(driver, "TrialSite")


@docker_required
def test_expect_trials_mismatch_aborts_before_any_write(driver, raw_file):
    _seed(driver)
    before = _snapshot(driver)

    with pytest.raises(SystemExit):
        run(driver=driver, source="ctifl", legacy_paths=[raw_file], apply=True, expect_trials=99)

    assert _snapshot(driver) == before


@docker_required
def test_impact_reports_remaining_evidence_per_crop(driver):
    _seed(driver)

    report = run(driver=driver, source="ctifl")  # no legacy files: c1, c2, c3 only

    by_crop = {i["crop"]: i for i in report["impact"]}
    fraan = by_crop["FRAAN"]  # c1 removed; g1, g2 (GENVCE) and legacy l1 (empty EPPO, same species) remain
    assert fraan["removedTrials"] == 1
    assert fraan["remainingRankable"] == 3
    assert fraan["remainingRankableBySource"] == {"GENVCE": 2, "LEGACY": 1}
    assert fraan["belowEvidenceThresholdAfter"] is True
    lypes = by_crop["LYPES"]  # c2 removed; legacy l2 remains
    assert lypes["removedTrials"] == 1 and lypes["remainingRankable"] == 1
