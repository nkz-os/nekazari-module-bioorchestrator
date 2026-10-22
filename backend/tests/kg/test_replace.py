"""replace-sources (T12 option b): removes only the named sources from a marked, restored copy.

Container tests (testcontainers Neo4j 5.26) on a synthetic mixed graph: legacy Navarra + legacy GENVCE/CREA +
reference knowledge + F1 units. Counts of every label outside the removal set must not change; a second run
deletes nothing.
"""
from __future__ import annotations

import asyncio
import json
import shutil
from urllib.parse import urlsplit

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.kg import cli
from app.kg import replace as replace_mod
from neo4j import AsyncGraphDatabase

from .seed_restored import seed

PASSWORD = "testpassword"
needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")
_loop = asyncio.new_event_loop()


def _q(driver, cypher, **params):
    async def go():
        async with driver.session() as s:
            return [r.data() for r in await (await s.run(cypher, **params)).fetch(10_000_000)]
    return _loop.run_until_complete(go())


@pytest.fixture(scope="module")
def graph():
    if shutil.which("docker") is None:
        pytest.skip("docker unavailable")
    with Neo4jContainer("neo4j:5.26-community", password=PASSWORD) as n:
        d = AsyncGraphDatabase.driver(n.get_connection_url(), auth=("neo4j", PASSWORD))
        yield n, d
        _loop.run_until_complete(d.close())


@pytest.fixture
def restored(graph):
    n, d = graph
    _q(d, "MATCH (x) DETACH DELETE x")
    _q(d, "CREATE (:KgBuildTarget {id: 'singleton', label: 'local-r', created_by_build: true})")
    seed(lambda cypher: _q(d, cypher))
    env = {"NEO4J_PASSWORD": PASSWORD, "NKZ_KG_ALLOWED_TARGET_HOSTS": urlsplit(n.get_connection_url()).hostname}
    return n, d, env


def _labels(d) -> dict[str, int]:
    return _loop.run_until_complete(replace_mod.label_counts(d, None))


def _run(n, env, tmp_path, *extra, label="local-r", execute=False):
    args = ["replace-sources", "--target", n.get_connection_url(), "--target-label", label,
            "--out", str(tmp_path / "out"), *extra]
    return cli.main(args + (["--execute"] if execute else []), env=env)


@needs_docker
def test_dry_run_reports_counts_per_source_and_the_sites_and_writes_nothing(restored, tmp_path, capsys):
    n, d, env = restored
    before = _labels(d)
    assert _run(n, env, tmp_path, "--sources", "GENVCE,CREA") == cli.EXIT_OK
    assert _labels(d) == before
    out = json.loads(capsys.readouterr().out)
    plan = out["plan"]
    assert out["mode"] == "dry-run" and out["writes"] == 0
    g, c = plan["per_source"]["GENVCE"], plan["per_source"]["CREA"]
    # 12 + 4 + 1500 legacy; 5 F1 units with 10 observations
    assert (g["legacy_trials"], g["units"], g["observations"]) == (1516, 5, 10)
    assert g["by_crop"] == {"TRZAX": 1516, "HORVX": 5}
    assert (c["legacy_trials"], c["units"]) == (8, 0) and c["by_crop"] == {"ZEAMA": 4, "ZEAMX": 4}
    keys = [s["siteKey"] for s in plan["delete"]["sites"]]
    assert keys == ["city-a", "crea-field", "empty-genvce"]            # deletable, explicit list
    assert [s["siteKey"] for s in plan["sites_kept"]] == ["shared-1"]  # Navarra trials still hang on it
    assert plan["delete"]["article_sources"] == 2 and plan["delete"]["studies"] == 1  # dg, dk-g; st-g
    assert plan["trials_of_other_sources"] == {"NAVARRA-AGRARIA": 8, "LEGACY": 4, "OTHER": 5}
    assert list((tmp_path / "out").glob("replace-*/replace-sources.json"))


@needs_docker
def test_execute_removes_only_the_named_sources_and_a_second_run_deletes_nothing(restored, tmp_path, capsys):
    n, d, env = restored
    before = _labels(d)
    assert _run(n, env, tmp_path, "--sources", "GENVCE,CREA", execute=True) == cli.EXIT_OK
    out = json.loads(capsys.readouterr().out)
    assert out["deleted"] == {"deleted_legacy_trials": 1524, "deleted_units": 5, "deleted_observations": 10,
                              "deleted_studies": 1, "deleted_documents": 2, "deleted_sites": 3,
                              "batches": out["deleted"]["batches"]}
    assert out["deleted"]["batches"] >= 4  # 1516 legacy GENVCE alone need 4 batches of 500
    after = _labels(d)
    changed = {k for k in before.keys() | after.keys() if before.get(k, 0) != after.get(k, 0)}
    assert changed <= replace_mod.TOUCHED_LABELS
    assert after["Species"] == 2 and after["PhenologyStage"] == 1 and after["CropNutrientProfile"] == 1
    assert after["RotationConstraint"] == 1 and after["ClimateCell"] == 1 and after["Pest"] == 1
    assert after["ManagementTrial"] == 1
    # every other source is intact, and so are the sites they use
    assert _q(d, "MATCH (v:VarietyTrial) RETURN v.source_id AS s, count(v) AS c ORDER BY s") == [
        {"s": "LEGACY", "c": 4}, {"s": "NAVARRA-AGRARIA", "c": 8}, {"s": "OTHER", "c": 5}]
    assert {r["k"] for r in _q(d, "MATCH (t:TrialSite) RETURN t.siteKey AS k")} == {"shared-1", "nav-1", "empty-ifapa"}
    assert _q(d, "MATCH (:VarietyTrial {source_id: 'NAVARRA-AGRARIA'})-[:TRIAL_AT]->(:TrialSite) "
                 "RETURN count(*) AS c") == [{"c": 8}]
    assert _q(d, "MATCH (o:Observation) RETURN count(o) AS c") == [{"c": 5}]  # the OTHER source's
    assert _q(d, "MATCH (m:KgBuildTarget) RETURN count(m) AS c") == [{"c": 1}]
    # second run: nothing left to delete
    again_before = _labels(d)
    assert _run(n, env, tmp_path, "--sources", "GENVCE,CREA", execute=True) == cli.EXIT_OK
    again = json.loads(capsys.readouterr().out)
    assert {k: v for k, v in again["deleted"].items() if k != "batches"} == {
        "deleted_legacy_trials": 0, "deleted_units": 0, "deleted_observations": 0,
        "deleted_studies": 0, "deleted_documents": 0, "deleted_sites": 0}
    assert _labels(d) == again_before


@needs_docker
def test_one_source_alone_leaves_the_other_and_keeps_shared_sites(restored, tmp_path, capsys):
    n, d, env = restored
    assert _run(n, env, tmp_path, "--sources", "CREA", execute=True) == cli.EXIT_OK
    out = json.loads(capsys.readouterr().out)
    assert out["deleted"]["deleted_legacy_trials"] == 8 and out["deleted"]["deleted_sites"] == 1
    assert _q(d, "MATCH (v:VarietyTrial {source_id: 'GENVCE'}) RETURN count(v) AS c") == [{"c": 1521}]
    assert _q(d, "MATCH (t:TrialSite {siteKey: 'city-a'}) RETURN count(t) AS c") == [{"c": 1}]


@needs_docker
def test_extra_empty_sites_are_deleted_only_when_no_trial_points_at_them(restored, tmp_path, capsys):
    n, d, env = restored
    assert _run(n, env, tmp_path, "--sources", "GENVCE,CREA", "--extra-site-keys", "empty-ifapa,nav-1",
                execute=True) == cli.EXIT_OK
    capsys.readouterr()
    keys = {r["k"] for r in _q(d, "MATCH (t:TrialSite) RETURN t.siteKey AS k")}
    assert keys == {"shared-1", "nav-1"}  # empty-ifapa went; nav-1 has Navarra trials; shared-1 too


@needs_docker
def test_replace_refuses_an_unmarked_or_mislabelled_target_and_bad_arguments(restored, tmp_path, capsys):
    n, d, env = restored
    before = _labels(d)
    assert _run(n, env, tmp_path, "--sources", "GENVCE", label="local-x", execute=True) == cli.EXIT_REFUSED
    assert _run(n, env, tmp_path, "--sources", "GENVCE", label="production", execute=True) == cli.EXIT_REFUSED
    assert _run(n, env, tmp_path, "--sources", "NAVARRA-AGRARIA", execute=True) == cli.EXIT_FAILED  # not buildable
    _q(d, "MATCH (m:KgBuildTarget) DELETE m")
    assert _run(n, env, tmp_path, "--sources", "GENVCE", execute=True) == cli.EXIT_REFUSED  # no marker
    assert _q(d, "MATCH (v:VarietyTrial {source_id: 'GENVCE'}) RETURN count(v) AS c") == [{"c": 1521}]
    assert _labels(d) == {k: v for k, v in before.items() if k != "KgBuildTarget"}
    # a dry run needs no marker but still the host guard
    assert _run(n, {"NEO4J_PASSWORD": PASSWORD}, tmp_path, "--sources", "GENVCE") in (cli.EXIT_OK, cli.EXIT_REFUSED)
    capsys.readouterr()


@needs_docker
def test_a_trial_of_another_source_that_vanishes_is_reported_as_an_error(restored, monkeypatch):
    """The post-check is an alarm: simulate a deletion statement that is too wide."""
    _n, d, _env = restored
    real = replace_mod._DELETE_TRIALS
    monkeypatch.setattr(replace_mod, "_DELETE_TRIALS", real.replace("WITH n LIMIT $b", "WITH n LIMIT $b "
                        "MATCH (w:VarietyTrial {{source_id: 'LEGACY'}}) DETACH DELETE w WITH n"))
    with pytest.raises(replace_mod.ReplaceError, match="other sources changed"):
        _loop.run_until_complete(replace_mod.execute(d, ["CREA"]))


def test_extra_site_key_is_taken_whole_so_keys_with_commas_work():
    args = cli._parser().parse_args([
        "replace-sources", "--sources", "GENVCE", "--target", "bolt://127.0.0.1:7687",
        "--extra-site-key", "almeria#36.79,-2.7", "--extra-site-key", "empty-ifapa",
        "--extra-site-keys", "nav-1,empty-ifapa"])
    assert cli._extra_site_keys(args) == ["almeria#36.79,-2.7", "empty-ifapa", "nav-1"]
