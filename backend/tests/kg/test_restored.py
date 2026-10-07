"""migrate-restored (T12 option b): F1 schema on a non-empty restored copy, after a duplicate preflight.

The 'restored' graph is a database that has only the pre-F1 migrations (001-009), legacy data and the scratch
marker; the F1 migration (010) is what ``migrate-restored`` has to add.
"""
from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.kg import cli
from app.kg import restored as restored_mod
from app.kg.migrations import MIGRATIONS_DIR, apply_migrations
from neo4j import AsyncGraphDatabase

PASSWORD = "testpassword"
needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")
_loop = asyncio.new_event_loop()


def _q(driver, cypher):
    async def go():
        async with driver.session() as s:
            return [r.data() for r in await (await s.run(cypher)).fetch(10_000_000)]
    return _loop.run_until_complete(go())


@pytest.fixture(scope="module")
def graph():
    if shutil.which("docker") is None:
        pytest.skip("docker unavailable")
    with Neo4jContainer("neo4j:5.26-community", password=PASSWORD) as n:
        d = AsyncGraphDatabase.driver(n.get_connection_url(), auth=("neo4j", PASSWORD))
        yield n, d
        _loop.run_until_complete(d.close())


@pytest.fixture(scope="module")
def pre_f1_dir(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("pre-f1")
    for f in sorted(MIGRATIONS_DIR.glob("*.cypher")):
        if f.name < "010":
            shutil.copy(f, out / f.name)
    return out


def _wipe(d) -> None:
    _q(d, "MATCH (x) DETACH DELETE x")
    for row in _q(d, "SHOW CONSTRAINTS YIELD name RETURN name"):
        _q(d, f"DROP CONSTRAINT `{row['name']}` IF EXISTS")
    for row in _q(d, "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"):
        _q(d, f"DROP INDEX `{row['name']}` IF EXISTS")


@pytest.fixture
def restored(graph, pre_f1_dir):
    n, d = graph
    _wipe(d)
    _q(d, "CREATE (:KgBuildTarget {id: 'singleton', label: 'local-r', created_by_build: true})")
    _loop.run_until_complete(apply_migrations(d, pre_f1_dir))
    _q(d, "CREATE (a:ArticleSource {sourceName: 'Navarra Agraria'}) "
          "CREATE (:VarietyTrial {mergeKey: 'v1', management: 'organic'})-[:SOURCED_FROM]->(a) "
          "CREATE (:Species {name: 'wheat', eppoCode: 'TRZAX'})")
    env = {"NEO4J_PASSWORD": PASSWORD, "NKZ_KG_ALLOWED_TARGET_HOSTS": urlsplit(n.get_connection_url()).hostname}
    return n, d, env


def _migrate(n, env, *, label="local-r", execute=False):
    args = ["migrate-restored", "--target", n.get_connection_url(), "--target-label", label]
    return cli.main(args + (["--execute"] if execute else []), env=env)


def _constraint_names(d) -> set[str]:
    return {r["name"] for r in _q(d, "SHOW CONSTRAINTS YIELD name RETURN name")}


def test_the_wanted_constraints_are_parsed_from_the_real_migrations():
    wanted = {w.name: (w.label, w.properties) for w in restored_mod.wanted_constraints()}
    assert wanted["observation_unit_unitkey"] == ("ObservationUnit", ("unitKey",))
    assert wanted["article_source_documentkey"] == ("ArticleSource", ("documentKey",))
    assert wanted["nutrient_profile_species_stage"] == ("CropNutrientProfile", ("species", "stage", "element"))
    assert wanted["stage_species_name"] == ("PhenologyStage", ("speciesName", "name"))


@needs_docker
def test_a_clean_restored_copy_gets_the_schema_and_keeps_its_data(restored, capsys):
    n, d, env = restored
    assert "observation_unit_unitkey" not in _constraint_names(d)
    data_nodes = _q(d, "MATCH (n) WHERE NOT n:SchemaVersion AND NOT n:KgBuildTarget RETURN count(n) AS c")
    assert _migrate(n, env) == cli.EXIT_OK  # dry run
    dry = json.loads(capsys.readouterr().out)
    assert dry["writes"] == 0 and dry["preflight"]["clean"] and "observation_unit_unitkey" in \
        dry["preflight"]["constraints_to_create"]
    assert "observation_unit_unitkey" not in _constraint_names(d)

    assert _migrate(n, env, execute=True) == cli.EXIT_OK
    out = json.loads(capsys.readouterr().out)
    assert out["data_statements_skipped"] > 0  # 006 and 007 data statements are not run again
    assert {"observation_unit_unitkey", "observation_obskey", "study_studykey",
            "article_source_documentkey"} <= _constraint_names(d)
    versions = {r["file"]: r["mode"] for r in _q(d, "MATCH (v:SchemaVersion) RETURN v.file AS file, v.mode AS mode")}
    assert versions["010_kg_identity.cypher"] == "full"  # schema statements only: nothing to skip
    assert versions["007_management_regime.cypher"] == "schema-only"
    # legacy data untouched: 007 would have tagged the Navarra trial 'conventional'
    assert _q(d, "MATCH (v:VarietyTrial {mergeKey: 'v1'}) RETURN v.management AS m") == [{"m": "organic"}]
    assert _q(d, "MATCH (n) WHERE NOT n:SchemaVersion AND NOT n:KgBuildTarget RETURN count(n) AS c") == data_nodes
    # idempotent
    assert _migrate(n, env, execute=True) == cli.EXIT_OK
    again = json.loads(capsys.readouterr().out)
    assert again["preflight"]["constraints_to_create"] == []
    # the build's own schema check now accepts the copy
    _loop.run_until_complete(cli._require_current_schema(d, None))


@needs_docker
def test_a_duplicate_key_refuses_before_anything_is_written(restored, capsys):
    n, d, env = restored
    _q(d, "CREATE (:ArticleSource {documentKey: 'dup-doc'}), (:ArticleSource {documentKey: 'dup-doc'}), "
          "(:ArticleSource {documentKey: 'fine'})")
    for execute in (False, True):
        assert _migrate(n, env, execute=execute) == cli.EXIT_FAILED
        out = json.loads(capsys.readouterr().out)
        (violation,) = out["preflight"]["violations"]
        assert violation["constraint"] == "article_source_documentkey" and violation["kind"] == "duplicate"
        assert violation["groups"] == 1 and violation["sample"] == [{"key": ["dup-doc"], "nodes": 2}]
    assert "observation_unit_unitkey" not in _constraint_names(d)  # nothing was applied
    assert _q(d, "MATCH (v:SchemaVersion {file: '010_kg_identity.cypher'}) RETURN count(v) AS c") == [{"c": 0}]


@needs_docker
def test_a_blocking_plain_index_is_reported_not_dropped(restored, capsys):
    n, d, env = restored
    _q(d, "CREATE INDEX crop_eppo_plain IF NOT EXISTS FOR (c:Crop) ON (c.eppo)")
    assert _migrate(n, env, execute=True) == cli.EXIT_FAILED
    (violation,) = json.loads(capsys.readouterr().out)["preflight"]["violations"]
    assert violation["kind"] == "blocking-index" and violation["index"] == "crop_eppo_plain"
    assert any(r["name"] == "crop_eppo_plain" for r in _q(d, "SHOW INDEXES YIELD name RETURN name"))


@needs_docker
def test_composite_keys_count_only_nodes_that_have_every_part(restored, capsys):
    n, d, env = restored
    _q(d, "DROP CONSTRAINT nutrient_profile_species_stage IF EXISTS")  # a copy that predates the composite key
    _q(d, "CREATE (:CropNutrientProfile {species: 's', stage: 'x'}), (:CropNutrientProfile {species: 's', stage: 'x'})")
    assert _migrate(n, env) == cli.EXIT_OK  # element missing: not a key, no violation
    capsys.readouterr()
    _q(d, "CREATE (:CropNutrientProfile {species: 's', stage: 'x', element: 'n'}), "
          "(:CropNutrientProfile {species: 's', stage: 'x', element: 'n'})")
    assert _migrate(n, env) == cli.EXIT_FAILED
    (violation,) = json.loads(capsys.readouterr().out)["preflight"]["violations"]
    assert violation["constraint"] == "nutrient_profile_species_stage"
    assert violation["sample"] == [{"key": ["s", "x", "n"], "nodes": 2}]


@needs_docker
def test_migrate_restored_refuses_an_unmarked_foreign_or_non_scratch_target(restored, capsys):
    n, d, env = restored
    assert _migrate(n, env, label="local-x", execute=True) == cli.EXIT_REFUSED
    assert _migrate(n, env, label="production", execute=True) == cli.EXIT_REFUSED
    _q(d, "MATCH (m:KgBuildTarget) DELETE m")
    assert _migrate(n, env, execute=True) == cli.EXIT_REFUSED
    assert _migrate(n, {"NEO4J_PASSWORD": PASSWORD}, execute=True) == cli.EXIT_REFUSED  # host not allow-listed
    assert "observation_unit_unitkey" not in _constraint_names(d)
    capsys.readouterr()
