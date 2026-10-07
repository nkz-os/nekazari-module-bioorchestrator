"""The whole T12 order in one test (option b), on throw-away containers:

    "prod" (legacy Navarra + legacy GENVCE/CREA + reference knowledge, pre-F1 schema) -> export archive
    -> empty target -> mark-target -> restore script -> migrate-restored -> replace-sources
    -> build (GENVCE, CREA) -> verify (inside the build) -> export
    -> restore the export into a SECOND container -> no KgBuildTarget marker there.

Always runs on the fixture slice; with ``NKZ_DATA_SOURCES_DIR`` it also runs on the real bundles.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.kg import cli
from app.kg import export as export_mod
from app.kg import replace as replace_mod
from app.kg.migrations import MIGRATIONS_DIR, apply_migrations
from neo4j import AsyncGraphDatabase
from tests.kg.seed_restored import seed
from tests.kg.test_cli import (  # noqa: F401
    PASSWORD,
    RAW_REPO,
    _Cell,
    _loop,
    _q,
    graph,
    needs_docker,
)
from tests.kg.test_mixed_graph import _patch_fixture_slice

RESTORE = Path(__file__).resolve().parents[2] / "scripts" / "neo4j_restore_from_export.py"
BACKEND = Path(__file__).resolve().parents[2]
REFERENCE = ("Species", "PhenologyStage", "Pest", "CropNutrientProfile", "RotationConstraint", "ClimateCell",
             "ManagementTrial", "Entitlement")


def _drop_all(d) -> None:
    _q(d, "MATCH (x) DETACH DELETE x")
    for row in _q(d, "SHOW CONSTRAINTS YIELD name RETURN name"):
        _q(d, f"DROP CONSTRAINT `{row['name']}` IF EXISTS")
    for row in _q(d, "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"):
        _q(d, f"DROP INDEX `{row['name']}` IF EXISTS")


def _restore(archive: Path, url: str, workdir: Path) -> subprocess.CompletedProcess:
    workdir.mkdir(exist_ok=True)
    return subprocess.run(
        [sys.executable, "-I", str(RESTORE), "--archive", str(archive), "--uri", url, "--workdir", str(workdir),
         "--confirm-empty-target"],
        env={"NEO4J_PASSWORD": PASSWORD, "PATH": os.environ.get("PATH", "")},
        capture_output=True, text=True, check=False, cwd=str(workdir))


def _counts(d) -> dict[str, int]:
    return _loop.run_until_complete(replace_mod.label_counts(d, None))


def _export(d, path: Path):
    return _loop.run_until_complete(export_mod.export_graph(d, path))


@pytest.fixture(scope="module")
def second_container():
    if shutil.which("docker") is None:
        pytest.skip("docker unavailable")
    with Neo4jContainer("neo4j:5.26-community", password=PASSWORD) as n:
        d = AsyncGraphDatabase.driver(n.get_connection_url(), auth=("neo4j", PASSWORD))
        yield n, d
        _loop.run_until_complete(d.close())


@needs_docker
@pytest.mark.parametrize("real", [False, pytest.param(True, marks=pytest.mark.skipif(
    not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to run on the real bundles"))])
def test_the_whole_order_ends_in_a_marker_free_graph_with_legacy_sources_intact(graph, second_container, tmp_path,  # noqa: F811
                                                                                  real):
    n, d = graph
    n2, d2 = second_container
    host = urlsplit(n.get_connection_url()).hostname
    env = {"NEO4J_PASSWORD": PASSWORD, "NKZ_KG_ALLOWED_TARGET_HOSTS": host}
    url = n.get_connection_url()
    label = "local-t12"

    # ── "production": pre-F1 schema + legacy data, exported the way the backup job does ──────────────
    _drop_all(d)
    pre = tmp_path / "pre-f1"
    pre.mkdir()
    for f in sorted(MIGRATIONS_DIR.glob("*.cypher")):
        if f.name < "010":
            shutil.copy(f, pre / f.name)
    _loop.run_until_complete(apply_migrations(d, pre))
    seed(lambda cypher: _q(d, cypher), f1=False, bulk=False)
    prod = _counts(d)
    prod_nodes = sum(prod.values())
    archive = tmp_path / "prod.tar"
    _export(d, archive)
    assert prod["VarietyTrial"] == 12 + 4 + 8 + 8 + 4  # genvce g+gs, navarra n+nn, crea c+z, legacy l
    navarra = _q(d, "MATCH (v:VarietyTrial {source_id: 'NAVARRA-AGRARIA'}) RETURN count(v) AS c")[0]["c"]

    # ── the copy: empty target -> marker -> restore -> migrate -> replace -> build ───────────────────
    _drop_all(d)
    assert cli.main(["mark-target", "--target", url, "--target-label", label, "--execute"], env=env) == cli.EXIT_OK
    done = _restore(archive, url, tmp_path / "w1")
    assert done.returncode == 0, done.stderr[-800:]  # the restore script accepts the marker-only target
    assert _q(d, "MATCH (m:KgBuildTarget) RETURN count(m) AS c") == [{"c": 1}]
    assert sum(v for k, v in _counts(d).items() if k != "KgBuildTarget") == prod_nodes

    out = ["--out", str(tmp_path / "out")]
    assert cli.main(["migrate-restored", "--target", url, "--target-label", label], env=env) == cli.EXIT_OK
    assert cli.main(["migrate-restored", "--target", url, "--target-label", label, "--execute"],
                    env=env) == cli.EXIT_OK
    assert cli.main(["replace-sources", "--sources", "GENVCE,CREA", "--target", url, *out], env=env) == cli.EXIT_OK
    before = _counts(d)
    assert cli.main(["replace-sources", "--sources", "GENVCE,CREA", "--target", url, "--target-label", label,
                     "--execute", *out], env=env) == cli.EXIT_OK
    after = _counts(d)
    assert {k for k in before if before[k] != after.get(k, 0)} <= replace_mod.TOUCHED_LABELS
    assert _q(d, "MATCH (v:VarietyTrial) WHERE v.source_id IN ['GENVCE', 'CREA'] RETURN count(v) AS c") == [{"c": 0}]
    assert all(after[k] == prod[k] for k in REFERENCE if k in prod)

    mp = pytest.MonkeyPatch()
    try:
        if not real:
            _patch_fixture_slice(mp, cli)
        cfg = cli.BuildConfig(
            sources=("GENVCE", "CREA"), raw_dir=Path(RAW_REPO or tmp_path), out_dir=tmp_path / "out", target=url,
            target_label=label, execute=True, allow_existing=True, allow_dirty=True,
            chelsa_online=real, climate_cache=tmp_path / "cache.json")
        built = cli.run_build(cfg, env=env, climate_reader=_Cell())
    finally:
        mp.undo()
    assert built.status == "ok", built.summary
    verify = json.loads((built.out / "08-verify.json").read_text())
    assert verify["ok"], verify["problems"]

    # ── the copy now: legacy sources intact, GENVCE/CREA from the build, reference knowledge untouched ──
    final = _counts(d)
    assert _q(d, "MATCH (v:VarietyTrial {source_id: 'NAVARRA-AGRARIA'}) RETURN count(v) AS c") == [{"c": navarra}]
    assert _q(d, "MATCH (v:VarietyTrial) WHERE v.unitKey IS NULL AND v.source_id IN ['GENVCE', 'CREA'] "
                 "RETURN count(v) AS c") == [{"c": 0}]
    assert _q(d, "MATCH (u:ObservationUnit) WHERE u.source_id IN ['GENVCE', 'CREA'] RETURN count(u) AS c")[0]["c"] > 0
    assert all(final[k] == prod[k] for k in REFERENCE if k in prod)
    assert final["Observation"] > 0 and final["KgBuildTarget"] == 1

    # ── export of the copy -> second container: no marker, same content ──────────────────────────────
    exported = tmp_path / "out.tar"
    result = _export(d, exported)
    assert result.nodes == _q(d, "MATCH (n) RETURN count(n) AS c")[0]["c"] - 1  # everything but the marker
    _drop_all(d2)
    again = _restore(exported, n2.get_connection_url(), tmp_path / "w2")
    assert again.returncode == 0, again.stderr[-800:]
    assert _q(d2, "MATCH (m:KgBuildTarget) RETURN count(m) AS c") == [{"c": 0}]
    assert _counts(d2) == {k: v for k, v in final.items() if k != "KgBuildTarget"}
    assert _q(d2, "MATCH (v:VarietyTrial {source_id: 'NAVARRA-AGRARIA'}) RETURN count(v) AS c") == [{"c": navarra}]
    # a graph restored from the export cannot be built into, replaced or migrated
    env2 = {"NEO4J_PASSWORD": PASSWORD, "NKZ_KG_ALLOWED_TARGET_HOSTS": urlsplit(n2.get_connection_url()).hostname}
    for command in (["replace-sources", "--sources", "GENVCE", *out], ["migrate-restored"]):
        assert cli.main([*command, "--target", n2.get_connection_url(), "--target-label", label, "--execute"],
                        env=env2) == cli.EXIT_REFUSED
