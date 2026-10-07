"""Build orchestrator CLI (plan task 10).

* Safety rules (no database): what authorizes a write, what never does.
* Container tests (testcontainers Neo4j 5.26) on the fixture slice: full build, idempotent re-run (same
  ``export_hash``, 0 new nodes), refused gate / non-empty / legacy-looking target write nothing.
* The same on the real bundles, opt-in with NKZ_DATA_SOURCES_DIR.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.kg import cli
from app.kg import gate as gate_mod
from app.kg.adapters import crea, genvce
from app.kg.contracts import Expected, load_contract, run_contract
from app.kg.registries import load_registries
from neo4j import AsyncGraphDatabase

FIXTURES = Path(__file__).parent / "fixtures"
RAW_REPO = os.environ.get("NKZ_DATA_SOURCES_DIR", "")
needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")
needs_raw = pytest.mark.skipif(not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to the raw-data repository to run")
PASSWORD = "testpassword"


def _cfg(tmp_path, **kw) -> cli.BuildConfig:
    base = {"sources": ("GENVCE", "CREA"), "raw_dir": Path(RAW_REPO or tmp_path), "out_dir": tmp_path / "out",
            "climate_cache": tmp_path / "cache.json", "allow_dirty": True}
    return cli.BuildConfig(**{**base, **kw})


# ═════════════════════════════════════════════════════════════════════════════
# safety rules, no database
# ═════════════════════════════════════════════════════════════════════════════

OK = {"target": "bolt://localhost:7687", "target_label": "local-1", "execute": True}


def test_a_scratch_loopback_target_is_authorized(tmp_path):
    cli.authorize_write(_cfg(tmp_path, **OK), {})
    cli.authorize_write(_cfg(tmp_path, **{**OK, "target_label": "ci"}), {"NEO4J_URI": "bolt://localhost:7999"})


@pytest.mark.parametrize("changes,env,why", [
    ({"target": None}, {}, "needs --target"),
    ({"target_label": None}, {}, "needs --target-label"),
    ({"target_label": "production"}, {}, "not a scratch label"),
    ({"target_label": "prod"}, {}, "not a scratch label"),
    ({"target_label": "green"}, {}, "not a scratch label"),
    ({"target_label": "localised"}, {}, "not a scratch label"),
    ({"target_label": "Local-1"}, {}, "not a scratch label"),
    ({"target": "bolt://graph.example.org:7687"}, {}, "neither loopback"),
    ({"target": "bolt://graph.example.org:7687"}, {"NKZ_KG_ALLOWED_TARGET_HOSTS": "other.example.org"}, "neither loopback"),
    ({"target": "http://localhost:7474"}, {}, "not a bolt/neo4j URI"),
    ({}, {"NEO4J_URI": "bolt://localhost:7687"}, "backend is configured"),
    ({"target": "neo4j://LOCALHOST"}, {"NEO4J_URI": "bolt://localhost:7687"}, "backend is configured"),
])
def test_unsafe_targets_are_refused(tmp_path, changes, env, why):
    with pytest.raises(cli.WriteRefused, match=why):
        cli.authorize_write(_cfg(tmp_path, **{**OK, **changes}), env)


def test_an_allow_listed_remote_host_is_accepted(tmp_path):
    cli.authorize_write(_cfg(tmp_path, **{**OK, "target": "bolt://build-box.example.org:7687"}),
                        {"NKZ_KG_ALLOWED_TARGET_HOSTS": "Build-Box.example.org, other"})


def test_export_gets_the_same_host_guard_as_build(tmp_path, capsys):
    args = ["export", "--out", str(tmp_path / "a.tar")]
    for target, env in (("bolt://graph.example.org:7687", {}),                           # remote, not allow-listed
                        ("bolt://localhost:7687", {"NEO4J_URI": "bolt://localhost:7687"})):  # the backend's graph
        assert cli.main(args + ["--target", target], env={"NEO4J_PASSWORD": "x", **env}) == cli.EXIT_REFUSED
    assert "REFUSED" in capsys.readouterr().err
    assert not (tmp_path / "a.tar").exists()


def test_main_exit_codes_for_safety_refusals(tmp_path, capsys):
    base = ["build", "--raw-dir", str(tmp_path), "--out", str(tmp_path / "o"), "--allow-dirty"]
    for extra in (["--execute"], ["--execute", "--target", "bolt://localhost:7687"],
                  ["--execute", "--target", "bolt://localhost:7687", "--target-label", "production"]):
        assert cli.main(base + extra, env={"NEO4J_PASSWORD": "x"}) == cli.EXIT_REFUSED
    assert "REFUSED" in capsys.readouterr().err
    assert not (tmp_path / "o").exists()  # refused before any work


def test_unknown_source_and_dirty_worktree_fail_before_writing(tmp_path, monkeypatch):
    with pytest.raises(cli.BuildFailed, match="unknown source"):
        cli.run_build(_cfg(tmp_path, sources=("NOPE",)), env={})
    monkeypatch.setattr(cli, "repo_state", lambda path: {"sha": "x", "dirty": path == cli.BACKEND})
    with pytest.raises(cli.BuildFailed, match="dirty worktree \\(module\\)"):
        cli.run_build(_cfg(tmp_path, allow_dirty=False), env={})
    assert not (tmp_path / "out").exists()


def test_the_raw_manifest_check_detects_a_changed_file(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "a.pdf").write_bytes(b"abc")
    import hashlib
    sha = hashlib.sha256(b"abc").hexdigest()
    (tmp_path / "RAW_MANIFEST.sha256").write_text(f"# header\n{sha} 3 data/a.pdf\n{sha} 3 data/missing.pdf\n")
    assert cli._check_raw_manifest(tmp_path) == {"listed": 2, "present_locally": 1, "absent_locally": 1}
    (tmp_path / "data" / "a.pdf").write_bytes(b"changed")
    with pytest.raises(cli.BuildFailed, match="differ from RAW_MANIFEST"):
        cli._check_raw_manifest(tmp_path)


# ═════════════════════════════════════════════════════════════════════════════
# fixture slice end to end (docker)
# ═════════════════════════════════════════════════════════════════════════════

_loop = asyncio.new_event_loop()


def _q(driver, cypher):
    async def go():
        async with driver.session() as s:
            return [r.data() for r in await (await s.run(cypher)).fetch(10_000_000)]
    return _loop.run_until_complete(go())


class _Cell:
    """A CHELSA reader that only counts calls: the offline run must make none."""

    def __init__(self):
        self.calls = 0

    async def __call__(self, lat, lon):
        self.calls += 1
        return {"koppen": "Cfa", "annual_temp_c": 12.5, "annual_rainfall_mm": 800.0, "annual_et0_mm": 900.0,
                "coldest_month_min_c": -3.0, "source": "test cell"}


@pytest.fixture(scope="module")
def graph():
    if shutil.which("docker") is None:
        pytest.skip("docker unavailable")
    with Neo4jContainer("neo4j:5.26-community", password=PASSWORD) as n:
        d = AsyncGraphDatabase.driver(n.get_connection_url(), auth=("neo4j", PASSWORD))
        yield n, d
        _loop.run_until_complete(d.close())


@pytest.fixture
def empty(graph):
    n, d = graph
    _q(d, "MATCH (x) DETACH DELETE x")
    return n, d, {"NEO4J_PASSWORD": PASSWORD, "NKZ_KG_ALLOWED_TARGET_HOSTS": urlsplit(n.get_connection_url()).hostname}


@pytest.fixture
def fixture_mode(monkeypatch):
    """The CLI reads the fixture slice; no git, no manifest, expected counts of the slice."""
    regs = load_registries()
    contracts = {s: load_contract(cli.SOURCES_DIR / f"{s}.yaml") for s in ("GENVCE", "CREA")}
    rows = {"GENVCE": genvce.load(FIXTURES / "genvce").rows, "CREA": crea.load(FIXTURES / "crea").rows}
    from unittest import mock
    sliced = {}
    for s, c in contracts.items():
        with mock.patch("app.kg.contracts._check_expected"):
            b = run_contract(c, regs, rows[s])
        sliced[s] = c.model_copy(update={"expected": Expected(units=len(b.units), observations=len(b.observations),
                                                              sites=len(b.sites))})
    monkeypatch.setattr(cli, "load_contract", lambda path: sliced[Path(path).stem])
    monkeypatch.setattr(cli, "repo_state", lambda path: {"sha": "fixture", "dirty": False})
    monkeypatch.setattr(cli, "_check_raw_manifest", lambda path: {"listed": 0, "present_locally": 0, "absent_locally": 0})
    monkeypatch.setattr(cli, "SOURCES", {
        "GENVCE": ("genvce", lambda _p: genvce.load(FIXTURES / "genvce")),
        "CREA": ("crea", lambda _p: crea.load(FIXTURES / "crea"))})


def _run(cfg, env, **kw):
    return cli.run_build(cfg, env=env, **kw)


def _nodes(d) -> int:
    return _q(d, "MATCH (n) RETURN count(n) AS c")[0]["c"]


@needs_docker
def test_dry_run_with_a_target_writes_nothing_and_reports(fixture_mode, empty, tmp_path):
    n, d, env = empty
    result = _run(_cfg(tmp_path, target=n.get_connection_url()), env)
    assert result.status == "dry-run" and result.summary["writes"] == 0
    assert _nodes(d) == 0
    assert (result.out / "03-adapt-gate.json").exists() and (result.out / "99-summary.json").exists()
    assert not (result.out / "04-schema.json").exists()
    assert result.summary["counts"]["GENVCE"]["units"] > 0


@needs_docker
def test_a_full_build_is_idempotent_and_calls_every_stage(fixture_mode, empty, tmp_path):
    n, d, env = empty
    cell = _Cell()
    cfg = _cfg(tmp_path, target=n.get_connection_url(), target_label="test-fixture", execute=True,
               chelsa_online=True)
    first = _run(cfg, env, climate_reader=cell)
    assert first.status == "ok", first.summary
    assert cell.calls > 0
    for stage in ("01-verify-raw", "02-registries", "03-adapt-gate", "04-schema", "05-load", "06-link",
                  "07-enrich", "08-verify", "09-export", "99-summary"):
        assert (first.out / f"{stage}.json").exists(), stage
    nodes_first = _nodes(d)
    load_manifest = json.loads((first.out / "05-load.json").read_text())
    assert "stamped_phenology_stages" in load_manifest  # stamp_phenology_species_name ran
    assert json.loads((first.out / "01-verify-raw.json").read_text())["target_label"] == "test-fixture"
    enrich = json.loads((first.out / "07-enrich.json").read_text())
    assert enrich["written"] > 0 and enrich["mode"] == "online"

    # second run: non-empty target needs --allow-existing; offline from the cache; nothing is created
    with pytest.raises(cli.WriteRefused, match="not empty"):
        _run(cfg, env, climate_reader=cell)
    again = _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="test-fixture", execute=True,
                      allow_existing=True), env, climate_reader=_Cell())
    assert again.status == "ok", again.summary
    assert _nodes(d) == nodes_first
    assert all(v == 0 for v in again.summary["nodes_created"].values())
    assert json.loads((again.out / "07-enrich.json").read_text())["mode"] == "offline-cache"
    assert again.summary["export_hash"] == first.summary["export_hash"]


@needs_docker
def test_a_rerun_on_a_graph_with_another_schema_is_refused(fixture_mode, empty, tmp_path):
    n, d, env = empty
    cfg = _cfg(tmp_path, target=n.get_connection_url(), target_label="local", execute=True)
    assert _run(cfg, env).status == "ok"
    _q(d, "MATCH (v:SchemaVersion) WITH v LIMIT 1 SET v.sha256 = 'changed'")
    with pytest.raises(cli.WriteRefused, match="different schema"):
        _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="local", execute=True,
                  allow_existing=True), env)


@needs_docker
def test_the_stamp_runs_after_the_load(fixture_mode, empty, tmp_path, monkeypatch):
    n, _d, env = empty
    order: list[str] = []
    real_load, real_stamp = cli.loader.load, cli.loader.stamp_phenology_species_name

    async def load(*a, **k):
        order.append("load")
        return await real_load(*a, **k)

    async def stamp(*a, **k):
        order.append("stamp")
        return await real_stamp(*a, **k)

    monkeypatch.setattr(cli.loader, "load", load)
    monkeypatch.setattr(cli.loader, "stamp_phenology_species_name", stamp)
    assert _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="local", execute=True),
                env).status == "ok"
    assert order == ["load", "load", "stamp"]


@needs_docker
def test_a_refused_gate_never_writes_not_even_the_schema(fixture_mode, empty, tmp_path, monkeypatch):
    n, d, env = empty
    real = gate_mod.run_gate

    def failing(*a, **k):
        return real(*a, **k).model_copy(update={"status": "fail"})

    monkeypatch.setattr(cli.gate_mod, "run_gate", failing)
    result = _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="local", execute=True), env)
    assert result.status == "refused-gate" and result.exit_code == cli.EXIT_FAILED
    assert _nodes(d) == 0  # no SchemaVersion either


@needs_docker
def test_a_legacy_looking_target_is_refused_even_with_allow_existing(fixture_mode, empty, tmp_path):
    n, d, env = empty
    _q(d, "CREATE (:VarietyTrial {mergeKey: 'legacy'})")
    for allow in (False, True):
        with pytest.raises(cli.WriteRefused, match="legacy VarietyTrial"):
            _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="local", execute=True,
                      allow_existing=allow), env)
    assert _nodes(d) == 1  # untouched: not even the schema was created


def _marked_build(n, d, env, tmp_path, label="local-m"):
    cfg = _cfg(tmp_path, target=n.get_connection_url(), target_label=label, execute=True)
    assert _run(cfg, env).status == "ok"
    return cfg


def _rerun(n, env, tmp_path, label="local-m", **kw):
    return _run(_cfg(tmp_path, target=n.get_connection_url(), target_label=label, execute=True,
                     allow_existing=True, **kw), env)


@needs_docker
def test_a_build_writes_the_environment_marker_and_the_export_leaves_it_out(fixture_mode, empty, tmp_path):
    n, d, env = empty
    first = _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="local-m", execute=True), env)
    assert first.status == "ok"
    assert _q(d, "MATCH (m:KgBuildTarget) RETURN m.label AS label, m.created_by_build AS by") == [
        {"label": "local-m", "by": True}]
    export = json.loads((first.out / "09-export.json").read_text())
    assert "KgBuildTarget" not in export["nodes_by_label"]
    assert export["nodes"] == _nodes(d) - 1  # everything but the marker


@needs_docker
def test_a_non_empty_graph_without_the_marker_is_refused_even_with_allow_existing(fixture_mode, empty, tmp_path):
    n, d, env = empty
    _marked_build(n, d, env, tmp_path)
    _q(d, "MATCH (m:KgBuildTarget) DELETE m")  # what a rebuilt production graph looks like: unitKey, no marker
    before = _nodes(d)
    for allow in (False, True):
        with pytest.raises(cli.WriteRefused):
            _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="local-m", execute=True,
                      allow_existing=allow), env)
    assert _nodes(d) == before  # untouched


@needs_docker
@pytest.mark.parametrize("marker_label", ["production", "prod-green", "green", "Local-1", "localised"])
def test_a_marker_with_a_non_scratch_label_is_refused(fixture_mode, empty, tmp_path, marker_label):
    n, d, env = empty
    _marked_build(n, d, env, tmp_path)
    _q(d, f"MATCH (m:KgBuildTarget) SET m.label = '{marker_label}'")
    before = _nodes(d)
    with pytest.raises(cli.WriteRefused, match="marked"):  # differs from --target-label, whichever way
        _rerun(n, env, tmp_path)
    # even when the operator passes the very same (non-scratch) label the static label rule refuses first
    with pytest.raises(cli.WriteRefused, match="scratch"):
        _rerun(n, env, tmp_path, label=marker_label)
    assert _nodes(d) == before


@needs_docker
def test_a_scratch_marker_with_another_label_is_refused(fixture_mode, empty, tmp_path):
    n, d, env = empty
    _marked_build(n, d, env, tmp_path, label="local-a")
    before = _nodes(d)
    with pytest.raises(cli.WriteRefused, match="marked 'local-a'"):
        _rerun(n, env, tmp_path, label="local-b")
    assert _nodes(d) == before


@needs_docker
def test_a_marker_not_created_by_the_build_is_refused(fixture_mode, empty, tmp_path):
    n, d, env = empty
    _marked_build(n, d, env, tmp_path)
    _q(d, "MATCH (m:KgBuildTarget) SET m.created_by_build = false")
    with pytest.raises(cli.WriteRefused, match="not a scratch build marker"):
        _rerun(n, env, tmp_path)
    _q(d, "MATCH (m:KgBuildTarget) REMOVE m.created_by_build")
    with pytest.raises(cli.WriteRefused, match="not a scratch build marker"):
        _rerun(n, env, tmp_path)


@needs_docker
def test_an_empty_target_with_a_foreign_marker_and_duplicate_markers_are_refused(fixture_mode, empty, tmp_path):
    n, d, env = empty
    _q(d, "CREATE (:KgBuildTarget {id: 'singleton', label: 'local-a', created_by_build: true})")
    with pytest.raises(cli.WriteRefused, match="marked 'local-a'"):
        _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="local-b", execute=True), env)
    _q(d, "CREATE (:KgBuildTarget {id: 'other', label: 'local-a', created_by_build: true})")
    with pytest.raises(cli.WriteRefused, match="2 KgBuildTarget markers"):
        _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="local-a", execute=True), env)
    assert _nodes(d) == 2  # no schema, no data


@needs_docker
def test_a_hand_made_non_empty_graph_is_refused_even_with_allow_existing(fixture_mode, empty, tmp_path):
    n, d, env = empty
    _q(d, "CREATE (:Species {name: 'x'})")
    with pytest.raises(cli.WriteRefused, match="no KgBuildTarget marker"):
        _rerun(n, env, tmp_path)
    assert _nodes(d) == 1


@needs_docker
def test_the_cli_refuses_a_foreign_host_without_connecting(fixture_mode, tmp_path, capsys):
    code = cli.main(["build", "--raw-dir", str(tmp_path), "--out", str(tmp_path / "o"), "--allow-dirty",
                     "--execute", "--target", "bolt://203.0.113.9:7687", "--target-label", "local-x"],
                    env={"NEO4J_PASSWORD": "x"})
    assert code == cli.EXIT_REFUSED
    assert "loopback" in capsys.readouterr().err


# ═════════════════════════════════════════════════════════════════════════════
# the real bundles (opt-in)
# ═════════════════════════════════════════════════════════════════════════════

@needs_raw
def test_real_dry_run_gates_both_sources_and_writes_nothing(tmp_path):
    result = _run(_cfg(tmp_path), {})
    assert result.status == "dry-run", result.summary
    assert result.summary["counts"]["GENVCE"]["units"] == 5013 and result.summary["counts"]["CREA"]["units"] == 320


@needs_docker
@needs_raw
def test_real_build_then_rerun_gives_the_same_export_hash(empty, tmp_path):
    n, d, env = empty
    cfg = _cfg(tmp_path, target=n.get_connection_url(), target_label="test-real", execute=True, chelsa_online=True)
    first = _run(cfg, env, climate_reader=_Cell())
    assert first.status == "ok", first.summary
    nodes = _nodes(d)
    second = _run(_cfg(tmp_path, target=n.get_connection_url(), target_label="test-real", execute=True,
                       allow_existing=True), env, climate_reader=_Cell())
    assert second.status == "ok", second.summary
    assert _nodes(d) == nodes
    assert all(v == 0 for v in second.summary["nodes_created"].values())
    assert second.summary["export_hash"] == first.summary["export_hash"]
    verify = json.loads((first.out / "08-verify.json").read_text())
    assert verify["ok"] and verify["checks"]["duplicates"]["detail"]["content_duplicate_groups"] == {"GENVCE": 0, "CREA": 0}


# ═════════════════════════════════════════════════════════════════════════════
# T12 option b: a marked target may hold legacy trials (mark-target -> restore -> build)
# ═════════════════════════════════════════════════════════════════════════════

def _drop_schema(d) -> None:
    for row in _q(d, "SHOW CONSTRAINTS YIELD name RETURN name"):
        _q(d, f"DROP CONSTRAINT `{row['name']}` IF EXISTS")
    for row in _q(d, "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"):
        _q(d, f"DROP INDEX `{row['name']}` IF EXISTS")


@pytest.fixture
def blank(empty):
    """A truly blank database: no node, constraint or index (what a restore target must be)."""
    n, d, env = empty
    _drop_schema(d)
    return n, d, env


def _guard(d, tmp_path, label="local-r", allow=True):
    cfg = _cfg(tmp_path, target="bolt://localhost:7687", target_label=label, execute=True, allow_existing=allow)
    return _loop.run_until_complete(cli._guard_target_contents(d, cfg))


def _mark(n, env, label="local-r", execute=True):
    args = ["mark-target", "--target", n.get_connection_url(), "--target-label", label]
    return cli.main(args + (["--execute"] if execute else []), env=env)


@needs_docker
def test_mark_target_writes_the_marker_only_on_an_empty_target(blank, capsys):
    n, d, env = blank
    assert _mark(n, env, execute=False) == cli.EXIT_OK
    assert _nodes(d) == 0  # dry run
    assert _mark(n, env) == cli.EXIT_OK
    assert _q(d, "MATCH (m:KgBuildTarget) RETURN m.label AS label, m.created_by_build AS by") == [
        {"label": "local-r", "by": True}]
    assert _mark(n, env) == cli.EXIT_OK  # idempotent
    assert _nodes(d) == 1
    assert _mark(n, env, label="local-other") == cli.EXIT_REFUSED  # a different label never re-marks
    assert _q(d, "MATCH (m:KgBuildTarget) RETURN m.label AS label") == [{"label": "local-r"}]


@needs_docker
@pytest.mark.parametrize("seed", ["CREATE (:Species {name: 'x'})",
                                  "CREATE CONSTRAINT some_rule IF NOT EXISTS FOR (s:Species) REQUIRE s.name IS UNIQUE",
                                  "CREATE INDEX some_ix IF NOT EXISTS FOR (s:Species) ON (s.name)"])
def test_mark_target_refuses_a_target_that_is_not_empty(blank, seed):
    n, d, env = blank
    _q(d, seed)
    try:
        assert _mark(n, env) == cli.EXIT_REFUSED
        assert _q(d, "MATCH (m:KgBuildTarget) RETURN count(m) AS c") == [{"c": 0}]
    finally:
        _q(d, "DROP CONSTRAINT some_rule IF EXISTS")
        _q(d, "DROP INDEX some_ix IF EXISTS")


def test_mark_target_needs_a_scratch_label_and_an_allowed_host(capsys):
    env = {"NEO4J_PASSWORD": "x"}
    assert cli.main(["mark-target", "--target", "bolt://localhost:7687", "--target-label", "production",
                     "--execute"], env=env) == cli.EXIT_REFUSED
    assert cli.main(["mark-target", "--target", "bolt://graph.example.org:7687", "--target-label", "local-r",
                     "--execute"], env=env) == cli.EXIT_REFUSED


@needs_docker
def test_legacy_trials_are_allowed_only_on_a_target_marked_before_they_arrived(blank, tmp_path):
    n, d, env = blank
    assert _mark(n, env) == cli.EXIT_OK
    _q(d, "CREATE (:VarietyTrial {mergeKey: 'legacy'}), (:Species {name: 'x'})")  # the restored copy
    assert _guard(d, tmp_path) == {"nodes_before": 2}
    with pytest.raises(cli.WriteRefused, match="not empty"):  # still needs the explicit flag
        _guard(d, tmp_path, allow=False)
    with pytest.raises(cli.WriteRefused, match="no scratch KgBuildTarget marker for 'local-x'"):  # another label
        _guard(d, tmp_path, label="local-x")


@needs_docker
def test_legacy_trials_without_the_marker_stay_refused_even_with_the_flag(empty, tmp_path):
    _n, d, _env = empty
    _q(d, "CREATE (:VarietyTrial {mergeKey: 'legacy'})")
    with pytest.raises(cli.WriteRefused, match="legacy VarietyTrial"):
        _guard(d, tmp_path)
    _q(d, "CREATE (:KgBuildTarget {id: 'a', label: 'local-r', created_by_build: true}), "
          "(:KgBuildTarget {id: 'b', label: 'local-r', created_by_build: true})")
    with pytest.raises(cli.WriteRefused, match="2 KgBuildTarget markers"):
        _guard(d, tmp_path)


@needs_docker
@pytest.mark.parametrize("props", ["label: 'production', created_by_build: true",
                                   "label: 'local-r', created_by_build: false",
                                   "label: 'local-r'"])
def test_legacy_trials_with_a_foreign_or_hand_made_marker_are_refused(empty, tmp_path, props):
    _n, d, _env = empty
    _q(d, f"CREATE (:VarietyTrial {{mergeKey: 'legacy'}}), (:KgBuildTarget {{id: 'singleton', {props}}})")
    with pytest.raises(cli.WriteRefused):
        _guard(d, tmp_path)
