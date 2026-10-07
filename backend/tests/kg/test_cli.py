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
    assert result.summary["counts"]["GENVCE"]["units"] == 3855 and result.summary["counts"]["CREA"]["units"] == 320


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
