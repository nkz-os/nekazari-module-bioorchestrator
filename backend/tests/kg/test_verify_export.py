"""Verify and export (plan task 9).

Container tests (testcontainers Neo4j 5.26, skipped without Docker) build the fixture graph, check that
``verify`` passes on it and fails on each kind of damage, that the archive restores with the real restore
script into a second, empty server, and that two builds in two servers give the same ``export_hash`` (the
determinism acceptance check). Nothing here touches a production graph.
"""
from __future__ import annotations

import asyncio
import gzip
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from unittest import mock

import pytest
from testcontainers.neo4j import Neo4jContainer

from app.kg import export, loader, verify
from app.kg.adapters import crea, genvce
from app.kg.contracts import Bundle, Expected, load_contract, run_contract
from app.kg.gate import run_gate
from app.kg.migrations import apply_migrations
from app.kg.registries import load_registries
from neo4j import AsyncGraphDatabase, GraphDatabase

BACKEND = Path(__file__).resolve().parents[2]
DATA = BACKEND / "data" / "sources"
FIXTURES = Path(__file__).parent / "fixtures"
REGISTRIES = load_registries()
CONTRACTS = {"GENVCE": load_contract(DATA / "GENVCE.yaml"), "CREA": load_contract(DATA / "CREA.yaml")}
RAW_REPO = os.environ.get("NKZ_DATA_SOURCES_DIR", "")

needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")

_loop = asyncio.new_event_loop()


def _run(coro):
    return _loop.run_until_complete(coro)


def _bundle(contract, rows) -> Bundle:
    with mock.patch("app.kg.contracts._check_expected"):
        return run_contract(contract, REGISTRIES, rows)


@pytest.fixture(scope="module")
def bundles() -> list[Bundle]:
    return [_bundle(CONTRACTS["GENVCE"], genvce.load(FIXTURES / "genvce").rows),
            _bundle(CONTRACTS["CREA"], crea.load(FIXTURES / "crea").rows)]


@pytest.fixture(scope="module")
def contracts(bundles):
    """The fixture holds a slice of the raw data, so the contract's expected counts are the slice's."""
    return [CONTRACTS[b.source_id].model_copy(update={"expected": Expected(
        units=len(b.units), observations=len(b.observations), sites=len(b.sites))}) for b in bundles]


@pytest.fixture(scope="module")
def gates(bundles):
    return [run_gate(b, REGISTRIES, "production") for b in bundles]


# ═════════════════════════════════════════════════════════════════════════════
# the archive codec, no database
# ═════════════════════════════════════════════════════════════════════════════

def test_the_value_codec_is_the_one_of_the_backup_job_and_the_restore_script():
    from neo4j.spatial import WGS84Point
    from neo4j.time import Date, DateTime, Duration, Time

    from scripts import neo4j_restore_from_export as restore

    values = ["text", 7, 2.5, True, None, ["a", 1], float("nan"), float("inf"), b"\x00\x01", Date(2024, 1, 2),
              DateTime(2024, 5, 1, 10, 0, 0, 0), Time(10, 0, 0, 0), Duration(months=1, days=2, seconds=3, nanoseconds=4),
              WGS84Point((-1.6, 42.8))]
    for value in values:
        assert export.enc(value) == restore.enc(value)
    assert export.canon({"b": 1, "a": [1, 2]}) == restore.canon({"a": [1, 2], "b": 1})


def _records():
    nodes = [{"id": "x1", "l": ["B", "A"], "p": {"k": 1}}, {"id": "x2", "l": ["A"], "p": {"k": 2}},
             {"id": "x3", "l": ["SchemaVersion"], "p": {"file": "f", "appliedAt": "2026-01-01T00:00:00Z"}}]
    rels = [{"id": "e1", "t": "T", "s": "x1", "e": "x2", "p": {}}]
    return nodes, rels


def test_the_canonical_lines_do_not_depend_on_ids_or_input_order():
    nodes, rels = _records()
    a = export._canonical_graph(nodes, rels)
    renamed = [{**n, "id": f"zz{i}"} for i, n in enumerate(reversed(nodes))]
    mapping = {old["id"]: new["id"] for old, new in zip(reversed(nodes), renamed, strict=True)}
    b = export._canonical_graph(renamed, [{**r, "id": "q", "s": mapping[r["s"]], "e": mapping[r["e"]]} for r in rels])
    assert a[0] == b[0] and a[1] == b[1]
    assert export.export_hash_of(a[0], a[1]) == export.export_hash_of(b[0], b[1])
    assert a[2]["exported"] == b[2]["exported"]


def test_wall_clock_properties_are_not_exported():
    nodes, rels = _records()
    node_lines, _, _ = export._canonical_graph(nodes, rels)
    assert not any(b"appliedAt" in line for line in node_lines)


def test_content_change_changes_the_hash():
    nodes, rels = _records()
    changed = [dict(n) for n in nodes]
    changed[0] = {**changed[0], "p": {"k": 99}}
    a, b = export._canonical_graph(nodes, rels), export._canonical_graph(changed, rels)
    assert export.export_hash_of(a[0], a[1]) != export.export_hash_of(b[0], b[1])


def test_identical_twins_are_counted_as_ambiguous():
    twins = [{"id": "a", "l": ["A"], "p": {"k": 1}}, {"id": "b", "l": ["A"], "p": {"k": 1}}]
    assert export._canonical_graph(twins, [])[2]["ambiguous"] == 1


def test_a_relationship_to_a_missing_node_is_refused():
    nodes, _ = _records()
    with pytest.raises(export.ExportError, match="endpoint"):
        export._canonical_graph(nodes, [{"id": "e", "t": "T", "s": "x1", "e": "gone", "p": {}}])


# ═════════════════════════════════════════════════════════════════════════════
# container tests
# ═════════════════════════════════════════════════════════════════════════════

async def _q(d, cypher: str, **params) -> list[dict]:
    async with d.session() as s:
        res = await s.run(cypher, **params)
        return [dict(r) async for r in res]


def _build(container, bundles) -> tuple:
    """A fresh build: migrations, then every bundle. Returns (async driver, sync driver)."""
    auth = (container.username, container.password)
    d = AsyncGraphDatabase.driver(container.get_connection_url(), auth=auth)
    _run(apply_migrations(d))
    for bundle in bundles:
        _run(loader.load(bundle, d, registries=REGISTRIES))
    return d, GraphDatabase.driver(container.get_connection_url(), auth=auth)


@pytest.fixture(scope="module")
def built(bundles):
    if shutil.which("docker") is None:
        pytest.skip("docker unavailable")
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d, sd = _build(n, bundles)
        yield n, d, sd
        _run(d.close())
        sd.close()


@pytest.fixture
def reset(built, bundles):
    """The built graph, restored to its clean state after a test that damages it."""
    _, d, _ = built
    yield built
    _run(_q(d, "MATCH (n) WHERE NOT n:SchemaVersion DETACH DELETE n"))
    for bundle in bundles:
        _run(loader.load(bundle, d, registries=REGISTRIES))


def _verify(built, bundles, contracts, gates, **kw):
    _, d, sd = built
    return _run(verify.verify(d, sd, bundles, contracts, gates, REGISTRIES, **kw))


@needs_docker
def test_verify_passes_on_a_clean_build_and_reports_sites_apart(built, bundles, contracts, gates):
    report = _verify(built, bundles, contracts, gates)
    assert report.ok, report.problems
    assert {c.name for c in report.checks} == {"counts", "duplicates", "gate", "unmapped", "orphans",
                                               "yield_metric", "licence"}
    view = report.sites["GENVCE"]
    # GENVCE has no field sites: its sites are zone aggregates and the unlabelled-table aggregate
    assert view.field_sites == 0 and view.zone_aggregate_sites > 0 and view.unlabelled_aggregate_sites <= 1
    assert view.field_units + view.zone_aggregate_units + view.unlabelled_aggregate_units == len(bundles[0].units)
    assert report.sites["CREA"].field_sites > 0
    total = report.sites["ALL"]
    assert total.field_units + total.zone_aggregate_units + total.unlabelled_aggregate_units == sum(
        len(b.units) for b in bundles)
    json.dumps(report.to_dict())


@needs_docker
def test_verify_fails_when_the_contract_expects_other_counts(built, bundles, contracts, gates):
    wrong = [contracts[0].model_copy(update={"expected": Expected(units=1, observations=1, sites=1)}), contracts[1]]
    report = _verify(built, bundles, wrong, gates)
    assert not report.check("counts").ok
    assert any("contract expects" in p for p in report.check("counts").problems)


@needs_docker
def test_verify_fails_on_a_missing_observation(reset, bundles, contracts, gates):
    _, d, _ = reset
    _run(_q(d, "MATCH (o:Observation) WITH o LIMIT 1 DETACH DELETE o"))
    check = _verify(reset, bundles, contracts, gates).check("counts")
    assert not check.ok and any("observations" in p for p in check.problems)


@needs_docker
def test_verify_fails_on_a_content_duplicate_under_another_key(reset, bundles, contracts, gates):
    _, d, _ = reset
    _run(_q(d, "MATCH (u:ObservationUnit {source_id: 'CREA'}) WITH u LIMIT 1 "
               "CREATE (c:ObservationUnit {unitKey: 'twin'}) SET c += u {.*, unitKey: 'twin'} "
               "WITH u, c MATCH (u)-[:TRIAL_AT]->(t) CREATE (c)-[:TRIAL_AT]->(t) "
               "WITH u, c MATCH (o:Observation)-[:ON_UNIT]->(u) "
               "CREATE (o2:Observation {obsKey: 'twin-' + o.obsKey}) SET o2 += o {.*, obsKey: 'twin-' + o.obsKey} "
               "CREATE (o2)-[:ON_UNIT]->(c)"))
    check = _verify(reset, bundles, contracts, gates).check("duplicates")
    assert not check.ok and any("same content and the same observations" in p for p in check.problems)


@needs_docker
def test_units_with_the_same_coordinates_but_other_observations_are_not_duplicates(reset, bundles, contracts, gates):
    _, d, _ = reset
    _run(_q(d, "MATCH (u:ObservationUnit {source_id: 'CREA'}) WITH u LIMIT 1 "
               "CREATE (c:ObservationUnit {unitKey: 'sibling'}) SET c += u {.*, unitKey: 'sibling'} "
               "WITH u, c MATCH (u)-[:TRIAL_AT]->(t) CREATE (c)-[:TRIAL_AT]->(t)"))
    check = _verify(reset, bundles, contracts, gates).check("duplicates")
    assert check.ok, check.problems
    assert check.detail["same_coordinates_different_observations_groups"]["CREA"] == 1


@needs_docker
def test_verify_fails_on_a_node_without_its_key(reset, bundles, contracts, gates):
    _, d, _ = reset
    _run(_q(d, "CREATE (:Study {source_id: 'CREA'})"))
    check = _verify(reset, bundles, contracts, gates).check("duplicates")
    assert not check.ok and any("Study" in p for p in check.problems)


@needs_docker
def test_verify_fails_on_an_observation_without_a_registered_variable(reset, bundles, contracts, gates):
    _, d, _ = reset
    _run(_q(d, "MATCH (o:Observation) WITH o LIMIT 1 SET o.variableId = 'not_a_variable'"))
    check = _verify(reset, bundles, contracts, gates).check("unmapped")
    assert not check.ok and any("registered variable" in p for p in check.problems)


@needs_docker
def test_verify_fails_on_orphans_above_the_threshold_and_passes_below_it(reset, bundles, contracts, gates):
    _, d, _ = reset
    _run(_q(d, "MATCH (u:ObservationUnit {source_id: 'CREA'})-[r:TRIAL_AT]->() WITH r LIMIT 1 DELETE r"))
    strict = _verify(reset, bundles, contracts, gates).check("orphans")
    assert not strict.ok and any("without a site" in p for p in strict.problems)
    relaxed = _verify(reset, bundles, contracts, gates,
                      thresholds=verify.Thresholds(max_units_without_site_ratio=0.5, max_sites_without_units=5))
    assert relaxed.check("orphans").ok
    # the orphan is reported apart in the sites view, never folded into a kind
    assert relaxed.sites["CREA"].units_without_site == 1


@needs_docker
def test_verify_fails_on_an_empty_site_and_an_unlinked_document(reset, bundles, contracts, gates):
    _, d, _ = reset
    _run(_q(d, "CREATE (:TrialSite {siteKey: 'EMPTY', name: 'empty', siteKind: 'field', sourceIds: ['CREA']})"))
    _run(_q(d, "CREATE (:ArticleSource {documentKey: 'lonely', source_id: 'CREA'})"))
    check = _verify(reset, bundles, contracts, gates).check("orphans")
    assert any("without units" in p and "site" in p for p in check.problems)
    assert any("document(s) without units" in p for p in check.problems)


@needs_docker
def test_verify_fails_on_a_yield_without_a_metric(reset, bundles, contracts, gates):
    _, d, _ = reset
    _run(_q(d, "MATCH (u:ObservationUnit) WHERE u.yieldKgHa IS NOT NULL WITH u LIMIT 1 REMOVE u.yieldMetric"))
    _run(_q(d, "MATCH (o:Observation {variableId: 'crop_yield'}) WHERE o.value IS NOT NULL WITH o LIMIT 1 REMOVE o.metric"))
    check = _verify(reset, bundles, contracts, gates).check("yield_metric")
    assert not check.ok and len(check.problems) == 2


@needs_docker
def test_verify_fails_on_a_source_without_a_permitted_licence(reset, bundles, contracts, gates):
    _, d, _ = reset
    _run(_q(d, "MATCH (s:Source {sourceId: 'CREA'}) SET s.commercialUse = 'denied'"))
    check = _verify(reset, bundles, contracts, gates).check("licence")
    assert not check.ok and any("not a permitted licence" in p for p in check.problems)
    _run(_q(d, "MATCH (s:Source {sourceId: 'CREA'}) DETACH DELETE s"))
    assert any("no Source node" in p for p in _verify(reset, bundles, contracts, gates).check("licence").problems)


@needs_docker
def test_verify_fails_when_a_gate_report_is_missing_or_failed(built, bundles, contracts, gates):
    assert any("no gate report" in p for p in _verify(built, bundles, contracts, gates[:1]).check("gate").problems)
    failed = gates[0].model_copy(update={"status": "fail"})
    assert not _verify(built, bundles, contracts, [failed, gates[1]]).check("gate").ok


@needs_docker
def test_verify_writes_nothing(built, bundles, contracts, gates):
    _, d, _ = built
    before = _run(_q(d, "MATCH (n) RETURN count(n) AS n"))[0]["n"], _run(_q(d, "MATCH ()-[r]->() RETURN count(r) AS r"))[0]["r"]
    _verify(built, bundles, contracts, gates)
    assert (_run(_q(d, "MATCH (n) RETURN count(n) AS n"))[0]["n"],
            _run(_q(d, "MATCH ()-[r]->() RETURN count(r) AS r"))[0]["r"]) == before


@needs_docker
def test_the_archive_restores_with_the_real_script_into_an_empty_server(built, tmp_path):
    _, d, _ = built
    result = _run(export.export_graph(d, tmp_path / "neo4j-build.tar"))
    assert result.nodes > 0 and result.relationships > 0 and result.ambiguous_nodes == 0
    manifest = export.verify_archive(result.path)
    assert manifest["export_hash"] == result.export_hash
    with tarfile.open(result.path) as tar:
        assert tar.getnames() == list(export.MEMBERS)
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as target:
        env = {**os.environ, "NEO4J_PASSWORD": "testpassword"}
        done = subprocess.run(
            [sys.executable, str(BACKEND / "scripts" / "neo4j_restore_from_export.py"), "--archive", str(result.path),
             "--uri", target.get_connection_url(), "--workdir", str(tmp_path), "--confirm-empty-target"],
            env=env, cwd=BACKEND, capture_output=True, text=True, timeout=600, check=False)
        assert done.returncode == 0, done.stdout + done.stderr
        assert "verification OK" in done.stdout


@needs_docker
def test_two_builds_from_the_same_inputs_give_the_same_export_hash_and_bytes(bundles, tmp_path):
    results = []
    for i in range(2):
        with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
            d, sd = _build(n, bundles)
            try:
                results.append(_run(export.export_graph(d, tmp_path / f"build-{i}.tar")))
            finally:
                _run(d.close())
                sd.close()
    a, b = results
    assert a.export_hash == b.export_hash
    assert a.archive_sha256 == b.archive_sha256
    assert a.fingerprint == b.fingerprint and a.ambiguous_nodes == b.ambiguous_nodes == 0
    assert (tmp_path / "build-0.tar").read_bytes() == (tmp_path / "build-1.tar").read_bytes()


@needs_docker
def test_a_paged_read_gives_the_same_archive_as_one_big_page(built, tmp_path, monkeypatch):
    _, d, _ = built
    whole = _run(export.export_graph(d, tmp_path / "whole.tar"))
    monkeypatch.setattr(export, "READ_PAGE_SIZE", 7)
    paged = _run(export.export_graph(d, tmp_path / "paged.tar"))
    assert paged.export_hash == whole.export_hash and paged.nodes == whole.nodes > 7
    assert (tmp_path / "paged.tar").read_bytes() == (tmp_path / "whole.tar").read_bytes()


@needs_docker
def test_a_changed_graph_changes_the_export_hash(built, tmp_path):
    _, d, _ = built
    first = _run(export.export_graph(d, tmp_path / "a.tar"))
    _run(_q(d, "MATCH (u:ObservationUnit) WITH u LIMIT 1 SET u.probe = 1"))
    try:
        second = _run(export.export_graph(d, tmp_path / "b.tar"))
    finally:
        _run(_q(d, "MATCH (u:ObservationUnit) WHERE u.probe IS NOT NULL REMOVE u.probe"))
    assert first.export_hash != second.export_hash


@needs_docker
def test_a_tampered_archive_is_refused(built, tmp_path):
    _, d, _ = built
    result = _run(export.export_graph(d, tmp_path / "t.tar"))
    with tarfile.open(result.path) as tar:
        members = {m.name: tar.extractfile(m).read() for m in tar.getmembers()}
    lines = gzip.decompress(members["nodes.jsonl.gz"]).splitlines(keepends=True)
    members["nodes.jsonl.gz"] = gzip.compress(b"".join(lines[:-1]))
    bad = tmp_path / "bad.tar"
    with tarfile.open(bad, "w:") as tar:
        for name in export.MEMBERS:
            info = tarfile.TarInfo(name)
            info.size = len(members[name])
            tar.addfile(info, io.BytesIO(members[name]))
    with pytest.raises(export.ExportError):
        export.verify_archive(bad)


@needs_docker
def test_an_empty_database_is_not_exported(tmp_path):
    with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
        d = AsyncGraphDatabase.driver(n.get_connection_url(), auth=(n.username, n.password))
        try:
            with pytest.raises(export.ExportError, match="empty"):
                _run(export.export_graph(d, tmp_path / "e.tar"))
        finally:
            _run(d.close())


# ═════════════════════════════════════════════════════════════════════════════
# the real bundles (opt-in: the private raw-data repository)
# ═════════════════════════════════════════════════════════════════════════════

@needs_docker
@pytest.mark.skipif(not RAW_REPO, reason="set NKZ_DATA_SOURCES_DIR to the raw-data repository to run")
def test_the_real_build_verifies_and_exports_deterministically(tmp_path):
    real = [run_contract(CONTRACTS["GENVCE"], REGISTRIES, genvce.load(Path(RAW_REPO) / "genvce").rows),
            run_contract(CONTRACTS["CREA"], REGISTRIES, crea.load(Path(RAW_REPO) / "crea").rows)]
    real_gates = [run_gate(b, REGISTRIES, "production") for b in real]
    assert all(g.status == "pass" for g in real_gates)
    hashes = []
    for i in range(2):
        with Neo4jContainer("neo4j:5.26-community", password="testpassword") as n:
            d, sd = _build(n, real)
            try:
                report = _run(verify.verify(d, sd, real, list(CONTRACTS.values()), real_gates, REGISTRIES))
                if i == 0:
                    # GENVCE prints some 2023 zone means in two tables of one report; the adapter keeps the
                    # lowest table (alias recorded), so nothing repeats.
                    assert report.ok, report.problems
                    assert {c.name: c for c in report.checks}[
                        "duplicates"].detail["content_duplicate_groups"] == {"GENVCE": 0, "CREA": 0}
                    v = report.sites
                    assert v["GENVCE"].field_sites == 0 and v["GENVCE"].unlabelled_aggregate_units == 1758
                    assert v["ALL"].units_without_site == 0
                hashes.append(_run(export.export_graph(d, tmp_path / f"real-{i}.tar")))
            finally:
                _run(d.close())
                sd.close()
    assert hashes[0].export_hash == hashes[1].export_hash
    assert hashes[0].archive_sha256 == hashes[1].archive_sha256
    assert hashes[0].ambiguous_nodes == 0

