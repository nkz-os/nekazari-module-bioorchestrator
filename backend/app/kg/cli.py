"""Build orchestrator CLI (plan task 10): ``python -m app.kg build ...`` and ``python -m app.kg export ...``.

Stages of ``build``, each writing a JSON manifest under ``<out>/<build-id>/``:

1. ``verify_raw``   raw-data repository clean (reproducibility), RAW_MANIFEST fingerprints of the files present;
2. ``registries``   load and hash the registries and the source contracts;
3. ``adapt_gate``   per source: adapter -> contract -> quality gate (read-only duplicate check when a target is given);
4. stop here unless ``--execute`` (dry run: counts and gate, **no write of any kind**);
5. ``schema``       migrations (the first write);
6. ``load``         per source batched MERGE, then ``stamp_phenology_species_name`` after the reference knowledge;
7. ``link``         re-link and cardinality check;
8. ``enrich``       CHELSA climate for field sites (offline cache by default);
9. ``verify``       acceptance checks;
10. ``export``      deterministic archive + ``export_hash``.

Safety (the production graph is untouchable from here in F1). A write needs ALL of:

* ``--execute`` (default is a dry run);
* ``--target-label`` that is a scratch label (``local``, ``test``, ``ci``, ``scratch`` or ``build``, optionally
  followed by ``-<suffix>``); production-like labels are rejected, so naming a target takes a deliberate act;
* a target host that is loopback, or listed in ``NKZ_KG_ALLOWED_TARGET_HOSTS`` (comma separated; nothing is
  allowed by default, no host is named in this repository);
* a target that is not the backend's own configured graph (``NEO4J_URI`` of the environment);
* a target that is empty (or ``--allow-existing``). A target holding legacy ``VarietyTrial`` nodes (no ``unitKey``)
  is refused, with or without ``--allow-existing``, unless it carries this tool's scratch marker with the same
  label (the restored-copy flow: ``mark-target`` on the empty target, restore the copy, ``migrate-restored``,
  ``replace-sources``, ``build``);
* an explicit environment marker: the build writes a singleton ``(:KgBuildTarget {label, created_by_build})`` on an
  empty target, and a NON-EMPTY target is written only if it carries that marker with a scratch label equal to
  ``--target-label``. A graph without the marker (any production graph, whatever its schema) or with another label
  is refused, even with ``--allow-existing``. The marker is not part of the export, so a restored graph has none;
* a gate that passed: a refused gate never writes, not even the schema.

Credentials come from ``NEO4J_USER`` (default ``neo4j``) and ``NEO4J_PASSWORD``; never from arguments or logs.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from neo4j import READ_ACCESS, AsyncGraphDatabase, GraphDatabase

from . import enrich as enrich_mod
from . import export as export_mod
from . import gate as gate_mod
from . import link as link_mod
from . import loader
from . import verify as verify_mod
from .adapters import AdapterError, crea, genvce
from .contracts import Bundle, Contract, ContractError, load_contract, run_contract
from .existing_graph import Neo4jExistingGraph
from .migrations import MigrationError, apply_migrations, discover_migrations
from .registries import RegistryError, load_registries

logger = logging.getLogger("app.kg.cli")

BACKEND = Path(__file__).resolve().parents[2]
SOURCES_DIR = BACKEND / "data" / "sources"
DEFAULT_OUT = BACKEND / "kg-build" / "out"
DEFAULT_CACHE = BACKEND / "kg-build" / "cache" / "chelsa-cells.json"

# source id -> (folder in the raw-data repository, adapter loader)
SOURCES: dict[str, tuple[str, Callable[[Path], Any]]] = {
    "GENVCE": ("genvce", genvce.load),
    "CREA": ("crea", crea.load),
}

SCRATCH_LABEL = re.compile(r"^(local|test|ci|scratch|build)(-[a-z0-9][a-z0-9._-]{0,40})?$")
ALLOWED_HOSTS_ENV = "NKZ_KG_ALLOWED_TARGET_HOSTS"
LOOPBACK = {"localhost", "127.0.0.1", "::1"}
MARKER_LABEL = export_mod.MARKER_LABEL

EXIT_OK, EXIT_FAILED, EXIT_REFUSED = 0, 1, 2


class WriteRefused(RuntimeError):
    """The safety rules forbid writing to this target; nothing was written."""


class BuildFailed(RuntimeError):
    """A stage failed; the message names it. Whatever was written stays as it is (idempotent re-run fixes it)."""


@dataclass(frozen=True)
class BuildConfig:
    sources: tuple[str, ...]
    raw_dir: Path
    profile: str = "production"  # gate target: "production" or "local-test"
    out_dir: Path = DEFAULT_OUT
    target: str | None = None
    target_label: str | None = None
    execute: bool = False
    allow_existing: bool = False
    allow_dirty: bool = False
    climate_cache: Path = DEFAULT_CACHE
    chelsa_online: bool = False
    batch_size: int = loader.DEFAULT_BATCH_SIZE
    database: str | None = None


@dataclass(frozen=True)
class TargetConfig:
    """What the target-only commands (``mark-target``, ``replace-sources``, ``migrate-restored``) need."""

    target: str | None = None
    target_label: str | None = None
    execute: bool = False
    database: str | None = None
    out_dir: Path = DEFAULT_OUT


@dataclass
class BuildResult:
    build_id: str
    status: str  # "ok" | "dry-run" | "refused-gate" | "failed"
    out: Path
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def exit_code(self) -> int:
        return EXIT_OK if self.status in ("ok", "dry-run") else EXIT_FAILED


# ═════════════════════════════════════════════════════════════════════════════
# safety
# ═════════════════════════════════════════════════════════════════════════════

def _host_port(uri: str) -> tuple[str, int]:
    parts = urlsplit(uri)
    if parts.scheme not in ("bolt", "neo4j", "bolt+s", "neo4j+s", "bolt+ssc", "neo4j+ssc") or not parts.hostname:
        raise WriteRefused(f"target {uri!r} is not a bolt/neo4j URI")
    return parts.hostname.lower(), parts.port or 7687


def authorize_write(cfg: BuildConfig | TargetConfig, env: Mapping[str, str]) -> None:
    """Raise :class:`WriteRefused` unless every static safety rule allows writing to ``cfg.target``.

    The checks that need the database (empty / legacy-looking target) run later, in :func:`_guard_target_contents`.
    """
    if cfg.target is None:
        raise WriteRefused("--execute needs --target")
    label = cfg.target_label
    if not label:
        raise WriteRefused("--execute needs --target-label (a scratch label: local|test|ci|scratch|build[-suffix])")
    if not SCRATCH_LABEL.match(label):
        raise WriteRefused(f"target label {label!r} is not a scratch label (local|test|ci|scratch|build[-suffix]); "
                           "production-like targets cannot be written by this tool")
    authorize_host(cfg.target, env)


def authorize_host(target: str, env: Mapping[str, str]) -> None:
    """Host rules shared by build and export: loopback or listed in the allowlist, never the backend's own graph."""
    host, port = _host_port(target)
    allowed = {h.strip().lower() for h in env.get(ALLOWED_HOSTS_ENV, "").split(",") if h.strip()}
    if host not in LOOPBACK and host not in allowed:
        raise WriteRefused(f"target host is neither loopback nor listed in {ALLOWED_HOSTS_ENV}")
    configured = env.get("NEO4J_URI")
    if configured:
        try:
            same = _host_port(configured) == (host, port)
        except WriteRefused:
            same = False  # an unparsable NEO4J_URI cannot be compared; the other rules still apply
        if same:
            raise WriteRefused("target is the graph this environment's backend is configured to serve (NEO4J_URI)")


# ═════════════════════════════════════════════════════════════════════════════
# reproducibility
# ═════════════════════════════════════════════════════════════════════════════

def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=False)
    if done.returncode != 0:
        raise BuildFailed(f"git {' '.join(args)} failed in {repo.name}: {done.stderr.strip()[:200]}")
    return done.stdout.strip()


def repo_state(path: Path) -> dict[str, Any]:
    top = Path(_git(path, "rev-parse", "--show-toplevel"))
    return {"sha": _git(top, "rev-parse", "HEAD"), "dirty": bool(_git(top, "status", "--porcelain"))}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _check_raw_manifest(source_dir: Path) -> dict[str, Any]:
    """Fingerprints of the raw files this machine has, against the repository's RAW_MANIFEST.sha256."""
    manifest = source_dir / "RAW_MANIFEST.sha256"
    if not manifest.exists():
        raise BuildFailed(f"{source_dir.name}: RAW_MANIFEST.sha256 is missing")
    listed = present = 0
    mismatched: list[str] = []
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        sha, _size, rel = line.split(None, 2)
        listed += 1
        path = source_dir / rel
        if path.is_file():
            present += 1
            if _sha256_file(path) != sha:
                mismatched.append(rel)
    if mismatched:
        raise BuildFailed(f"{source_dir.name}: raw file(s) differ from RAW_MANIFEST: {mismatched[:5]}")
    return {"listed": listed, "present_locally": present, "absent_locally": listed - present}


# ═════════════════════════════════════════════════════════════════════════════
# manifests
# ═════════════════════════════════════════════════════════════════════════════

def _write_manifest(out: Path, stage: str, data: Mapping[str, Any]) -> None:
    out.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n"
    tmp = out / f".{stage}.json.tmp"
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, out / f"{stage}.json")
    logger.info("kg build stage=%s manifest=%s", stage, out / f"{stage}.json")


def _counts(bundle: Bundle) -> dict[str, int]:
    return {"documents": len(bundle.documents), "studies": len(bundle.studies), "sites": len(bundle.sites),
            "varieties": len(bundle.varieties), "units": len(bundle.units),
            "observations": len(bundle.observations)}


def _auth(env: Mapping[str, str]) -> tuple[str, str]:
    password = env.get("NEO4J_PASSWORD")
    if not password:
        raise WriteRefused("NEO4J_PASSWORD is not set")
    return env.get("NEO4J_USER", "neo4j"), password


# ═════════════════════════════════════════════════════════════════════════════
# the build
# ═════════════════════════════════════════════════════════════════════════════

async def _scalar(driver: Any, database: str | None, cypher: str) -> int:
    async with driver.session(database=database, default_access_mode=READ_ACCESS) as session:
        record = await (await session.run(cypher)).single()
        return int(record["c"])


async def _read_marker(driver: Any, database: str | None) -> dict[str, Any] | None:
    """The environment marker, ``None`` if absent; more than one marker is refused (it is a singleton)."""
    async with driver.session(database=database, default_access_mode=READ_ACCESS) as session:
        result = await session.run(f"MATCH (m:{MARKER_LABEL}) RETURN m.label AS label, "
                                   "m.created_by_build AS created_by_build")
        rows = [r.data() for r in await result.fetch(10)]
    if len(rows) > 1:
        raise WriteRefused(f"target holds {len(rows)} {MARKER_LABEL} markers (must be exactly one): refusing")
    return rows[0] if rows else None


def _check_scratch_marker(marker: Mapping[str, Any] | None, cfg: BuildConfig | TargetConfig, *, why: str) -> None:
    """Refuse unless ``marker`` is this tool's scratch marker carrying ``cfg.target_label``."""
    if marker is None:
        raise WriteRefused(f"target has no {MARKER_LABEL} marker: it was not created by this tool, refusing "
                           f"({why})")
    if marker["label"] != cfg.target_label:
        raise WriteRefused(f"target is marked {marker['label']!r}, not {cfg.target_label!r}: refusing")
    if marker["created_by_build"] is not True or not SCRATCH_LABEL.match(str(marker["label"])):
        raise WriteRefused(f"target marker {marker['label']!r} is not a scratch build marker: refusing")


async def _require_marked_target(driver: Any, cfg: BuildConfig | TargetConfig, *, why: str) -> None:
    _check_scratch_marker(await _read_marker(driver, cfg.database), cfg, why=why)


async def _guard_target_contents(driver: Any, cfg: BuildConfig) -> dict[str, int]:
    """Refuse a legacy-looking graph unless marked, and a non-empty one unless it carries the scratch marker."""
    legacy = await _scalar(driver, cfg.database,
                           "MATCH (n:VarietyTrial) WHERE n.unitKey IS NULL RETURN count(n) AS c")
    nodes = await _scalar(driver, cfg.database,
                          f"MATCH (n) WHERE NOT n:SchemaVersion AND NOT n:{MARKER_LABEL} RETURN count(n) AS c")
    marker = await _read_marker(driver, cfg.database)
    if legacy:
        # Option b (build on a restored copy of the served graph) always has legacy trials. They are allowed only
        # on a target this tool marked while it was still empty; an unmarked one looks like the production graph.
        scratch_marker = (marker is not None and marker["label"] == cfg.target_label
                          and marker["created_by_build"] is True and bool(SCRATCH_LABEL.match(str(marker["label"]))))
        if not scratch_marker:
            raise WriteRefused(f"target holds {legacy} legacy VarietyTrial node(s) without unitKey and no scratch "
                               f"{MARKER_LABEL} marker for {cfg.target_label!r}: it looks like the production "
                               "graph, refusing")
    if marker is not None and marker["label"] != cfg.target_label:
        # also on an empty target: the marker names the environment, a different label is a different environment
        raise WriteRefused(f"target is marked {marker['label']!r}, not {cfg.target_label!r}: refusing")
    if nodes:
        if not cfg.allow_existing:
            raise WriteRefused(f"target is not empty ({nodes} nodes); pass --allow-existing to re-run on a build graph")
        if marker is None:
            raise WriteRefused(f"target is not empty and has no {MARKER_LABEL} marker: it was not created by this "
                               "tool, refusing even with --allow-existing")
        _check_scratch_marker(marker, cfg, why="non-empty target")
    return {"nodes_before": nodes}


async def _write_marker(driver: Any, cfg: BuildConfig | TargetConfig) -> None:
    """Singleton marker, written before the first data/schema write on an empty target (idempotent)."""
    async with driver.session(database=cfg.database) as session:
        await (await session.run(
            f"MERGE (m:{MARKER_LABEL} {{id: 'singleton'}}) "
            "ON CREATE SET m.label = $label, m.created_by_build = true, m.created_at = datetime()",
            label=cfg.target_label)).consume()


async def _require_current_schema(driver: Any, database: str | None) -> None:
    async with driver.session(database=database, default_access_mode=READ_ACCESS) as session:
        result = await session.run("MATCH (v:SchemaVersion) RETURN v.file AS file, v.sha256 AS sha")
        recorded = {r["file"]: r["sha"] for r in await result.fetch(10_000)}
    expected = {m.name: m.sha256 for m in discover_migrations()}
    if recorded != expected:
        stale = sorted(k for k in expected.keys() | recorded.keys() if expected.get(k) != recorded.get(k))
        raise WriteRefused(f"existing graph was built with a different schema ({stale[:5]}): rebuild it from empty "
                           "(a restored copy: run migrate-restored first)")


def _build_id(cfg: BuildConfig, state: Mapping[str, Any], registries_hash: str) -> str:
    digest = hashlib.sha256(json.dumps(
        [list(cfg.sources), cfg.profile, state, registries_hash], sort_keys=True).encode()).hexdigest()[:10]
    return f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{digest}"


async def _run(cfg: BuildConfig, env: Mapping[str, str], climate_reader: Any) -> BuildResult:
    unknown = [s for s in cfg.sources if s not in SOURCES]
    if unknown:
        raise BuildFailed(f"unknown source(s) {unknown}; known: {sorted(SOURCES)}")
    if cfg.execute:
        authorize_write(cfg, env)  # before any work: a refused target costs nothing
        _auth(env)
    if cfg.profile not in gate_mod.TARGETS:
        raise BuildFailed(f"profile must be one of {gate_mod.TARGETS}")

    # 1. verify-raw ------------------------------------------------------------------------------
    module_state = repo_state(BACKEND)
    data_state = repo_state(cfg.raw_dir)
    dirty = [name for name, st in (("module", module_state), ("raw-data", data_state)) if st["dirty"]]
    if dirty and not cfg.allow_dirty:
        raise BuildFailed(f"dirty worktree ({', '.join(dirty)}): commit first, or --allow-dirty for a local trial "
                          "(recorded in the manifest)")
    registries = load_registries()
    lock_hash = _sha256_file(BACKEND / "requirements.txt")
    state = {"module": module_state, "raw_data": data_state, "registries_hash": registries.registries_hash,
             "requirements_sha256": lock_hash,
             "neo4j_image_digest": env.get("NKZ_KG_NEO4J_IMAGE_DIGEST") or "unrecorded",
             "allow_dirty": cfg.allow_dirty}
    build_id = _build_id(cfg, state, registries.registries_hash)
    out = cfg.out_dir / build_id
    raw_report = {s: _check_raw_manifest(cfg.raw_dir / SOURCES[s][0]) for s in cfg.sources}
    _write_manifest(out, "01-verify-raw", {**state, "mode": "execute" if cfg.execute else "dry-run",
                                           "target_label": cfg.target_label, "raw_manifest": raw_report})

    # 2. registries + contracts ------------------------------------------------------------------
    contracts: dict[str, Contract] = {s: load_contract(SOURCES_DIR / f"{s}.yaml") for s in cfg.sources}
    _write_manifest(out, "02-registries", {"registries_hash": registries.registries_hash,
                                           "contracts": sorted(contracts)})

    # 3. adapt -> map -> gate (read-only duplicate check against the target when one is given) -----
    sync_driver = None
    if cfg.target is not None:
        user, password = _auth(env)
        sync_driver = GraphDatabase.driver(cfg.target, auth=(user, password))
    try:
        existing = Neo4jExistingGraph(sync_driver, database=cfg.database) if sync_driver is not None else None
        bundles: list[Bundle] = []
        gates: list[gate_mod.GateReport] = []
        stage3: dict[str, Any] = {}
        for source in cfg.sources:
            folder, adapter_load = SOURCES[source]
            adapted = adapter_load(cfg.raw_dir / folder)
            bundle = run_contract(contracts[source], registries, adapted.rows)
            report = gate_mod.run_gate(bundle, registries, cfg.profile, existing=existing,  # type: ignore[arg-type]
                                       adapter_warnings=adapted.warnings)
            gate_mod.write_report(report, out)
            gate_mod.write_review_queue(report, out)
            bundles.append(bundle)
            gates.append(report)
            stage3[source] = {"counts": _counts(bundle), "gate_status": report.status,
                              "publishable": report.publishable, "error_counts": report.error_counts,
                              "warning_counts": report.warning_counts,
                              "adapter_inputs": [list(i) for i in adapted.inputs],
                              "content_duplicate_check": report.content_duplicate_check}
        _write_manifest(out, "03-adapt-gate", stage3)
        refused = [g.source_id for g in gates if g.status != "pass" or (cfg.profile == "production" and not g.publishable)]
        if refused:
            summary = {"refused_sources": refused, "gate": {g.source_id: g.status for g in gates}}
            _write_manifest(out, "99-summary", {"status": "refused-gate", **summary})
            return BuildResult(build_id, "refused-gate", out, summary)

        if not cfg.execute:
            diff: dict[str, Any] = {s: stage3[s]["counts"] for s in cfg.sources}
            summary = {"mode": "dry-run", "counts": diff, "writes": 0}
            _write_manifest(out, "99-summary", {"status": "dry-run", **summary})
            return BuildResult(build_id, "dry-run", out, summary)
    finally:
        if sync_driver is not None:
            sync_driver.close()

    # execute: from here on the target is written -------------------------------------------------
    user, password = _auth(env)
    driver = AsyncGraphDatabase.driver(cfg.target, auth=(user, password))
    sync_driver = GraphDatabase.driver(cfg.target, auth=(user, password))
    try:
        before = await _guard_target_contents(driver, cfg)
        if before["nodes_before"] == 0:
            await _write_marker(driver, cfg)
            migrations = await apply_migrations(driver, database=cfg.database)
            schema = {"migrations": [m.__dict__ for m in migrations.applied]}
        else:
            # Re-run: the legacy data migrations (e.g. management tagging) rewrite loaded units, so they must
            # not run again. The schema has to be exactly the current one; anything else needs a fresh build.
            await _require_current_schema(driver, cfg.database)
            schema = {"migrations": "skipped: schema already current"}
        _write_manifest(out, "04-schema", {**before, **schema})

        loads = {}
        for bundle in bundles:
            loads[bundle.source_id] = await loader.load(bundle, driver, cfg.batch_size, registries=registries,
                                                        database=cfg.database)
        stamped = await loader.stamp_phenology_species_name(driver, database=cfg.database)
        _write_manifest(out, "05-load", {
            "stamped_phenology_stages": stamped,
            "sources": {s: {"nodes_created": r.nodes_created, "relationships_created": r.relationships_created,
                            "batches": r.batches} for s, r in loads.items()}})

        links = {b.source_id: await link_mod.link(b, driver, registries=registries, batch_size=cfg.batch_size,
                                                  database=cfg.database) for b in bundles}
        _write_manifest(out, "06-link", {s: {"relinked": r.relinked, "problems": r.problems} for s, r in links.items()})
        bad = [p for r in links.values() for p in r.problems]
        if bad:
            raise BuildFailed(f"link: {bad[:5]}")

        sites = [site for b in bundles for site in b.sites]
        enriched = await enrich_mod.enrich_sites(sites, cache_path=cfg.climate_cache, reader=climate_reader,
                                                 offline=not cfg.chelsa_online)
        written = await enrich_mod.apply_enrichment(enriched, driver, database=cfg.database)
        _write_manifest(out, "07-enrich", {
            "mode": "online" if cfg.chelsa_online else "offline-cache", "counts": enriched.counts,
            "written": written, "failed_sites": list(enriched.failed),
            "country_mismatch": enriched.country_mismatch})

        verified = await verify_mod.verify(driver, sync_driver, bundles, [contracts[b.source_id] for b in bundles],
                                           gates, registries, database=cfg.database)
        _write_manifest(out, "08-verify", {
            "ok": verified.ok, "problems": verified.problems,
            "checks": {c.name: {"ok": c.ok, "detail": c.detail} for c in verified.checks},
            "sites": {k: v.to_dict() for k, v in verified.sites.items()}})
        if not verified.ok:
            raise BuildFailed(f"verify: {verified.problems[:5]}")

        exported = await export_mod.export_graph(driver, out / "neo4j-build.tar", database=cfg.database)
        _write_manifest(out, "09-export", exported.to_dict())
    finally:
        await driver.close()
        sync_driver.close()
    summary = {"mode": "execute", "target_label": cfg.target_label, "export_hash": exported.export_hash,
               "archive": str(exported.path), "nodes": exported.nodes, "relationships": exported.relationships,
               "nodes_created": {s: r.nodes_created for s, r in loads.items()}}
    _write_manifest(out, "99-summary", {"status": "ok", **summary})
    return BuildResult(build_id, "ok", out, summary)


def run_build(cfg: BuildConfig, *, env: Mapping[str, str] | None = None, climate_reader: Any = None) -> BuildResult:
    return asyncio.run(_run(cfg, os.environ if env is None else env, climate_reader))


async def _mark_target(cfg: TargetConfig, env: Mapping[str, str]) -> dict[str, Any]:
    """Write the scratch marker on a completely EMPTY target (no node, constraint or index): step one of option b.

    The marker is what later allows legacy trials in the target (a restored copy), so it may only be created
    while the target is provably nothing else: nothing can be marked after the fact. Idempotent when the target
    holds only the same marker.
    """
    authorize_write(cfg, env)
    user, password = _auth(env)
    driver = AsyncGraphDatabase.driver(cfg.target, auth=(user, password))
    try:
        nodes = await _scalar(driver, cfg.database, f"MATCH (n) WHERE NOT n:{MARKER_LABEL} RETURN count(n) AS c")
        schema = await _scalar(driver, cfg.database, "SHOW CONSTRAINTS YIELD name RETURN count(name) AS c") + \
            await _scalar(driver, cfg.database, "SHOW INDEXES YIELD type WHERE type <> 'LOOKUP' RETURN count(*) AS c")
        marker = await _read_marker(driver, cfg.database)
        if marker is not None:
            _check_scratch_marker(marker, cfg, why="existing marker")
        if nodes or schema:
            raise WriteRefused(f"target is not empty ({nodes} nodes, {schema} schema rules): a marker can only be "
                               "created on an empty target")
        if not cfg.execute:
            return {"mode": "dry-run", "target_label": cfg.target_label, "marker_present": marker is not None,
                    "writes": 0}
        await _write_marker(driver, cfg)  # type: ignore[arg-type]
        return {"mode": "execute", "target_label": cfg.target_label, "marker_present": True,
                "created": marker is None}
    finally:
        await driver.close()


async def _export_only(target: str, label: str | None, out_path: Path, env: Mapping[str, str]) -> export_mod.ExportResult:
    authorize_host(target, env)  # a read of the live graph is refused too: it is a build-instance tool
    user, password = _auth(env)
    driver = AsyncGraphDatabase.driver(target, auth=(user, password))
    try:
        return await export_mod.export_graph(driver, out_path)
    finally:
        await driver.close()


# ═════════════════════════════════════════════════════════════════════════════
# command line
# ═════════════════════════════════════════════════════════════════════════════

def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m app.kg", description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build", help="adapt -> gate -> (with --execute) load, link, enrich, verify, export")
    b.add_argument("--sources", default="GENVCE,CREA", help="comma separated source ids")
    b.add_argument("--raw-dir", default=os.environ.get("NKZ_DATA_SOURCES_DIR", ""),
                   help="raw-data repository (default: $NKZ_DATA_SOURCES_DIR)")
    b.add_argument("--profile", choices=gate_mod.TARGETS, default="production", help="gate target")
    b.add_argument("--out", default=str(DEFAULT_OUT), help="manifest directory (gitignored)")
    b.add_argument("--target", help="bolt:// URI of a build graph (read-only unless --execute)")
    b.add_argument("--target-label", help="scratch label confirming the target is not production")
    b.add_argument("--execute", action="store_true", help="write to the target (default: dry run)")
    b.add_argument("--allow-existing", action="store_true", help="re-run on a non-empty build graph")
    b.add_argument("--allow-dirty", action="store_true", help="local trial with uncommitted changes (recorded)")
    b.add_argument("--climate-cache", default=str(DEFAULT_CACHE), help="CHELSA cell cache (JSON)")
    b.add_argument("--chelsa-online", action="store_true", help="fetch missing CHELSA cells (default: cache only)")
    b.add_argument("--batch-size", type=int, default=loader.DEFAULT_BATCH_SIZE)
    m = sub.add_parser("mark-target", help="write the scratch marker on an EMPTY target (first step of a restored "
                                           "copy: mark, restore, migrate-restored, replace-sources, build)")
    m.add_argument("--target", required=True)
    m.add_argument("--target-label", required=True, help="scratch label")
    m.add_argument("--execute", action="store_true", help="write the marker (default: report only)")
    e = sub.add_parser("export", help="export a build graph to a deterministic archive (read-only)")
    e.add_argument("--target", required=True)
    e.add_argument("--out", required=True, help="archive path")
    return p


def main(argv: Sequence[str] | None = None, *, env: Mapping[str, str] | None = None, climate_reader: Any = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", stream=sys.stderr)
    args = _parser().parse_args(argv)
    environment = os.environ if env is None else env
    try:
        if args.command == "export":
            result = asyncio.run(_export_only(args.target, None, Path(args.out), environment))
            print(json.dumps(result.to_dict(), sort_keys=True, indent=2, default=str))
            return EXIT_OK
        if args.command == "mark-target":
            report = asyncio.run(_mark_target(
                TargetConfig(target=args.target, target_label=args.target_label, execute=args.execute), environment))
            print(json.dumps(report, sort_keys=True, indent=2, default=str))
            return EXIT_OK
        if not args.raw_dir:
            raise BuildFailed("--raw-dir or NKZ_DATA_SOURCES_DIR is required")
        cfg = BuildConfig(
            sources=tuple(s.strip() for s in args.sources.split(",") if s.strip()), raw_dir=Path(args.raw_dir),
            profile=args.profile, out_dir=Path(args.out), target=args.target, target_label=args.target_label,
            execute=args.execute, allow_existing=args.allow_existing, allow_dirty=args.allow_dirty,
            climate_cache=Path(args.climate_cache), chelsa_online=args.chelsa_online, batch_size=args.batch_size)
        result = run_build(cfg, env=environment, climate_reader=climate_reader)
    except WriteRefused as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    except (BuildFailed, AdapterError, ContractError, RegistryError, MigrationError, loader.LoadError) as exc:
        print(f"FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_FAILED
    print(json.dumps({"build_id": result.build_id, "status": result.status, "manifests": str(result.out),
                      **result.summary}, sort_keys=True, indent=2, default=str))
    return result.exit_code


__all__ = ["BuildConfig", "BuildFailed", "BuildResult", "TargetConfig", "WriteRefused", "authorize_host", "authorize_write", "main", "run_build"]
