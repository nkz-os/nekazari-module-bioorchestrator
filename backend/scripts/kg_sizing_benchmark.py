#!/usr/bin/env python3
"""Sizing benchmark of the KG loader (plan task 9, amendment E4): batch size, store size, hot-query memory.

Everything runs on throwaway ``neo4j:5.26-community`` containers (Docker, testcontainers) configured like
the production instance (heap 512m initial / 768m max, page cache 768m, same JVM flags). Nothing touches a
real graph.

``micro``  Per write step (unit MERGE, observation MERGE, observation->unit relationship) and per batch size,
           one transaction of exactly that many rows of the *largest real shape*, under a ladder of
           per-transaction memory limits (``db.memory.transaction.max``; Community cannot change it at
           runtime, so one container per limit). The result is, per step and size, the smallest limit
           of the ladder under which the transaction commits. The ladder includes a limit and a size that
           MUST fail (the negative control): a benchmark that never fails measures nothing.
``macro``  The projected volume (the real units x4 with about ten observations each) loaded with the chosen
           batch size under the budgeted limit, then: store size after a checkpoint, JVM heap-pool peaks,
           and PROFILE memory and time of the hot evidence queries.

The default batch size is picked by :func:`choose_batch_size` from the micro result: the largest size whose
worst step passes under HALF the budget (the ladder is a factor of two wide, so a pass at L means peak <= L
and the budget keeps a 2x margin on top).

    NKZ_DATA_SOURCES_DIR=<raw repo> python scripts/kg_sizing_benchmark.py all --out /path/outside/repo.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from neo4j.exceptions import Neo4jError
from testcontainers.neo4j import Neo4jContainer

from app.graph import evidence_policy as ep
from app.graph.dao import _extrapolate_batch_query
from app.kg import identity, loader
from app.kg.adapters import crea, genvce
from app.kg.contracts import Bundle, load_contract, run_contract
from app.kg.migrations import apply_migrations
from app.kg.registries import load_registries
from neo4j import AsyncGraphDatabase

BACKEND = Path(__file__).resolve().parents[1]
FIXTURES = BACKEND / "tests" / "kg" / "fixtures"
IMAGE = "neo4j:5.26-community"
PASSWORD = "benchmark-password"  # nosec: throwaway container
HEAP_INITIAL, HEAP_MAX, PAGECACHE = "512m", "768m", "768m"  # the production values
JVM_ADDITIONAL = "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=8m -XX:+ParallelRefProcEnabled"
LIMITS_MIB = (16, 32, 64, 128, 256)
SIZES = (500, 1000, 2500, 5000, 10000, 20000)
STEPS = ("unit", "observation", "rel:obs_unit")
BUDGET_MIB = 32  # per-transaction budget: 1/24 of the heap, so several writers and the API's reads coexist
SETUP_BATCH = 100
# The loader default is the output of choose_batch_size on the recorded benchmark (see the task report).
RECORDED_DEFAULT_BATCH_SIZE = 500
# A size that must NOT fit under the smallest limit of the ladder (the negative control).
NEGATIVE_CONTROL_SIZE = 5000
MEMORY_ERRORS = ("MemoryLimit", "MemoryPoolOutOfMemory", "OutOfMemory")


@contextmanager
def neo4j_like_production(tx_limit_mib: int | None) -> Iterator[Neo4jContainer]:
    container = (Neo4jContainer(IMAGE, password=PASSWORD)
                 .with_env("NEO4J_server_memory_heap_initial__size", HEAP_INITIAL)
                 .with_env("NEO4J_server_memory_heap_max__size", HEAP_MAX)
                 .with_env("NEO4J_server_memory_pagecache_size", PAGECACHE)
                 .with_env("NEO4J_server_jvm_additional", JVM_ADDITIONAL))
    if tx_limit_mib is not None:
        container = container.with_env("NEO4J_db_memory_transaction_max", f"{tx_limit_mib}m")
    with container as running:
        yield running


def _driver(container: Neo4jContainer) -> Any:
    # No retry: the driver retries a memory-pool error (a transient one) for 30 s, which only delays the verdict.
    return AsyncGraphDatabase.driver(container.get_connection_url(), auth=(container.username, container.password),
                                     max_transaction_retry_time=0)


async def _scalar(driver: Any, cypher: str, **params: Any) -> Any:
    async with driver.session() as session:
        record = await (await session.run(cypher, **params)).single()
        return record[0] if record is not None else None


# ═════════════════════════════════════════════════════════════════════════════
# shapes
# ═════════════════════════════════════════════════════════════════════════════

def _bundle(contract_name: str, adapter: Any, raw_dir: Path | None, registries: Any) -> Bundle:
    contract = load_contract(BACKEND / "data" / "sources" / f"{contract_name}.yaml")
    rows = adapter.load((raw_dir / contract_name.lower()) if raw_dir else (FIXTURES / contract_name.lower())).rows
    if raw_dir:
        return run_contract(contract, registries, rows)
    with mock.patch("app.kg.contracts._check_expected"):  # the fixtures hold a slice of the raw data
        return run_contract(contract, registries, rows)


def real_bundles(raw_dir: Path | None) -> list[Bundle]:
    registries = load_registries()
    return [_bundle("GENVCE", genvce, raw_dir, registries), _bundle("CREA", crea, raw_dir, registries)]


def largest_row_shapes(bundles: Sequence[Bundle]) -> dict[str, Any]:
    """The largest unit and observation property maps of the real bundles (the conservative shape)."""
    registries = load_registries()
    best_unit: dict[str, Any] | None = None
    best_obs: dict[str, Any] | None = None
    size = lambda props: len(json.dumps(props, default=str))
    for bundle in bundles:
        plan = loader._plan(bundle, registries)
        for rows in plan.units_by_label.values():
            for row in rows:
                if best_unit is None or size(row["props"]) > size(best_unit):
                    best_unit = row["props"]
        for row in plan.observations:
            if best_obs is None or size(row["props"]) > size(best_obs):
                best_obs = row["props"]
    assert best_unit is not None and best_obs is not None
    return {"unit": best_unit, "observation": best_obs, "unit_bytes": size(best_unit), "observation_bytes": size(best_obs)}


def _unit_rows(shape: dict[str, Any], tag: str, n: int) -> list[dict[str, Any]]:
    return [{"unitKey": f"{tag}-u{i:07d}", "props": {**shape["unit"], "unitKey": f"{tag}-u{i:07d}",
                                                      "mergeKey": f"{tag}-u{i:07d}"}} for i in range(n)]


def _obs_rows(shape: dict[str, Any], tag: str, n: int) -> list[dict[str, Any]]:
    return [{"obsKey": f"{tag}-o{i:07d}", "props": {**shape["observation"], "obsKey": f"{tag}-o{i:07d}"}}
            for i in range(n)]


# ═════════════════════════════════════════════════════════════════════════════
# micro: one transaction of B rows under a limit
# ═════════════════════════════════════════════════════════════════════════════

def _is_memory_error(exc: Neo4jError) -> bool:
    return any(token in (exc.code or "") or token in str(exc) for token in MEMORY_ERRORS)


async def _one_transaction(driver: Any, step: str, shape: dict[str, Any], size: int) -> dict[str, Any]:
    """Run ``size`` rows of ``step`` as ONE transaction on fresh keys; report pass or the memory error."""
    tag = f"{step.replace(':', '_')}-{size}"
    statements = dict(loader._RELATIONSHIPS)
    async with driver.session() as session:
        if step == "unit":
            statement, rows, kwargs = loader._unit_statement("VarietyTrial"), _unit_rows(shape, tag, size), {}
        elif step == "observation":
            statement, rows, kwargs = loader._OBSERVATION, _obs_rows(shape, tag, size), {}
        else:
            # the endpoints must exist: create them in small transactions first (not measured)
            await loader._run_step(session, "setup-units", loader._unit_statement("VarietyTrial"),
                                   _unit_rows(shape, tag, size), SETUP_BATCH)
            await loader._run_step(session, "setup-obs", loader._OBSERVATION, _obs_rows(shape, tag, size), SETUP_BATCH)
            statement = statements["obs_unit"]
            rows = [{"a": f"{tag}-o{i:07d}", "b": f"{tag}-u{i:07d}"} for i in range(size)]
            kwargs = {"all_must_match": True}
        started = time.monotonic()
        try:
            await loader._run_step(session, f"bench:{step}", statement, rows, size, **kwargs)
        except Neo4jError as exc:
            if _is_memory_error(exc):
                return {"ok": False, "error": exc.code, "seconds": round(time.monotonic() - started, 3)}
            raise
    return {"ok": True, "seconds": round(time.monotonic() - started, 3)}


async def _micro_at_limit(container: Neo4jContainer, shape: dict[str, Any], sizes: Sequence[int],
                          steps: Sequence[str]) -> dict[str, dict[int, dict[str, Any]]]:
    driver = _driver(container)
    try:
        await apply_migrations(driver)
        out: dict[str, dict[int, dict[str, Any]]] = {step: {} for step in steps}
        for step in steps:
            for size in sizes:
                out[step][size] = await _one_transaction(driver, step, shape, size)
        return out
    finally:
        await driver.close()


def micro(shape: dict[str, Any], limits: Sequence[int] = LIMITS_MIB, sizes: Sequence[int] = SIZES,
          steps: Sequence[str] = STEPS) -> dict[str, Any]:
    results: dict[int, Any] = {}
    for limit in limits:
        with neo4j_like_production(limit) as container:
            configured = asyncio.run(_read_limit(container))
            results[limit] = asyncio.run(_micro_at_limit(container, shape, sizes, steps))
            results[limit]["_configured"] = configured
        print(f"micro limit={limit}MiB configured={configured} done", flush=True)
    return summarize_micro(results, sizes, steps)


async def _read_limit(container: Neo4jContainer) -> str:
    driver = _driver(container)
    try:
        return await _scalar(driver, "CALL dbms.listConfig('db.memory.transaction.max') YIELD value RETURN value")
    finally:
        await driver.close()


def summarize_micro(results: dict[int, Any], sizes: Sequence[int], steps: Sequence[str]) -> dict[str, Any]:
    """Per step and size: the smallest limit that commits (None: none of the ladder), plus the raw grid."""
    limits = sorted(results)
    needed: dict[str, dict[int, int | None]] = {}
    for step in steps:
        needed[step] = {}
        for size in sizes:
            passing = [lim for lim in limits if results[lim][step][size]["ok"]]
            needed[step][size] = min(passing) if passing else None
    failures = sum(1 for lim in limits for step in steps for size in sizes if not results[lim][step][size]["ok"])
    return {"limits_mib": limits, "sizes": list(sizes), "steps": list(steps), "min_passing_limit_mib": needed,
            "failures_observed": failures,
            "grid": {str(lim): {step: {str(s): results[lim][step][s] for s in sizes} for step in steps} for lim in limits},
            "configured": {str(lim): results[lim].get("_configured") for lim in limits}}


def choose_batch_size(summary: dict[str, Any], budget_mib: int = BUDGET_MIB) -> int:
    """Largest size whose worst step commits under HALF the budget (the ladder is 2x wide, the budget adds 2x)."""
    ceiling = budget_mib / 2
    sizes = sorted(summary["sizes"])
    ok = []
    for size in sizes:
        worst = [summary["min_passing_limit_mib"][step][size] for step in summary["steps"]]
        if all(limit is not None and limit <= ceiling for limit in worst):
            ok.append(size)
    if not ok:
        raise SystemExit(f"no batch size of {sizes} fits under {ceiling} MiB: the loader cannot be made safe")
    return max(ok)


# ═════════════════════════════════════════════════════════════════════════════
# macro: the projected volume
# ═════════════════════════════════════════════════════════════════════════════

def projected_bundle(base: Bundle, copies: int = 4, obs_per_unit: int = 10) -> Bundle:
    """``base`` repeated ``copies`` times; every unit carries about ``obs_per_unit`` observations (extra ones are
    qualifier variants of an observation of a non-copied variable). Distribution of crops, sites and years is the real one."""
    obs_by_unit: dict[str, list[Any]] = {}
    for obs in base.observations:
        obs_by_unit.setdefault(obs.unit_key, []).append(obs)
    spare = next((o for o in base.observations if o.variable_id not in loader.DENORMALIZED_COPIES), base.observations[0])
    units, observations = [], []
    for copy in range(copies):
        for unit in base.units:
            new = unit if copy == 0 else unit.model_copy(update={
                "row_discriminator": f"{unit.row_discriminator or ''}~x{copy}"})
            key, new_key = identity.unit_key(unit), identity.unit_key(new)
            units.append(new)
            have = [o.model_copy(update={"unit_key": new_key}) for o in obs_by_unit.get(key, [])]
            extra = [o for o in have if o.variable_id not in loader.DENORMALIZED_COPIES] or [spare]
            observations.extend(have)
            for j in range(max(0, obs_per_unit - len(have))):
                template = extra[j % len(extra)]
                observations.append(template.model_copy(update={"unit_key": new_key, "qualifier": f"synthetic-{j}"}))
    return base.model_copy(update={"units": tuple(units), "observations": tuple(observations)})


async def _profile(driver: Any, tier: str, site_names: list[str], crops: list[str]) -> dict[str, Any]:
    params = {"site_names": site_names, "crops": crops, "irrigation_uri": None, "production_system": None, "top_n": 10,
              "excluded_sites": None, "site_weights": dict.fromkeys(site_names, 1.0), "now_year": 2026,
              "half_life": 8.0, "target_regime": None, "regime_penalty": 0.4}
    started = time.monotonic()
    try:
        async with driver.session() as session:
            result = await session.run("PROFILE " + _extrapolate_batch_query(ep.MODE_MAIN, tier), **params)
            rows = [r async for r in result]
            summary = await result.consume()
    except Neo4jError as exc:
        return {"ok": False, "error": exc.code}
    args = summary.profile["args"]
    return {"ok": True, "rows": len(rows), "seconds": round(time.monotonic() - started, 3),
            "peak_query_memory_bytes": args.get("GlobalMemory"), "db_hits": args.get("DbHits")}


def _exec(container: Neo4jContainer, command: list[str]) -> str:
    exit_code, output = container.get_wrapped_container().exec_run(command)
    return output.decode().strip() if exit_code == 0 else f"exec failed ({exit_code})"


async def _heap_peaks(driver: Any) -> dict[str, Any]:
    """Peak usage of every heap pool since the JVM started (the container is fresh, so: since the load began)."""
    async with driver.session() as session:
        result = await session.run("CALL dbms.queryJmx('java.lang:type=MemoryPool,name=*') YIELD name, attributes "
                                   "RETURN name, attributes")
        rows = [dict(r) async for r in result]
    peaks = {}
    for row in rows:
        usage = row["attributes"].get("PeakUsage", {}).get("value", {}).get("properties", {})
        name = next((part.split("=", 1)[1] for part in row["name"].split(",") if part.startswith("name=")
                     or ":name=" in part), row["name"])
        name = name.split("name=")[-1]
        if usage and name.startswith("G1"):  # the heap pools; metaspace and code cache are not heap
            peaks[name] = {"peak_used_mib": round(usage["used"] / 1048576, 1),
                           "max_mib": round(usage["max"] / 1048576, 1) if usage["max"] > 0 else None}
    return peaks


async def _rows(driver: Any, cypher: str) -> list[dict[str, Any]]:
    async with driver.session() as session:
        result = await session.run(cypher)
        return [dict(r) async for r in result]


def macro(bases: Sequence[Bundle], batch_size: int, tx_limit_mib: int, *, copies: int = 4, obs_per_unit: int = 10) -> dict[str, Any]:
    registries = load_registries()
    big = [projected_bundle(base, copies, obs_per_unit) for base in bases]
    with neo4j_like_production(tx_limit_mib) as container:
        return asyncio.run(_macro(container, big, registries, batch_size, tx_limit_mib, bases))


async def _macro(container: Neo4jContainer, big: Sequence[Bundle], registries: Any, batch_size: int, tx_limit_mib: int,
                 bases: Sequence[Bundle]) -> dict[str, Any]:
    driver = _driver(container)
    try:
        await apply_migrations(driver)
        started = time.monotonic()
        reports = [await loader.load(b, driver, batch_size=batch_size, registries=registries) for b in big]
        load_seconds = round(time.monotonic() - started, 1)
        for bundle in big:
            again = await loader.load(bundle, driver, batch_size=batch_size, registries=registries)
            assert again.nodes_created == again.relationships_created == 0, "a reload created data: not idempotent"
        try:
            async with driver.session() as session:
                await (await session.run("CALL db.checkpoint()")).consume()
        except Neo4jError:
            pass  # not available: the store size below is then the files as they are
        store = _exec(container, ["du", "-sb", "/data/databases/neo4j"])
        txlogs = _exec(container, ["du", "-sb", "/data/transactions/neo4j"])
        site_names = [r["name"] for r in await _rows(driver, "MATCH (t:TrialSite) RETURN t.name AS name ORDER BY name")]
        crops = [r["c"] for r in await _rows(driver, "MATCH (u:ObservationUnit) RETURN DISTINCT u.cropEppo AS c ORDER BY c")]
        queries = {tier: await _profile(driver, tier, site_names, crops)
                   for tier in (ep.EVIDENCE_TIER_FIELD, ep.EVIDENCE_TIER_REGIONAL)}
        units = sum(len(b.units) for b in big)
        observations = sum(len(b.observations) for b in big)
        return {
            "tx_limit_mib": tx_limit_mib, "batch_size": batch_size,
            "volume": {"units": units, "observations": observations,
                       "observations_per_unit": round(observations / units, 2),
                       "base_units": sum(len(b.units) for b in bases),
                       "base_observations": sum(len(b.observations) for b in bases)},
            "load": {"seconds": load_seconds, "batches": sum(r.batches for r in reports),
                     "nodes_created": sum(r.nodes_created for r in reports),
                     "relationships_created": sum(r.relationships_created for r in reports), "reload_created": 0},
            "store": {"databases_dir": store, "transactions_dir": txlogs},
            "heap_pool_peaks": await _heap_peaks(driver),
            "hot_queries": queries,
        }
    finally:
        await driver.close()


# ═════════════════════════════════════════════════════════════════════════════
# command line
# ═════════════════════════════════════════════════════════════════════════════

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("what", choices=("micro", "macro", "all"))
    parser.add_argument("--out", required=True, help="JSON result (keep it outside the repository)")
    parser.add_argument("--raw-dir", default=os.environ.get("NKZ_DATA_SOURCES_DIR", ""),
                        help="raw-data repository (default NKZ_DATA_SOURCES_DIR; without it the test fixtures are used)")
    parser.add_argument("--batch-size", type=int, default=0, help="macro only: skip the choice and use this size")
    parser.add_argument("--limits", default="", help=f"micro only: comma-separated ladder in MiB (default {LIMITS_MIB})")
    parser.add_argument("--sizes", default="", help=f"micro only: comma-separated batch sizes (default {SIZES})")
    args = parser.parse_args(argv)
    raw = Path(args.raw_dir) if args.raw_dir else None
    bundles = real_bundles(raw)
    result: dict[str, Any] = {"raw_data": "real" if raw else "fixtures", "image": IMAGE,
                              "server": {"heap_initial": HEAP_INITIAL, "heap_max": HEAP_MAX, "pagecache": PAGECACHE}}
    chosen = args.batch_size
    if args.what in ("micro", "all"):
        shape = largest_row_shapes(bundles)
        result["row_shape"] = {"unit_props_json_bytes": shape["unit_bytes"], "observation_props_json_bytes": shape["observation_bytes"]}
        ladder = tuple(int(x) for x in args.limits.split(",") if x) or LIMITS_MIB
        sizes = tuple(int(x) for x in args.sizes.split(",") if x) or SIZES
        result["micro"] = micro(shape, ladder, sizes)
        chosen = chosen or choose_batch_size(result["micro"])
        result["chosen_batch_size"] = chosen
        result["budget_mib"] = BUDGET_MIB
    if args.what in ("macro", "all"):
        if not chosen:
            raise SystemExit("macro needs --batch-size or a micro run")
        result["macro"] = macro(bundles, chosen, BUDGET_MIB)
    Path(args.out).write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n")
    print(json.dumps({"chosen_batch_size": result.get("chosen_batch_size")}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
