"""Sizing benchmark (plan task 9): the pure parts, and the negative control on a real server.

The full benchmark (several containers, minutes) is ``scripts/kg_sizing_benchmark.py``. These tests pin what
its conclusion rests on: the batch size is picked by a stated rule, the projected volume is what it says, and
a transaction that is too big for the memory limit really fails (a benchmark that never fails proves nothing).
"""
from __future__ import annotations

import asyncio
import shutil
from pathlib import Path

import pytest

from app.kg import identity, loader
from app.kg.migrations import apply_migrations
from scripts import kg_sizing_benchmark as bench

FIXTURES = Path(__file__).parent / "fixtures"
needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")


def _summary(needed: dict[str, dict[int, int | None]]) -> dict:
    sizes = sorted(next(iter(needed.values())))
    return {"sizes": sizes, "steps": list(needed), "min_passing_limit_mib": needed}


def test_the_chosen_size_is_the_largest_that_fits_under_half_the_budget():
    needed = {"unit": {500: 16, 1000: 16, 2500: 16, 5000: 32, 10000: 64},
              "observation": {500: 16, 1000: 16, 2500: 16, 5000: 16, 10000: 32}}
    assert bench.choose_batch_size(_summary(needed), budget_mib=32) == 2500  # 5000 needs 32 > 16
    assert bench.choose_batch_size(_summary(needed), budget_mib=64) == 5000  # 10000 needs 64 > 32


def test_a_size_that_fits_no_limit_of_the_ladder_is_never_chosen():
    needed = {"unit": {500: 16, 1000: None}, "observation": {500: 16, 1000: 16}}
    assert bench.choose_batch_size(_summary(needed), budget_mib=32) == 500


def test_no_safe_size_is_an_error_not_a_default():
    needed = {"unit": {500: 64, 1000: 128}}
    with pytest.raises(SystemExit, match="cannot be made safe"):
        bench.choose_batch_size(_summary(needed), budget_mib=32)


def test_the_summary_reports_the_smallest_passing_limit_and_the_failures():
    results = {
        16: {"unit": {500: {"ok": True}, 1000: {"ok": False}}},
        32: {"unit": {500: {"ok": True}, 1000: {"ok": True}}},
    }
    out = bench.summarize_micro(results, [500, 1000], ["unit"])
    assert out["min_passing_limit_mib"] == {"unit": {500: 16, 1000: 32}}
    assert out["failures_observed"] == 1


def test_the_default_batch_size_is_the_measured_one():
    # the figure the loader ships is the output of choose_batch_size on the recorded benchmark
    assert loader.DEFAULT_BATCH_SIZE == bench.RECORDED_DEFAULT_BATCH_SIZE


def test_the_projected_volume_repeats_the_units_and_fills_up_the_observations():
    from unittest import mock

    from app.kg.adapters import crea
    from app.kg.contracts import load_contract, run_contract
    from app.kg.registries import load_registries

    registries = load_registries()
    contract = load_contract(Path(bench.BACKEND) / "data" / "sources" / "CREA.yaml")
    with mock.patch("app.kg.contracts._check_expected"):
        base = run_contract(contract, registries, crea.load(FIXTURES / "crea").rows)
    big = bench.projected_bundle(base, copies=3, obs_per_unit=10)
    assert len(big.units) == 3 * len(base.units)
    assert len({identity.unit_key(u) for u in big.units}) == len(big.units)
    assert len(big.observations) >= 10 * len(big.units) - 0  # every unit reaches ten
    per_unit: dict[str, int] = {}
    for obs in big.observations:
        per_unit[obs.unit_key] = per_unit.get(obs.unit_key, 0) + 1
    assert min(per_unit.values()) >= 10
    assert len({identity.obs_key(o) for o in big.observations}) == len(big.observations)
    assert loader._plan(big, registries)  # the projection is a closed bundle the loader accepts


@needs_docker
def test_negative_control_a_transaction_over_the_limit_fails_and_one_under_it_commits():
    shape = bench.largest_row_shapes(bench.real_bundles(None))
    with bench.neo4j_like_production(16) as container:
        driver = bench._driver(container)
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(apply_migrations(driver))
            assert loop.run_until_complete(bench._read_limit(container)).lower().startswith("16")
            ok = loop.run_until_complete(bench._one_transaction(driver, "unit", shape, bench.RECORDED_DEFAULT_BATCH_SIZE))
            assert ok["ok"], ok
            # the control: this many rows in one transaction must not fit in 16 MiB
            too_big = loop.run_until_complete(bench._one_transaction(driver, "unit", shape, bench.NEGATIVE_CONTROL_SIZE))
            assert not too_big["ok"] and any(t in too_big["error"] for t in ("MemoryLimit", "MemoryPool")), too_big
            # and a failed transaction leaves nothing behind
            async def count() -> int:
                async with driver.session() as s:
                    return (await (await s.run(
                        "MATCH (o:ObservationUnit) WHERE o.unitKey STARTS WITH $p RETURN count(o) AS c",
                        p=f"unit-{bench.NEGATIVE_CONTROL_SIZE}")).single())["c"]

            assert loop.run_until_complete(count()) == 0
        finally:
            loop.run_until_complete(driver.close())
            loop.close()

