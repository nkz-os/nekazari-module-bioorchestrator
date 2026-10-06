"""App startup wiring of the strict migration runner.

A deterministic schema failure fails readiness (and recovers by itself once a retry succeeds); a
transient or connectivity error never does. No real Neo4j: the runner is mocked.
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError

from app.kg.migrations import AppliedMigration, MigrationError, MigrationReport

SCHEMA_CODE = "Neo.DatabaseError.Schema.ConstraintCreationFailed"


@pytest.fixture
def main_mod(client, monkeypatch):  # `client` imports app.main with ikerketa stubbed
    from app import main

    main._migration_failure = None
    main._migration_task = None
    main._ikerketa_available = True
    # No real waiting between attempts.
    monkeypatch.setattr(main, "_MIGRATION_TRANSIENT_BACKOFF_START_S", 0.0)
    monkeypatch.setattr(main, "_MIGRATION_TRANSIENT_BACKOFF_CAP_S", 0.0)
    monkeypatch.setattr(main, "_MIGRATION_SCHEMA_RETRY_S", 0.0)
    yield main
    if main._migration_task is not None:
        main._migration_task.cancel()
    main._migration_failure = None
    main._migration_task = None
    main._ikerketa_available = False


def _report() -> MigrationReport:
    return MigrationReport((AppliedMigration("001_x.cypher", "ab", "schema-only", 3, 0, 2, True),))


def _schema_error(file: str = "002_x.cypher") -> MigrationError:
    return MigrationError(f"{file} statement 3 failed", file=file, statement_no=3,
                          statement="CREATE CONSTRAINT ...", code=SCHEMA_CODE)


async def _wait_for_retry_task(main) -> None:
    await asyncio.wait_for(main._migration_task, timeout=5)


async def test_success_runs_schema_only_with_no_retry_task(main_mod, client, monkeypatch):
    runner = AsyncMock(return_value=_report())
    monkeypatch.setattr(main_mod, "apply_migrations", runner)

    await main_mod._run_startup_migrations(object())

    runner.assert_awaited_once()
    assert runner.await_args.kwargs == {"include_data": False}
    assert main_mod._migration_failure is None and main_mod._migration_task is None
    assert client.get("/readyz").status_code == 200


async def test_schema_error_latches_503_then_clears_after_a_successful_retry(
    main_mod, client, monkeypatch, log_records
):
    runner = AsyncMock(side_effect=[_schema_error(), _schema_error(), _report()])
    monkeypatch.setattr(main_mod, "apply_migrations", runner)
    records = log_records("app.main", logging.WARNING)

    await main_mod._run_startup_migrations(object())  # must not raise: the app still starts

    # Latched right after the first attempt, i.e. before the pod can report Ready.
    assert any(r.levelno == logging.CRITICAL and "002_x.cypher" in r.getMessage() for r in records)
    resp = client.get("/readyz")
    assert resp.status_code == 503
    assert resp.json() == {
        "status": "not_ready", "reason": "schema migration failed", "file": "002_x.cypher",
        "statement_no": 3, "code": SCHEMA_CODE,
    }
    assert client.get("/healthz").status_code == 200  # liveness unaffected

    await _wait_for_retry_task(main_mod)  # keeps retrying (here: second failure, then success)

    assert runner.await_count == 3
    assert main_mod._migration_failure is None
    assert client.get("/readyz").status_code == 200


@pytest.mark.parametrize(
    "transient",
    [
        ServiceUnavailable("neo4j restarting"),
        SessionExpired("session gone"),
        TransientError._hydrate_neo4j(code="Neo.TransientError.General.DatabaseUnavailable", message="down"),
        MigrationError("x", file="001_x.cypher", statement_no=1, code="Neo.TransientError.Transaction.DeadlockDetected"),
        ConnectionRefusedError("refused"),
    ],
    ids=["service_unavailable", "session_expired", "transient_error", "wrapped_transient", "connection_refused"],
)
async def test_transient_error_does_not_latch_and_a_later_attempt_succeeds(
    main_mod, client, monkeypatch, log_records, transient
):
    runner = AsyncMock(side_effect=[transient, transient, _report()])
    monkeypatch.setattr(main_mod, "apply_migrations", runner)
    records = log_records("app.main", logging.WARNING)

    await main_mod._run_startup_migrations(object())

    assert main_mod._migration_failure is None
    assert client.get("/readyz").status_code == 200  # same as Neo4j being down at startup today
    assert any(r.levelno == logging.WARNING and "transiently" in r.getMessage() for r in records)
    assert not any(r.levelno >= logging.CRITICAL for r in records)

    await _wait_for_retry_task(main_mod)

    assert runner.await_count == 3
    assert main_mod._migration_failure is None
    assert client.get("/readyz").status_code == 200


async def test_transient_error_during_a_latch_keeps_the_latch_until_success(main_mod, client, monkeypatch):
    runner = AsyncMock(side_effect=[_schema_error(), ServiceUnavailable("blip"), _report()])
    monkeypatch.setattr(main_mod, "apply_migrations", runner)
    seen: list[int] = []
    real_attempt = main_mod._attempt_startup_migrations

    async def spy(driver):
        ok = await real_attempt(driver)
        seen.append(client.get("/readyz").status_code)
        return ok

    monkeypatch.setattr(main_mod, "_attempt_startup_migrations", spy)

    await main_mod._run_startup_migrations(object())
    await _wait_for_retry_task(main_mod)

    assert seen == [503, 503, 200]  # attempt 1 latches, the transient attempt 2 leaves it, 3 clears


async def test_unexpected_exception_is_retried_not_latched_and_logged_with_traceback(
    main_mod, client, monkeypatch, log_records
):
    runner = AsyncMock(side_effect=[ValueError("bug"), _report()])
    monkeypatch.setattr(main_mod, "apply_migrations", runner)
    records = log_records("app.main", logging.WARNING)

    await main_mod._run_startup_migrations(object())

    assert main_mod._migration_failure is None
    assert any(r.exc_info is not None and "ValueError" in r.getMessage() for r in records)
    await _wait_for_retry_task(main_mod)
    assert runner.await_count == 2


async def test_malformed_file_error_without_server_code_is_deterministic(main_mod, client, monkeypatch):
    err = MigrationError("bad", file="011_x.cypher")  # parse error / bad name: no server code
    monkeypatch.setattr(main_mod, "apply_migrations", AsyncMock(side_effect=[err, _report()]))

    await main_mod._run_startup_migrations(object())

    resp = client.get("/readyz")
    assert resp.status_code == 503 and resp.json()["code"] == "MigrationError"
    await _wait_for_retry_task(main_mod)
    assert client.get("/readyz").status_code == 200


async def test_retry_task_is_strongly_referenced_and_cancelled_on_shutdown(main_mod, monkeypatch):
    monkeypatch.setattr(main_mod, "_MIGRATION_TRANSIENT_BACKOFF_START_S", 3600.0)
    monkeypatch.setattr(main_mod, "apply_migrations", AsyncMock(side_effect=ServiceUnavailable("down")))

    await main_mod._run_startup_migrations(object())
    task = main_mod._migration_task

    assert task in main_mod._BG_TASKS and not task.done()
    await main_mod._stop_startup_migration_retry()
    assert task.cancelled() and main_mod._migration_task is None
    await asyncio.sleep(0)
    assert task not in main_mod._BG_TASKS


async def test_real_unreachable_driver_is_classified_transient(main_mod, client):
    """A driver pointing nowhere raises a driver-level error out of the runner (not a MigrationError)."""
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver("bolt://127.0.0.1:1", auth=("neo4j", "x"), connection_timeout=1)
    try:
        await main_mod._attempt_startup_migrations(driver)
        assert main_mod._migration_failure is None
        assert client.get("/readyz").status_code == 200
    finally:
        await driver.close()
