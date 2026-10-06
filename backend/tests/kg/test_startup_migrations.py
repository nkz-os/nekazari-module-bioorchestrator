"""App startup wiring of the strict migration runner: failure must fail readiness, loudly."""
from __future__ import annotations

import logging
from unittest.mock import AsyncMock

import pytest

from app.kg.migrations import AppliedMigration, MigrationError, MigrationReport


@pytest.fixture
def main_mod(client):
    from app import main

    main._migration_failure = None
    main._ikerketa_available = True
    yield main
    main._migration_failure = None
    main._ikerketa_available = False


def _report() -> MigrationReport:
    return MigrationReport((AppliedMigration("001_x.cypher", "ab", "schema-only", 3, 0, 2, True),))


async def test_startup_runs_schema_only_and_readiness_stays_up(main_mod, client, monkeypatch):
    runner = AsyncMock(return_value=_report())
    monkeypatch.setattr(main_mod, "apply_migrations", runner)

    await main_mod._run_startup_migrations(object())

    runner.assert_awaited_once()
    assert runner.await_args.kwargs == {"include_data": False}
    assert main_mod._migration_failure is None
    assert client.get("/readyz").status_code == 200


async def test_failed_migration_fails_readiness_logs_critical_and_does_not_raise(
    main_mod, client, monkeypatch, log_records
):
    err = MigrationError(
        "002_x.cypher statement 3 failed", file="002_x.cypher", statement_no=3,
        statement="CREATE CONSTRAINT ...", code="Neo.DatabaseError.Schema.ConstraintCreationFailed",
    )
    monkeypatch.setattr(main_mod, "apply_migrations", AsyncMock(side_effect=err))

    records = log_records("app.main", logging.CRITICAL)
    await main_mod._run_startup_migrations(object())  # must not raise: the app still starts

    assert any(r.levelno == logging.CRITICAL and "002_x.cypher" in r.getMessage() for r in records)
    resp = client.get("/readyz")
    assert resp.status_code == 503
    assert resp.json() == {
        "status": "not_ready", "reason": "schema migration failed", "file": "002_x.cypher",
        "statement_no": 3, "code": "Neo.DatabaseError.Schema.ConstraintCreationFailed",
    }
    assert client.get("/healthz").status_code == 200  # liveness unaffected: no restart loop


async def test_unexpected_exception_also_fails_readiness(main_mod, client, monkeypatch):
    monkeypatch.setattr(main_mod, "apply_migrations", AsyncMock(side_effect=OSError("boom")))

    await main_mod._run_startup_migrations(object())

    resp = client.get("/readyz")
    assert resp.status_code == 503 and resp.json()["code"] == "OSError"


async def test_success_after_failure_clears_the_flag(main_mod, client, monkeypatch):
    main_mod._migration_failure = {"file": "x", "statement_no": 1, "code": "c"}
    monkeypatch.setattr(main_mod, "apply_migrations", AsyncMock(return_value=_report()))

    await main_mod._run_startup_migrations(object())

    assert main_mod._migration_failure is None
    assert client.get("/readyz").status_code == 200
