"""The ops script is a thin wrapper over the strict runner."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.kg.migrations import AppliedMigration, MigrationError, MigrationReport
from scripts import apply_cypher_migrations as script


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("NEO4J_PASSWORD", "x")
    driver = MagicMock()
    driver.__aenter__ = AsyncMock(return_value=driver)
    driver.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(script.AsyncGraphDatabase, "driver", MagicMock(return_value=driver))


async def test_runs_data_statements_and_exits_zero(monkeypatch, capsys):
    runner = AsyncMock(return_value=MigrationReport((AppliedMigration("001_x.cypher", "ab", "full", 1, 0, 0, True),)))
    monkeypatch.setattr(script, "apply_migrations", runner)

    assert await script.main() == 0

    assert runner.await_args.kwargs == {"include_data": True}
    assert "done: 1 files" in capsys.readouterr().out


async def test_failure_exits_one_and_names_the_statement(monkeypatch, capsys):
    err = MigrationError("003_y.cypher statement 2 failed: Neo.X", file="003_y.cypher", statement_no=2)
    monkeypatch.setattr(script, "apply_migrations", AsyncMock(side_effect=err))

    assert await script.main() == 1

    assert "003_y.cypher statement 2 failed" in capsys.readouterr().err
