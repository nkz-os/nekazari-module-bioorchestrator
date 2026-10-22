"""Seed script write path (driver mocked: nothing here touches a database)."""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts import seed_regional_yields as seed

DATA = Path(__file__).resolve().parents[1] / "data"


def _run(*extra):
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    argv = ["seed_regional_yields", "--data-dir", str(DATA), *extra]
    with patch.object(sys, "argv", argv), \
            patch.object(seed.GraphDatabase, "driver", return_value=driver) as make_driver:
        seed.main()
    return make_driver, session


def test_index_statement_covers_the_lookup_used_by_the_dao():
    assert seed.INDEX == (
        "CREATE INDEX regional_yield_stat_eppo_nuts IF NOT EXISTS "
        "FOR (n:RegionalYieldStat) ON (n.eppo, n.nuts)"
    )


def test_execute_creates_constraint_then_index_before_merging():
    _, session = _run("--execute")
    statements = [c.args[0] for c in session.run.call_args_list]
    assert statements[:2] == [seed.CONSTRAINT, seed.INDEX]
    assert statements[2:] and all(s == seed.MERGE for s in statements[2:])


def test_dry_run_builds_no_driver():
    make_driver, session = _run()
    make_driver.assert_not_called()
    session.run.assert_not_called()
