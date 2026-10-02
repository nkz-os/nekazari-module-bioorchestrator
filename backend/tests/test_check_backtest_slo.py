"""check_backtest_slo script: arg parsing and gate values (no Neo4j)."""
from __future__ import annotations

import importlib.util
from pathlib import Path

_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check_backtest_slo.py"


def _load():
    spec = importlib.util.spec_from_file_location("check_backtest_slo", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_parse_args_defaults_and_choices():
    m = _load()
    assert m.parse_args([]).strategy == "hybrid"
    assert m.parse_args(["--strategy", "koppen", "--gate-only"]).strategy == "koppen"


def test_gate_rebaselined():
    assert _load().GATE == {"top3_overlap": 0.200, "median_abs_error_kg_ha": 850.0, "coverage": 0.89}


def test_script_uses_app_settings_not_base_ingester():
    src = _PATH.read_text()
    assert "base_ingester" not in src
    assert "settings.neo4j_uri" in src


async def test_main_end_to_end_with_mocks(monkeypatch, capsys):
    from unittest.mock import AsyncMock, MagicMock

    from app.core.config import settings
    from app.eval.backtest import Backtester

    m = _load()
    driver = MagicMock()
    driver.close = AsyncMock()
    factory = MagicMock(return_value=driver)
    monkeypatch.setattr(m.AsyncGraphDatabase, "driver", factory)
    good = {"top3_overlap": 0.3, "median_abs_error_kg_ha": 700.0, "coverage": 0.95}
    run = AsyncMock(return_value={"overall": good})
    monkeypatch.setattr(Backtester, "run", run)
    monkeypatch.setattr("sys.argv", ["check_backtest_slo.py", "--strategy", "v2"])

    rc = await m.main()

    assert rc == 0
    factory.assert_called_once_with(
        settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password),
    )
    run.assert_awaited_once()
    assert run.await_args.kwargs == {"strategy": "v2"}
    driver.close.assert_awaited_once()
    assert "strategy: v2" in capsys.readouterr().out
