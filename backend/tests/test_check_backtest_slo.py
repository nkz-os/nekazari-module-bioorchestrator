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
    assert m.parse_args([]).strategy == "hybrid" and m.parse_args([]).min_folds == m.MIN_FOLDS
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
    good = {"top3_overlap": 0.3, "median_abs_error_kg_ha": 700.0, "coverage": 0.95,
            "folds": 100, "error_pairs": 300}
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


_GOOD = {"top3_overlap": 0.3, "median_abs_error_kg_ha": 700.0, "coverage": 0.95, "folds": 100, "error_pairs": 300}


def test_check_pass_and_fail_statuses(capsys):
    m = _load()
    assert m._check(_GOOD, m.GATE, "gate") == "pass"
    assert m._check({**_GOOD, "median_abs_error_kg_ha": 900.0}, m.GATE, "gate") == "fail"
    assert "FAIL [gate] MAE 900.0 > 850.0" in capsys.readouterr().out


def test_null_mae_is_not_measurable_not_a_crash(capsys):
    m = _load()
    nulls = {**_GOOD, "median_abs_error_kg_ha": None, "error_pairs": 0}
    assert m._check(nulls, m.GATE, "gate") == "not_measurable"
    out = capsys.readouterr().out
    assert "NOT MEASURABLE [gate] MAE" in out and "Traceback" not in out


def test_null_coverage_and_missing_keys_are_not_measurable():
    m = _load()
    assert m._check({**_GOOD, "coverage": None}, m.GATE, "gate") == "not_measurable"
    assert m._check({"folds": 100}, m.GATE, "gate") == "not_measurable"
    assert m._check({}, m.GATE, "gate") == "not_measurable"


def test_the_t12_report_is_not_measurable():
    """One fold, nothing covered: the report prints coverage 0.0 and overlap 0.0 and a null MAE."""
    m = _load()
    t12 = {"median_abs_error_kg_ha": None, "top3_overlap": 0.0, "coverage": 0.0, "folds": 1, "error_pairs": 0}
    assert m._check(t12, m.GATE, "gate") == "not_measurable"
    assert m._exit_code("not_measurable") == m.EXIT_NOT_MEASURABLE == 2


def test_few_folds_are_not_measurable_even_with_values():
    m = _load()
    assert m._check({**_GOOD, "folds": 5}, m.GATE, "gate") == "not_measurable"
    assert m._check({**_GOOD, "folds": 5}, m.GATE, "gate", min_folds=1) == "pass"


def test_a_measured_failure_wins_over_an_unmeasurable_metric():
    m = _load()
    mixed = {**_GOOD, "median_abs_error_kg_ha": None, "error_pairs": 0, "coverage": 0.5}
    assert m._check(mixed, m.GATE, "gate") == "fail"
    assert m._exit_code("fail", "not_measurable") == m.EXIT_FAIL == 1
    assert m._exit_code("pass", "pass") == m.EXIT_OK == 0


async def test_main_exits_2_on_unmeasurable_report(monkeypatch, capsys):
    from unittest.mock import AsyncMock, MagicMock

    from app.eval.backtest import Backtester

    m = _load()
    driver = MagicMock()
    driver.close = AsyncMock()
    monkeypatch.setattr(m.AsyncGraphDatabase, "driver", MagicMock(return_value=driver))
    overall = {"median_abs_error_kg_ha": None, "top3_overlap": 0.0, "coverage": 0.0, "folds": 1, "error_pairs": 0}
    monkeypatch.setattr(Backtester, "run", AsyncMock(return_value={"overall": overall}))
    monkeypatch.setattr("sys.argv", ["check_backtest_slo.py"])

    assert await m.main() == 2
    assert "NOT MEASURABLE [gate]" in capsys.readouterr().out
