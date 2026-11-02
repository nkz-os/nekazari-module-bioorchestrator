"""CLI and pure plumbing of scripts/check_ranking_backtest.py (no Neo4j)."""

from app.planning.varieties import variety_stats
from scripts.check_ranking_backtest import (
    DEFAULT_CROPS,
    EXIT_FAIL,
    EXIT_NOT_MEASURABLE,
    EXIT_OK,
    cells_for_group,
    exit_code,
    parse_args,
    stats_functions,
)


def test_defaults_cover_the_nine_crops_and_report_mode():
    args = parse_args([])
    assert tuple(args.crops) == DEFAULT_CROPS and args.regimes == ["rainfed", "irrigated"]
    assert (args.from_year, args.to_year, args.gate) == (2016, 2025, False)
    assert args.tiers == ["field", "regional"]


def test_crops_and_years_can_be_narrowed():
    args = parse_args(["--crops", "TRZAX,HORVX", "--from", "2020", "--to", "2022", "--gate", "--tiers", "regional"])
    assert args.crops == ["TRZAX", "HORVX"] and (args.from_year, args.to_year, args.gate) == (2020, 2022, True)
    assert args.tiers == ["regional"]


def test_stats_functions_pass_the_test_year_only_when_the_table_accepts_it():
    def year_aware(rows, *, ref_year=None):
        return [{"ref_year": ref_year}], 0

    main_fn, flat_fn = stats_functions(year_aware, test_year=2021)
    assert main_fn([])[0] == [{"ref_year": 2021}] and flat_fn([])[0] == [{"ref_year": None}]
    main_plain, flat_plain = stats_functions(variety_stats, test_year=2021)
    assert main_plain is variety_stats and flat_plain is variety_stats


def _env(site, year, values):
    return [{"variety": v, "site": site, "year": year, "source_id": "S", "yield_kg_ha": y} for v, y in values.items()]


def test_cells_for_group_labels_cells_and_respects_the_window():
    rows = []
    for year in range(2010, 2026):
        for site in "ABCDEF":
            rows += _env(site, year, {f"V{i}": 5000 + 100 * i for i in range(10)})
    cells = cells_for_group(rows, crop="TRZAX", zone="Csa", regime="rainfed", tier="regional", first=2016, last=2017,
                            table_fn=variety_stats, seed=1, truncated=True)
    assert [c["test_year"] for c in cells] == [2016, 2017]
    assert all((c["crop"], c["zone"], c["regime"], c["tier"], c["truncated"])
               == ("TRZAX", "Csa", "rainfed", "regional", True) for c in cells)
    assert all(c["status"] == "ok" for c in cells)


def test_exit_code_follows_the_verdict_only_with_gate():
    assert exit_code({"TRZAX": "fail"}, gate=False) == EXIT_OK
    assert exit_code({"TRZAX": "pass", "HORVX": "not_measurable"}, gate=True) == EXIT_NOT_MEASURABLE
    assert exit_code({"TRZAX": "fail", "HORVX": "not_measurable"}, gate=True) == EXIT_FAIL
    assert exit_code({"TRZAX": "pass"}, gate=True) == EXIT_OK


def test_guards_can_be_relaxed_from_the_command_line():
    args = parse_args(["--min-train-years", "2", "--min-test-envs", "1", "--country", "ES"])
    assert (args.min_train_years, args.min_test_envs, args.country) == (2, 1, "ES")
    assert (parse_args([]).min_train_years, parse_args([]).min_test_envs, parse_args([]).country) == (5, 5, "ES")


def test_regional_groups_are_the_aggregate_pseudo_sites_of_the_country():
    from scripts.check_ranking_backtest import regional_groups
    sites = [{"name": "General", "site_kind": "aggregate", "country": "ES"},
             {"name": "Zonas frías", "site_kind": "aggregate", "country": "ES"},
             {"name": "Registry FR", "site_kind": "aggregate", "country": "FR"},
             {"name": "Pamplona", "site_kind": "field", "country": "ES"}]
    assert regional_groups(sites, country="ES") == ["General", "Zonas frías"]


def test_cells_for_group_relaxes_the_environment_guard():
    rows = []
    for year in range(2010, 2026):
        rows += _env("General", year, {f"V{i}": 5000 + 100 * i for i in range(10)})
    strict = cells_for_group(rows, crop="TRZAX", zone="General", regime="rainfed", tier="regional", first=2016,
                             last=2017, table_fn=variety_stats, seed=1, truncated=False)
    relaxed = cells_for_group(rows, crop="TRZAX", zone="General", regime="rainfed", tier="regional", first=2016,
                              last=2017, table_fn=variety_stats, seed=1, truncated=False, min_test_envs=1)
    assert [c["status"] for c in strict] == ["skipped", "skipped"]
    assert [c["status"] for c in relaxed] == ["ok", "ok"] and relaxed[0]["spearman"] == 1.0
