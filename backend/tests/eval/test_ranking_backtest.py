"""Forward-chaining backtest of the variety table (pure, synthetic rows, no Neo4j)."""
import random
from functools import partial

import pytest

from app.eval import ranking_backtest as rb
from app.planning.varieties import variety_stats


def _env(site, year, values, source="GENVCE"):
    return [{"variety": v, "site": site, "year": year, "source_id": source, "yield_kg_ha": y}
            for v, y in values.items()]


def _flat(names, value=5000):
    return {n: value for n in names}


# --- spearman ---------------------------------------------------------------

def test_spearman_identical_orders_is_one():
    assert rb.spearman([1, 2, 3, 4, 5], [10, 20, 30, 40, 50]) == pytest.approx(1.0)


def test_spearman_reversed_orders_is_minus_one():
    assert rb.spearman([1, 2, 3, 4, 5], [50, 40, 30, 20, 10]) == pytest.approx(-1.0)


def test_spearman_unrelated_orders_is_zero():
    assert rb.spearman([1, 2, 3, 4], [2, 4, 1, 3]) == pytest.approx(0.0)


def test_spearman_needs_two_points_and_spread():
    assert rb.spearman([1], [1]) is None
    assert rb.spearman([1, 1, 1], [1, 2, 3]) is None


# --- forward chaining -------------------------------------------------------

def test_split_forward_trains_only_on_earlier_years():
    rows = _env("A", 2022, _flat(["V1", "V2", "V3"])) + _env("A", 2023, _flat(["V1", "V2", "V3"])) \
        + _env("A", 2024, _flat(["V1", "V2", "V3"])) + _env("A", 2025, _flat(["V1", "V2", "V3"]))
    train, test = rb.split_forward(rows, 2024)
    assert {r["year"] for r in train} == {2022, 2023}
    assert {r["year"] for r in test} == {2024}


# --- environment ratios -----------------------------------------------------

def test_environment_ratios_divide_by_the_environment_mean_and_drop_thin_environments():
    rows = _env("A", 2024, {"V1": 6000, "V2": 5000, "V3": 4000}) + _env("B", 2024, {"V1": 6000, "V2": 5000})
    ratios = rb.environment_ratios(rows)
    assert set(ratios) == {("A", 2024, "GENVCE")}
    assert ratios[("A", 2024, "GENVCE")]["V1"] == pytest.approx(1.2)


# --- top-3 utility ----------------------------------------------------------

def test_top3_utility_is_the_mean_ratio_of_the_predicted_top3_in_each_test_environment():
    rows = _env("A", 2024, {"V1": 6000, "V2": 5000, "V3": 4000, "V4": 5000})
    env_ratios = rb.environment_ratios(rows)
    # mean of V1..V4 = 5000 → ratios 1.2, 1.0, 0.8, 1.0; top-3 predicted = V1, V2, V3
    assert rb.top3_utility_pct(["V1", "V2", "V3"], env_ratios) == pytest.approx(100.0)
    assert rb.top3_utility_pct(["V1", "V2", "V4"], env_ratios) == pytest.approx((1.2 + 1.0 + 1.0) / 3 * 100)


def test_top3_utility_skips_an_environment_with_fewer_than_two_of_the_three_present():
    rows = _env("A", 2024, {"V1": 6000, "X": 5000, "Y": 4000})
    env_ratios = rb.environment_ratios(rows)
    assert rb.top3_utility_pct(["V1", "V2", "V3"], env_ratios) is None


# --- baselines --------------------------------------------------------------

def test_last_year_baseline_uses_the_year_before_the_test_year():
    train = _env("A", 2022, {"OLD": 9000, "V2": 5000, "V3": 4000}) \
        + _env("A", 2023, {"NEW": 9000, "V2": 5000, "V3": 4000, "OLD": 1000})
    assert rb.last_year_top3(train, 2024)[0] == "NEW"


def test_last_year_baseline_falls_back_to_the_latest_year_with_data():
    train = _env("A", 2021, {"OLD": 9000, "V2": 5000, "V3": 4000})
    assert rb.last_year_top3(train, 2024)[0] == "OLD"


def test_random_baseline_is_about_one_hundred_on_a_large_fixture():
    rows = []
    rng = random.Random(1)
    for site in "ABCDEFGH":
        for year in range(2015, 2024):
            rows += _env(site, year, {f"V{i}": rng.uniform(2000, 8000) for i in range(12)})
    value = rb.random_top3_utility_pct(rb.environment_ratios(rows), seed=7, draws=2000)
    assert value == pytest.approx(100.0, abs=1.0)


# --- coverage ---------------------------------------------------------------

def _train_spread():
    rows = []
    for i, mean in enumerate(range(1000, 11000, 1000)):
        rows += _env(f"S{i}", 2015 + i % 5, {"V1": mean + 500, "V2": mean, "V3": mean - 500})
    return rows


def test_coverage_counts_test_environments_inside_the_train_p10_p90_range():
    inside = []
    for i in range(5):
        inside += _env(f"T{i}", 2024, {"V1": 5500, "V2": 5000, "V3": 4500})
    assert rb.coverage_pct(_train_spread(), inside) == pytest.approx(100.0)
    outside = []
    for i in range(5):
        outside += _env(f"T{i}", 2024, {"V1": 20500, "V2": 20000, "V3": 19500})
    assert rb.coverage_pct(_train_spread(), outside) == pytest.approx(0.0)


def test_coverage_needs_enough_test_environments():
    few = _env("T0", 2024, {"V1": 5500, "V2": 5000, "V3": 4500})
    assert rb.coverage_pct(_train_spread(), few) is None


# --- the cell ---------------------------------------------------------------

def _cell_rows(n_varieties, n_test_sites, years=range(2018, 2024)):
    names = [f"V{i}" for i in range(n_varieties)]
    rows = []
    for year in years:
        for site in "ABC":
            rows += _env(site, year, {n: 5000 + 100 * i for i, n in enumerate(names)})
    test = []
    for j in range(n_test_sites):
        test += _env(f"T{j}", 2024, {n: 5000 + 100 * i for i, n in enumerate(names)})
    return rows + test


def test_cell_is_skipped_with_too_few_common_varieties():
    cell = rb.evaluate_cell(_cell_rows(7, 6), test_year=2024, stats_fn=variety_stats)
    assert cell["status"] == "skipped" and cell["reason"] == "common_varieties"
    assert cell["n_common_varieties"] == 7


def test_cell_is_skipped_with_too_few_test_environments():
    cell = rb.evaluate_cell(_cell_rows(10, 4), test_year=2024, stats_fn=variety_stats)
    assert cell["status"] == "skipped" and cell["reason"] == "test_environments"
    assert cell["n_test_environments"] == 4


def test_cell_reports_metrics_and_baselines_when_the_guards_pass():
    cell = rb.evaluate_cell(_cell_rows(10, 6), test_year=2024, stats_fn=variety_stats,
                            flat_fn=variety_stats, seed=3)
    assert cell["status"] == "ok"
    assert cell["spearman"] == pytest.approx(1.0)  # the same order every year
    assert cell["top3_utility_pct"] > 100.0
    assert cell["baselines"]["flat"] == pytest.approx(cell["top3_utility_pct"])
    assert cell["baselines"]["last_year"] == pytest.approx(cell["top3_utility_pct"])
    assert cell["baselines"]["random"] == pytest.approx(100.0, abs=2.0)
    assert cell["coverage_pct"] == pytest.approx(100.0)


def test_cell_passes_the_test_year_to_a_year_aware_stats_fn():
    seen = []

    def stats_fn(rows, *, ref_year=None):
        seen.append(ref_year)
        return variety_stats(rows)

    rb.evaluate_cell(_cell_rows(10, 6), test_year=2024, stats_fn=partial(stats_fn, ref_year=2024))
    assert seen == [2024]


# --- aggregation ------------------------------------------------------------

def test_aggregate_by_crop_reports_median_positive_share_worst_and_skips():
    cells = [
        {"crop": "TRZAX", "status": "ok", "spearman": 0.5, "top3_utility_pct": 104.0, "coverage_pct": 80.0,
         "baselines": {"random": 100.0, "last_year": 101.0, "flat": 103.0}},
        {"crop": "TRZAX", "status": "ok", "spearman": -0.2, "top3_utility_pct": 99.0, "coverage_pct": 60.0,
         "baselines": {"random": 100.0, "last_year": 102.0, "flat": 99.0}},
        {"crop": "TRZAX", "status": "ok", "spearman": 0.3, "top3_utility_pct": 102.0, "coverage_pct": 100.0,
         "baselines": {"random": 100.0, "last_year": 100.0, "flat": 102.0}},
        {"crop": "TRZAX", "status": "skipped", "reason": "common_varieties"},
    ]
    agg = rb.aggregate_by_crop(cells)["TRZAX"]
    assert agg["n_cells"] == 3 and agg["skipped"] == {"common_varieties": 1}
    assert agg["spearman_median"] == pytest.approx(0.3)
    assert agg["spearman_positive_share"] == pytest.approx(2 / 3)
    assert agg["spearman_worst"] == pytest.approx(-0.2)
    assert agg["top3_utility_median_pct"] == pytest.approx(102.0)
    assert agg["baselines_median"]["last_year"] == pytest.approx(101.0)
    assert agg["coverage_mean_pct"] == pytest.approx(80.0)
    assert agg["measurable"] is False  # fewer than MIN_FOLDS valid cells
