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


# --- test years, gate verdict, report ---------------------------------------

def test_test_years_need_enough_earlier_years_with_data():
    rows = []
    for year in (2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017):
        rows += _env("A", year, _flat(["V1", "V2", "V3"]))
    # 2015: five earlier years (2010-2014) → in; 2014: four earlier → out; 2018: no rows → out
    assert rb.test_years_for(rows, first=2014, last=2018) == [2015, 2016, 2017]


def _agg(**over):
    base = {"n_cells": 40, "skipped": {}, "measurable": True, "spearman_median": 0.4,
            "spearman_positive_share": 0.8, "spearman_worst": -0.1, "top3_utility_median_pct": 104.0,
            "baselines_median": {"random": 100.0, "last_year": 101.0, "flat": 103.0},
            "delta_median": {"random": 4.0, "last_year": 3.0, "flat": 1.0}, "delta_n": {"random": 40, "last_year": 40, "flat": 40},
            "coverage_mean_pct": 80.0}
    return {**base, **over}


def test_gate_verdict_passes_fails_and_declares_not_measurable():
    summary = {"TRZAX": _agg(), "HORVX": _agg(spearman_median=0.1), "TRZDU": _agg(n_cells=3, measurable=False),
               "ZEAMX": _agg(top3_utility_median_pct=100.5), "BRSNN": _agg(coverage_mean_pct=95.0)}
    verdict = rb.gate_verdict(summary)
    assert verdict == {"TRZAX": "pass", "HORVX": "fail", "TRZDU": "not_measurable",
                       "ZEAMX": "fail", "BRSNN": "fail"}


def test_gate_verdict_requires_beating_random_and_last_year_in_the_paired_cells():
    verdict = rb.gate_verdict({"TRZAX": _agg(delta_median={"random": 4.0, "last_year": -1.0, "flat": 1.0})})
    assert verdict["TRZAX"] == "fail"


def test_render_markdown_lists_every_cell_not_only_the_medians():
    cells = [
        {"crop": "TRZAX", "zone": "Csa", "regime": "rainfed", "test_year": 2020, "status": "ok", "spearman": 0.5,
         "n_common_varieties": 9, "n_test_environments": 6, "top3_utility_pct": 104.0, "coverage_pct": 80.0,
         "baselines": {"random": 100.0, "last_year": 101.0, "flat": 103.0}},
        {"crop": "TRZAX", "zone": "BSk", "regime": "rainfed", "test_year": 2021, "status": "skipped",
         "reason": "common_varieties", "n_common_varieties": 4, "n_test_environments": 7},
    ]
    report = {"meta": {"base": "x"}, "cells": cells, "summary": rb.aggregate_by_crop(cells),
              "verdict": rb.gate_verdict(rb.aggregate_by_crop(cells))}
    md = rb.render_markdown(report)
    assert "| TRZAX | Csa | rainfed | 2020 | ok |" in md
    assert "| TRZAX | BSk | rainfed | 2021 | skipped |" in md and "common_varieties" in md
    assert "not_measurable" in md


def test_cell_environment_guard_is_a_parameter():
    cell = rb.evaluate_cell(_cell_rows(10, 1), test_year=2024, stats_fn=variety_stats, min_test_environments=1)
    assert cell["status"] == "ok" and cell["n_test_environments"] == 1 and cell["coverage_pct"] is None


def test_cell_reports_how_many_predicted_top3_are_still_trialled_in_the_test_year():
    names = [f"V{i}" for i in range(10)]
    rows = []
    for year in range(2018, 2024):
        for site in "ABC":
            rows += _env(site, year, {n: 5000 + 100 * i for i, n in enumerate(names)})
    # test year: the two best historical varieties (V9, V8) are no longer trialled
    kept = names[:8] + ["N1", "N2"]
    for j in range(6):
        rows += _env(f"T{j}", 2024, {n: 5000 + 100 * i for i, n in enumerate(kept)})
    cell = rb.evaluate_cell(rows, test_year=2024, stats_fn=variety_stats)
    assert cell["status"] == "ok" and cell["top3_present_share"] == pytest.approx(1 / 3)
    assert cell["top3_utility_pct"] is None


def test_aggregate_reports_the_mean_top3_present_share():
    cells = [{"crop": "X", "status": "ok", "spearman": 0.1, "top3_utility_pct": None, "coverage_pct": None,
              "top3_present_share": 1 / 3, "baselines": {"random": 100.0, "last_year": 101.0, "flat": None}},
             {"crop": "X", "status": "ok", "spearman": 0.2, "top3_utility_pct": 103.0, "coverage_pct": None,
              "top3_present_share": 1.0, "baselines": {"random": 100.0, "last_year": 101.0, "flat": None}}]
    assert rb.aggregate_by_crop(cells)["X"]["top3_present_share_mean"] == pytest.approx(2 / 3)


# --- paired comparison against the rivals (review fix) ------------------------------------------

def test_cell_pairs_the_model_against_each_rival_only_where_both_are_scored():
    cell = rb.evaluate_cell(_cell_rows(10, 6), test_year=2024, stats_fn=variety_stats, flat_fn=variety_stats, seed=3)
    assert cell["deltas"]["flat"] == pytest.approx(0.0)
    assert cell["deltas"]["last_year"] == pytest.approx(0.0)
    assert cell["deltas"]["random"] == pytest.approx(cell["top3_utility_pct"] - cell["baselines"]["random"])


def _paired(crop, utility, last_year, n):
    return [{"crop": crop, "status": "ok", "spearman": 0.5, "top3_utility_pct": utility, "coverage_pct": None,
             "top3_present_share": 1.0, "baselines": {"random": 100.0, "last_year": last_year, "flat": None},
             "deltas": {"random": None if utility is None else utility - 100.0,
                        "last_year": None if utility is None or last_year is None else utility - last_year,
                        "flat": None}} for _ in range(n)]


def test_aggregate_reports_the_median_paired_delta_and_its_count():
    cells = _paired("X", 103.0, 104.0, 12) + _paired("X", None, 90.0, 20)
    agg = rb.aggregate_by_crop(cells)["X"]
    assert agg["delta_median"]["last_year"] == pytest.approx(-1.0) and agg["delta_n"]["last_year"] == 12
    assert agg["delta_median"]["flat"] is None and agg["delta_n"]["flat"] == 0


def test_gate_fails_when_the_model_loses_every_paired_cell_even_if_unpaired_medians_look_good():
    cells = _paired("TRZAX", 103.0, 104.0, 12) + _paired("TRZAX", None, 90.0, 20)
    agg = rb.aggregate_by_crop(cells)
    agg["TRZAX"].update({"measurable": True, "spearman_median": 0.5, "spearman_positive_share": 1.0,
                         "coverage_mean_pct": 80.0})
    assert rb.gate_verdict(agg)["TRZAX"] == "fail"


def test_gate_is_not_measurable_when_a_rival_was_never_paired():
    cells = _paired("TRZAX", 105.0, None, 31)
    agg = rb.aggregate_by_crop(cells)
    agg["TRZAX"].update({"measurable": True, "spearman_median": 0.5, "spearman_positive_share": 1.0,
                         "coverage_mean_pct": 80.0})
    assert rb.gate_verdict(agg)["TRZAX"] == "not_measurable"
