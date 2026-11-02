"""Forward-chaining backtest of the variety table the farmer sees (pure; no graph access).

The unit under test is ``variety_stats`` (``app.planning.varieties``), i.e. the table of
``parcel_varieties``: for a test year *t* the table is built from trials of years < *t* only and
compared with what the trials of year *t* measured. The statistics function is injected so the
same harness measures the table before and after a change (the ``flat`` baseline is the table
without the change).

Three metrics per cell (crop x zone x regime x test year), each behind a guard that turns a thin
cell into a ``skipped`` cell with a reason, never into a zero:

* ``spearman``: rank correlation between the predicted order and the observed order (mean of
  the test-year ratios) over the varieties present on both sides.
* ``top3_utility_pct``: mean, over test environments, of the mean ratio of the predicted top-3
  present there, x100; compared with ``random`` (simulated), ``last_year`` (top-3 of the latest
  training year) and ``flat``.
* ``coverage_pct``: share of test environments whose mean falls inside the P10-P90 range of the
  training environment means (kg/ha).

Thresholds are ASSUMPTIONs (named constants, report-only until the owner fixes them on the
baseline). Spearman is computed by hand (ranks + Pearson) so scipy is not a dependency.
"""
from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Callable
from statistics import fmean, median, quantiles
from typing import Any

from app.planning.varieties import MIN_ENV_VARIETIES

StatsFn = Callable[[list[dict]], tuple[list[dict], int]]

MIN_COMMON_VARIETIES = 8
MIN_TEST_ENVIRONMENTS = 5
MIN_TRAIN_YEARS = 5
TEST_YEAR_FIRST = 2016
TEST_YEAR_LAST = 2025
MIN_FOLDS = 30
TOP_N = 3
RANDOM_DRAWS = 2000

# ASSUMPTION (owner fixes them after the baseline; report-only until then).
THRESHOLDS: dict[str, float] = {
    "SPEARMAN_MEDIAN_MIN": 0.30,
    "SPEARMAN_POSITIVE_SHARE_MIN": 0.70,
    "TOP3_UTILITY_MIN_PCT": 102.0,
    "COVERAGE_MIN": 0.70,
    "COVERAGE_MAX": 0.90,
}
GATED_CROPS = ("TRZAX", "HORVX", "TRZDU", "ZEAMX", "BRSNN")
MEASURED_CROPS = ("TTLSS", "AVESA", "SECCE", "HELAN")


def _ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        rank = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[order[k]] = rank
        i = j + 1
    return ranks


def spearman(a: list[float], b: list[float]) -> float | None:
    """Rank correlation; None with fewer than two points or no spread on one side."""
    if len(a) != len(b) or len(a) < 2:
        return None
    ra, rb_ = _ranks([float(x) for x in a]), _ranks([float(x) for x in b])
    ma, mb = fmean(ra), fmean(rb_)
    sa = sum((x - ma) ** 2 for x in ra)
    sb = sum((y - mb) ** 2 for y in rb_)
    if sa == 0 or sb == 0:
        return None
    return sum((x - ma) * (y - mb) for x, y in zip(ra, rb_)) / (sa * sb) ** 0.5


def split_forward(rows: list[dict], test_year: int) -> tuple[list[dict], list[dict]]:
    """Train = rows of years strictly before ``test_year``; test = rows of ``test_year``."""
    train = [r for r in rows if isinstance(r.get("year"), int) and r["year"] < test_year]
    test = [r for r in rows if r.get("year") == test_year]
    return train, test


def environment_ratios(rows: list[dict]) -> dict[tuple, dict[str, float]]:
    """Per environment (site x year x source), each variety's yield over the environment mean.

    Same environment rules as ``variety_stats``: duplicate rows of a variety are averaged, an
    environment with fewer than MIN_ENV_VARIETIES varieties is dropped.
    """
    cells: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        variety, value = row.get("variety"), row.get("yield_kg_ha")
        site, year = row.get("site"), row.get("year")
        if not variety or not isinstance(value, (int, float)) or value <= 0 or not site or year is None:
            continue
        cells[(site, year, row.get("source_id"))][variety].append(float(value))
    out: dict[tuple, dict[str, float]] = {}
    for key, by_variety in cells.items():
        if len(by_variety) < MIN_ENV_VARIETIES:
            continue
        means = {v: fmean(values) for v, values in by_variety.items()}
        env_mean = fmean(means.values())
        out[key] = {v: m / env_mean for v, m in means.items()}
    return out


def environment_means(rows: list[dict]) -> dict[tuple, float]:
    """Mean yield (kg/ha over varieties) of each usable environment."""
    cells: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        variety, value = row.get("variety"), row.get("yield_kg_ha")
        site, year = row.get("site"), row.get("year")
        if not variety or not isinstance(value, (int, float)) or value <= 0 or not site or year is None:
            continue
        cells[(site, year, row.get("source_id"))][variety].append(float(value))
    return {key: fmean(fmean(v) for v in by_variety.values())
            for key, by_variety in cells.items() if len(by_variety) >= MIN_ENV_VARIETIES}


def observed_order(env_ratios: dict[tuple, dict[str, float]]) -> dict[str, float]:
    """Mean ratio of each variety over the test environments."""
    acc: dict[str, list[float]] = defaultdict(list)
    for ratios in env_ratios.values():
        for variety, ratio in ratios.items():
            acc[variety].append(ratio)
    return {v: fmean(r) for v, r in acc.items()}


def top3_utility_pct(predicted: list[str], env_ratios: dict[tuple, dict[str, float]]) -> float | None:
    """Mean over environments of the mean ratio of the predicted top-3 present (>= 2 of 3), x100."""
    top = predicted[:TOP_N]
    per_env = []
    for ratios in env_ratios.values():
        present = [ratios[v] for v in top if v in ratios]
        if len(present) >= 2:
            per_env.append(fmean(present))
    return fmean(per_env) * 100 if per_env else None


def last_year_top3(train: list[dict], test_year: int) -> list[str]:
    """Top-3 by mean ratio in the latest training year with usable environments."""
    years = sorted({r["year"] for r in train if isinstance(r.get("year"), int) and r["year"] < test_year},
                   reverse=True)
    for year in years:
        ratios = environment_ratios([r for r in train if r.get("year") == year])
        if ratios:
            order = observed_order(ratios)
            return sorted(order, key=lambda v: (-order[v], v))[:TOP_N]
    return []


def random_top3_utility_pct(env_ratios: dict[tuple, dict[str, float]], *, seed: int,
                            draws: int = RANDOM_DRAWS) -> float | None:
    """Utility of a random top-3 drawn from the varieties observed in the test environments."""
    pool = sorted({v for ratios in env_ratios.values() for v in ratios})
    if len(pool) < TOP_N:
        return None
    rng = random.Random(seed)
    values = []
    for _ in range(draws):
        pick = rng.sample(pool, TOP_N)
        utility = top3_utility_pct(pick, env_ratios)
        if utility is not None:
            values.append(utility)
    return fmean(values) if values else None


def coverage_pct(train: list[dict], test: list[dict]) -> float | None:
    """Share of test environments whose mean lies inside the train P10-P90 of environment means."""
    train_means = list(environment_means(train).values())
    test_means = list(environment_means(test).values())
    if len(test_means) < MIN_TEST_ENVIRONMENTS or len(train_means) < 2:
        return None
    deciles = quantiles(train_means, n=10, method="inclusive")
    low, high = deciles[0], deciles[-1]
    inside = sum(1 for m in test_means if low <= m <= high)
    return inside / len(test_means) * 100


def predicted_order(stats_fn: StatsFn, train: list[dict]) -> list[str]:
    stats, _ = stats_fn(train)
    return [s["variety"] for s in stats]


RIVALS = ("random", "last_year", "flat", "recent_only")
RECENT_RIVAL_YEARS = 3  # the "recent_only" rival builds its table from the last N training campaigns


def evaluate_cell(rows: list[dict], *, test_year: int, stats_fn: StatsFn, flat_fn: StatsFn | None = None,
                  recent_fn: StatsFn | None = None, recent_years: int = RECENT_RIVAL_YEARS, seed: int = 0,
                  min_test_environments: int = MIN_TEST_ENVIRONMENTS, **labels: Any) -> dict:
    """One cell: train on years < test_year, score on test_year. ``labels`` are copied through.

    ``min_test_environments`` is the guard of the cell (the coverage metric keeps its own guard: a
    P10-P90 share over one environment means nothing). ``recent_fn`` scores the ``recent_only`` rival:
    the same kind of table built from the last ``recent_years`` training campaigns only.
    """
    train, test = split_forward(rows, test_year)
    env_ratios = environment_ratios(test)
    cell: dict[str, Any] = {**labels, "test_year": test_year, "n_train_rows": len(train),
                            "n_test_environments": len(env_ratios)}
    if len(env_ratios) < min_test_environments:
        return {**cell, "status": "skipped", "reason": "test_environments"}
    predicted = predicted_order(stats_fn, train)
    observed = observed_order(env_ratios)
    common = [v for v in predicted if v in observed]
    cell["n_common_varieties"] = len(common)
    if len(common) < MIN_COMMON_VARIETIES:
        return {**cell, "status": "skipped", "reason": "common_varieties"}
    rank_pred = list(range(len(common)))
    rank_obs = [observed[v] for v in common]
    rho = spearman(rank_pred, [-x for x in rank_obs])  # predicted index 0 = best; higher ratio = better
    recent_train = [r for r in train if isinstance(r.get("year"), int) and r["year"] >= test_year - recent_years]
    baselines = {
        "random": random_top3_utility_pct(env_ratios, seed=seed),
        "last_year": top3_utility_pct(last_year_top3(train, test_year), env_ratios),
        "flat": top3_utility_pct(predicted_order(flat_fn, train), env_ratios) if flat_fn else None,
        "recent_only": (top3_utility_pct(predicted_order(recent_fn, recent_train), env_ratios)
                        if recent_fn and recent_train else None),
    }
    present = sum(1 for v in predicted[:TOP_N] if v in observed)
    utility = top3_utility_pct(predicted, env_ratios)
    # Paired comparison: the model minus each rival in THIS cell, only where both are scored.
    deltas = {name: (utility - rival if utility is not None and rival is not None else None)
              for name, rival in baselines.items()}
    return {**cell, "status": "ok", "spearman": rho,
            "top3_utility_pct": utility,
            "top3_present_share": present / TOP_N,  # predicted top-3 still trialled in the test year
            "top3_predicted": predicted[:TOP_N], "baselines": baselines, "deltas": deltas,
            "coverage_pct": coverage_pct(train, test)}


def _median(values: list[float]) -> float | None:
    return median(values) if values else None


def aggregate_by_crop(cells: list[dict]) -> dict[str, dict]:
    """Per crop: medians, positive share, worst cell, baselines, coverage, skip reasons."""
    by_crop: dict[str, list[dict]] = defaultdict(list)
    for cell in cells:
        by_crop[cell.get("crop", "?")].append(cell)
    out: dict[str, dict] = {}
    for crop, all_items in by_crop.items():
        # A cell read from a page-capped group is a partial sample: counted, never aggregated.
        n_truncated = sum(1 for c in all_items if c.get("truncated"))
        items = [c for c in all_items if not c.get("truncated")]
        ok = [c for c in items if c.get("status") == "ok"]
        skipped: dict[str, int] = defaultdict(int)
        for c in items:
            if c.get("status") == "skipped":
                skipped[c.get("reason", "?")] += 1
        rhos = [c["spearman"] for c in ok if c.get("spearman") is not None]
        utilities = [c["top3_utility_pct"] for c in ok if c.get("top3_utility_pct") is not None]
        coverages = [c["coverage_pct"] for c in ok if c.get("coverage_pct") is not None]
        present = [c["top3_present_share"] for c in ok if c.get("top3_present_share") is not None]
        baselines = {name: _median([c["baselines"][name] for c in ok
                                    if c.get("baselines", {}).get(name) is not None])
                     for name in RIVALS}
        paired = {name: [c["deltas"][name] for c in ok if (c.get("deltas") or {}).get(name) is not None]
                  for name in RIVALS}
        out[crop] = {
            "n_cells": len(ok),
            "skipped": dict(skipped),
            "n_truncated": n_truncated,
            "measurable": len(ok) >= MIN_FOLDS,
            "spearman_median": _median(rhos),
            "spearman_positive_share": (sum(1 for r in rhos if r > 0) / len(rhos)) if rhos else None,
            "spearman_worst": min(rhos) if rhos else None,
            "top3_utility_median_pct": _median(utilities),
            "top3_present_share_mean": fmean(present) if present else None,
            "baselines_median": baselines,  # unpaired, descriptive only
            "delta_median": {name: _median(v) for name, v in paired.items()},  # paired: model minus rival
            "delta_n": {name: len(v) for name, v in paired.items()},
            "coverage_mean_pct": fmean(coverages) if coverages else None,
        }
    return out



def test_years_for(rows: list[dict], *, first: int = TEST_YEAR_FIRST, last: int = TEST_YEAR_LAST,
                   min_train_years: int = MIN_TRAIN_YEARS) -> list[int]:
    """Test years in [first, last] that have rows and at least ``min_train_years`` earlier years with rows."""
    years = sorted({r["year"] for r in rows if isinstance(r.get("year"), int)})
    return [t for t in years if first <= t <= last and sum(1 for y in years if y < t) >= min_train_years]


def gate_verdict(summary: dict[str, dict]) -> dict[str, str]:
    """Report-mode verdict per gated crop: 'pass', 'fail' or 'not_measurable' (never raises)."""
    verdict = {}
    for crop in GATED_CROPS:
        agg = summary.get(crop)
        if not agg or not agg["measurable"]:
            verdict[crop] = "not_measurable"
            continue
        t = THRESHOLDS
        rho, share = agg["spearman_median"], agg["spearman_positive_share"]
        utility, cover = agg["top3_utility_median_pct"], agg["coverage_mean_pct"]
        deltas, counts = agg.get("delta_median") or {}, agg.get("delta_n") or {}
        # A rival that was never paired with the model is not beaten: the comparison cannot be made.
        if any(not counts.get(n) for n in ("random", "last_year")):
            verdict[crop] = "not_measurable"
            continue
        ok = (rho is not None and rho >= t["SPEARMAN_MEDIAN_MIN"]
              and share is not None and share >= t["SPEARMAN_POSITIVE_SHARE_MIN"]
              and utility is not None and utility >= t["TOP3_UTILITY_MIN_PCT"]
              and all(deltas[n] is not None and deltas[n] > 0 for n in ("random", "last_year"))
              and cover is not None and t["COVERAGE_MIN"] * 100 <= cover <= t["COVERAGE_MAX"] * 100)
        verdict[crop] = "pass" if ok else "fail"
    return verdict


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def render_markdown(report: dict) -> str:
    """Human-readable report: meta, per-crop summary, verdict and EVERY cell."""
    lines = ["# Ranking backtest (forward-chaining)", ""]
    for key, value in (report.get("meta") or {}).items():
        lines.append(f"- {key}: {value}")
    header = ("| crop | cells | skipped | measurable | ρ median | ρ>0 share | ρ worst | top-3 % | top-3 present | "
              + " | ".join(RIVALS) + " | " + " | ".join(f"Δ {n} (n)" for n in RIVALS) + " | coverage % |")
    lines += ["", "## Per crop", "", header, "|" + "---|" * (9 + 2 * len(RIVALS) + 1)]
    for crop, agg in sorted((report.get("summary") or {}).items()):
        b = agg["baselines_median"]
        skipped = ", ".join(f"{k}={v}" for k, v in sorted(agg["skipped"].items())) or "—"
        lines.append(f"| {crop} | {agg['n_cells']} | {skipped} | {agg['measurable']} | "
                     f"{_fmt(agg['spearman_median'])} | {_fmt(agg['spearman_positive_share'])} | "
                     f"{_fmt(agg['spearman_worst'])} | {_fmt(agg['top3_utility_median_pct'], 1)} | "
                     f"{_fmt(agg.get('top3_present_share_mean'))} | "
                     + " | ".join(_fmt(b.get(n), 1) for n in RIVALS) + " | "
                     + " | ".join(f"{_fmt((agg.get('delta_median') or {}).get(n), 1)} ({(agg.get('delta_n') or {}).get(n, 0)})"
                                  for n in RIVALS)
                     + f" | {_fmt(agg['coverage_mean_pct'], 1)} |")
    note = ("Rival columns are unpaired medians (descriptive); Δ columns are the median of the per-cell "
            "difference model − rival over the n cells where both are scored (the gate reads Δ).")
    lines += ["", note, "", "## Verdict (report mode, ASSUMPTION thresholds)", ""]
    for crop, status in (report.get("verdict") or {}).items():
        lines.append(f"- {crop}: {status}")
    guards = (f"Thresholds: {THRESHOLDS}; MIN_FOLDS={MIN_FOLDS}; MIN_COMMON_VARIETIES={MIN_COMMON_VARIETIES}; "
              f"MIN_TEST_ENVIRONMENTS={MIN_TEST_ENVIRONMENTS}")
    cell_header = ("| crop | zone | regime | year | status | reason | common | test envs | ρ | top-3 % | top-3 present | "
                   + " | ".join(RIVALS) + " | coverage % |")
    lines += ["", guards, "", "## Every cell", "", cell_header, "|" + "---|" * (11 + len(RIVALS) + 1)]
    for c in report.get("cells") or []:
        b = c.get("baselines") or {}
        lines.append(f"| {c.get('crop')} | {c.get('zone')} | {c.get('regime')} | {c.get('test_year')} | "
                     f"{c.get('status')} | {c.get('reason', '—')} | {_fmt(c.get('n_common_varieties'))} | "
                     f"{_fmt(c.get('n_test_environments'))} | {_fmt(c.get('spearman'))} | "
                     f"{_fmt(c.get('top3_utility_pct'), 1)} | {_fmt(c.get('top3_present_share'))} | "
                     + " | ".join(_fmt(b.get(n), 1) for n in RIVALS)
                     + f" | {_fmt(c.get('coverage_pct'), 1)} |")
    return "\n".join(lines) + "\n"
