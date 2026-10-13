"""Variety ranking from trials (pure): yield relative to each trial environment, and stability.

An environment is one trial table: site x year x source. Each variety's yield is divided by
the environment mean, so year and site cancel out. The ranking is the mean of those ratios.
Stability is the Finlay-Wilkinson (1963) slope: the variety's yield regressed on the
environment mean. A variety with several rows in one environment counts once, at their mean;
a row without site or year cannot define an environment and is left out.
MIN_ENV_VARIETIES, MIN_FW_ENVIRONMENTS, FEW_ENVIRONMENTS and the slope bands are kept as they
are (owner, 2026-10-13), to revisit with real use. They are named constants, so a review changes
numbers, not code.
"""
from __future__ import annotations

from collections import defaultdict
from statistics import fmean

MIN_ENV_VARIETIES = 3
MIN_FW_ENVIRONMENTS = 5
FEW_ENVIRONMENTS = 3
STABLE_BELOW = 0.85
RESPONSIVE_ABOVE = 1.15


def fw_slope(env_means: list[float], yields: list[float]) -> float | None:
    if len(env_means) < 2:
        return None
    mx, my = fmean(env_means), fmean(yields)
    sxx = sum((x - mx) ** 2 for x in env_means)
    if sxx == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(env_means, yields)) / sxx


def stability_class(slope: float | None) -> str | None:
    if slope is None:
        return None
    if slope < STABLE_BELOW:
        return "stable"
    if slope > RESPONSIVE_ABOVE:
        return "responsive"
    return "average"


def variety_stats(rows: list[dict]) -> tuple[list[dict], int]:
    cells: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        variety, value = row.get("variety"), row.get("yield_kg_ha")
        site, year = row.get("site"), row.get("year")
        if not variety or not isinstance(value, (int, float)) or value <= 0 or not site or year is None:
            continue
        cells[(site, year, row.get("source_id"))][variety].append(float(value))
    envs = {key: {variety: fmean(values) for variety, values in by_variety.items()}
            for key, by_variety in cells.items()}
    usable = {k: v for k, v in envs.items() if len(v) >= MIN_ENV_VARIETIES}
    means = {k: fmean(v.values()) for k, v in usable.items()}
    per_variety: dict[str, list[tuple[tuple, float, float]]] = defaultdict(list)
    for key, values in usable.items():
        for variety, value in values.items():
            per_variety[variety].append((key, value, means[key]))
    stats = []
    for variety, obs in per_variety.items():
        years = sorted({k[1] for k, _, _ in obs if isinstance(k[1], int)})
        slope = fw_slope([m for _, _, m in obs], [y for _, y, _ in obs]) if len(obs) >= MIN_FW_ENVIRONMENTS else None
        shown_slope = None if slope is None else round(slope, 2)
        stats.append({
            "variety": variety,
            "relative_yield_pct": round(fmean(y / m for _, y, m in obs) * 100, 1),
            "n_environments": len(obs),
            "years": [years[0], years[-1]] if years else [],
            "mean_kg_ha": round(fmean(y for _, y, _ in obs)),
            "fw_slope": shown_slope,
            "stability": stability_class(shown_slope),
            "few_environments": len(obs) < FEW_ENVIRONMENTS,
        })
    stats.sort(key=lambda s: (s["few_environments"], -s["relative_yield_pct"], -s["n_environments"], s["variety"]))
    return stats, len(usable)
