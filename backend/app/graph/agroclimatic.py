"""C.1 — agro-climatic distance between sites.

Köppen classes are coarse: a coastal-Atlantic and a continental-interior site can
share a class yet be agronomically unlike. This module scores similarity as a
normalized weighted distance over a small agro-climatic feature vector:

  * aridity   = annualRainfall / annualET0  (the dominant Mediterranean signal)
  * rainfall  = annual rainfall (mm)
  * frost     = frost days per year
  * elevation = m a.s.l.

Pure functions, no Neo4j — the DAO computes population bounds once and calls these.
Sites lacking any component have no vector; the DAO falls back to Köppen for them
(soft prior, never a hard gate) so coverage is preserved.
"""
from __future__ import annotations

import math

FEATURES: tuple[str, ...] = ("aridity", "rainfall", "frost", "elevation")

# Normalized distances fall in [0, 1]; a same-Köppen site with no numeric vector is
# kept but pushed beyond any real analog so it only fills coverage when nothing
# closer exists.
KOPPEN_FALLBACK_DISTANCE: float = 2.0

# Aridity carries the most agronomic signal for peninsular/Mediterranean parcels;
# elevation the least. Tunable — C.3 backtest measures the effect.
DEFAULT_WEIGHTS: dict[str, float] = {
    "aridity": 2.0,
    "rainfall": 1.0,
    "frost": 1.0,
    "elevation": 0.5,
}


def feature_vector(
    rainfall: float | None,
    et0: float | None,
    frost: float | None,
    elevation: float | None,
) -> dict[str, float] | None:
    """Build the agro-climatic vector, or None if any component is missing."""
    if rainfall is None or et0 is None or frost is None or elevation is None:
        return None
    if not et0:  # guard div-by-zero
        return None
    return {
        "aridity": rainfall / et0,
        "rainfall": float(rainfall),
        "frost": float(frost),
        "elevation": float(elevation),
    }


def normalize_bounds(vectors: list[dict[str, float] | None]) -> dict[str, tuple[float, float]]:
    """Per-feature (min, max) over the site population, for min-max normalization."""
    bounds: dict[str, tuple[float, float]] = {}
    for f in FEATURES:
        vals = [v[f] for v in vectors if v is not None]
        bounds[f] = (min(vals), max(vals)) if vals else (0.0, 1.0)
    return bounds


def distance(
    target: dict[str, float],
    candidate: dict[str, float],
    bounds: dict[str, tuple[float, float]],
    weights: dict[str, float] | None = None,
) -> float:
    """Normalized weighted Euclidean distance in [0, ~1] between two vectors.

    Each feature is min-max scaled to [0, 1] over the population bounds, so no
    single feature (e.g. elevation in metres) dominates by unit magnitude.
    """
    w = weights or DEFAULT_WEIGHTS
    total_w = sum(w[f] for f in FEATURES)
    acc = 0.0
    for f in FEATURES:
        lo, hi = bounds[f]
        span = (hi - lo) or 1.0
        tn = (target[f] - lo) / span
        cn = (candidate[f] - lo) / span
        acc += w[f] * (tn - cn) ** 2
    return math.sqrt(acc / total_w) if total_w else 0.0
