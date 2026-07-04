"""C.1 — pure agro-climatic distance (no Neo4j, fast).

A site's agronomic similarity is a weighted distance over an agro-climatic feature
vector — aridity (rain/ET0), rainfall, frost days, elevation — not the coarse Köppen
label. Coastal-Atlantic and continental-interior sites that share a Köppen class must
land at different distances from a target.
"""
from __future__ import annotations

import math

from app.graph.agroclimatic import feature_vector, normalize_bounds, distance


def test_feature_vector_computes_aridity_or_none_when_incomplete():
    v = feature_vector(rainfall=500, et0=1000, frost=40, elevation=300)
    assert v["aridity"] == 0.5
    assert v["rainfall"] == 500 and v["frost"] == 40 and v["elevation"] == 300
    # Any missing component (or et0=0) → no vector.
    assert feature_vector(rainfall=500, et0=None, frost=40, elevation=300) is None
    assert feature_vector(rainfall=None, et0=1000, frost=40, elevation=300) is None
    assert feature_vector(rainfall=500, et0=0, frost=40, elevation=300) is None


def test_identical_vector_has_zero_distance():
    vecs = [
        feature_vector(500, 1000, 40, 300),
        feature_vector(900, 900, 5, 50),
    ]
    bounds = normalize_bounds(vecs)
    target = feature_vector(500, 1000, 40, 300)
    assert distance(target, target, bounds) == 0.0


def test_same_koppen_different_aridity_ranks_by_distance():
    # Target is semi-arid (aridity 0.5). CandA is near-identical; CandB is humid
    # (aridity ~1.0) — both could be the same Köppen bucket, but A must be closer.
    target = feature_vector(500, 1000, 40, 300)
    cand_a = feature_vector(520, 1000, 38, 320)   # very similar
    cand_b = feature_vector(1000, 1000, 5, 300)   # much wetter, frost-free
    bounds = normalize_bounds([target, cand_a, cand_b])

    d_a = distance(target, cand_a, bounds)
    d_b = distance(target, cand_b, bounds)
    assert d_a < d_b
    assert d_a > 0.0
    assert math.isfinite(d_b)
