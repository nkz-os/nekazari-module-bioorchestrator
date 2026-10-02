"""Agro-climatic vector v2 (CHELSA on both sides): pure functions."""
from __future__ import annotations

from app.graph import agroclimatic as ac


def test_v2_constants():
    assert ac.FEATURES_V2 == ("aridity", "rainfall", "cold", "temp")
    assert ac.DEFAULT_WEIGHTS_V2 == {"aridity": 2.0, "rainfall": 1.0, "cold": 1.0, "temp": 1.0}


def test_v2_vector_values():
    v = ac.feature_vector_v2(500, 1000, -2.5, 14.0)
    assert v == {"aridity": 0.5, "rainfall": 500.0, "cold": -2.5, "temp": 14.0}


def test_v2_vector_none_cases():
    assert ac.feature_vector_v2(None, 1000, 0, 14) is None
    assert ac.feature_vector_v2(500, None, 0, 14) is None
    assert ac.feature_vector_v2(500, 1000, None, 14) is None
    assert ac.feature_vector_v2(500, 1000, 0, None) is None
    assert ac.feature_vector_v2(500, 0, 0, 14) is None
    # zero is a valid cold/temp value, not missing
    assert ac.feature_vector_v2(500, 1000, 0, 0) is not None


def test_v2_distance_identical_is_zero_and_orders_closer_first():
    target = ac.feature_vector_v2(500, 1000, -3, 13)
    near = ac.feature_vector_v2(520, 1000, -2, 13.5)
    far = ac.feature_vector_v2(1100, 900, 4, 17)
    bounds = ac.normalize_bounds([target, near, far], features=ac.FEATURES_V2)
    assert set(bounds) == set(ac.FEATURES_V2)
    kw = {"weights": ac.DEFAULT_WEIGHTS_V2, "features": ac.FEATURES_V2}
    assert ac.distance(target, target, bounds, **kw) == 0.0
    d_near = ac.distance(target, near, bounds, **kw)
    d_far = ac.distance(target, far, bounds, **kw)
    assert 0.0 < d_near < d_far


def test_v1_defaults_unchanged():
    bounds = ac.normalize_bounds([ac.feature_vector(500, 1000, 40, 300)])
    assert set(bounds) == set(ac.FEATURES)
