"""Parcel centroid extraction must terminate for every GeoJSON shape."""

from __future__ import annotations

import pytest

from app.graph.dao import _geometry_centroid

SQUARE = [[[0.0, 0.0], [2.0, 0.0], [2.0, 2.0], [0.0, 2.0], [0.0, 0.0]]]


def test_point():
    assert _geometry_centroid([-1.8, 42.1]) == (-1.8, 42.1)


def test_polygon_uses_outer_ring_mean_without_closing_vertex():
    assert _geometry_centroid(SQUARE) == pytest.approx((1.0, 1.0))


def test_polygon_with_hole_ignores_inner_ring():
    hole = [[0.5, 0.5], [0.6, 0.5], [0.6, 0.6], [0.5, 0.5]]
    assert _geometry_centroid([SQUARE[0], hole]) == pytest.approx((1.0, 1.0))


def test_multipolygon_uses_first_polygon():
    other = [[[10.0, 10.0], [12.0, 10.0], [12.0, 12.0], [10.0, 10.0]]]
    assert _geometry_centroid([SQUARE, other]) == pytest.approx((1.0, 1.0))


def test_open_ring_without_closing_vertex():
    ring = [[0.0, 0.0], [2.0, 0.0], [2.0, 2.0], [0.0, 2.0]]
    assert _geometry_centroid([ring]) == pytest.approx((1.0, 1.0))


@pytest.mark.parametrize("coords", [[], [[]], [[[]]], "x", None, [["a", "b"]], [[1.0]]])
def test_malformed_returns_none(coords):
    assert _geometry_centroid(coords) is None
