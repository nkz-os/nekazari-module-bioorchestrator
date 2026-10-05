"""Round-trip of the value codec used by the Neo4j backup archive (no database needed)."""
import math

import pytest
from neo4j.spatial import CartesianPoint, WGS84Point
from neo4j.time import Date, DateTime, Duration, Time

from scripts.neo4j_restore_from_export import canon, dec, enc


@pytest.mark.parametrize(
    "value",
    [
        "text",
        7,
        2.5,
        True,
        ["a", "b"],
        [1, 2, 3],
        b"\x00\x01binary",
        DateTime(2024, 5, 1, 10, 0, 0, 0),
        Date(2024, 1, 2),
        Time(10, 0, 0, 0),
        Duration(months=1, days=2, seconds=3, nanoseconds=4),
        [DateTime(2024, 5, 1, 10, 0, 0, 0), DateTime(2024, 6, 1, 10, 0, 0, 0)],
        WGS84Point((-1.6, 42.8)),
        CartesianPoint((1.0, 2.0, 3.0)),
    ],
)
def test_round_trip(value):
    assert dec(enc(value)) == value


def test_duration_is_not_flattened_to_a_list():
    # neo4j.time.Duration is a tuple subclass; it must keep its type tag.
    assert enc(Duration(days=1))["$t"] == "duration"


def test_non_finite_floats_round_trip():
    assert math.isnan(dec(enc(float("nan"))))
    assert dec(enc(float("inf"))) == float("inf")


def test_canonical_form_is_key_order_independent():
    assert canon({"b": 1, "a": 2}) == canon({"a": 2, "b": 1})
