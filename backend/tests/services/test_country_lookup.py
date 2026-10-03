"""Point -> ISO 3166 alpha-2 country lookup over the bundled Natural Earth boundaries."""
import json

import pytest

from app.services import country_lookup as cl


@pytest.mark.parametrize("lat,lon,iso2", [
    (40.4168, -3.7038, "ES"),   # Madrid
    (48.8566, 2.3522, "FR"),    # Paris
    (38.7223, -9.1393, "PT"),   # Lisbon
    (52.5200, 13.4050, "DE"),   # Berlin
    (52.2297, 21.0122, "PL"),   # Warsaw
    (41.9028, 12.4964, "IT"),   # Rome
])
def test_capitals(lat, lon, iso2):
    assert cl.country_at(lat, lon) == iso2


def test_enclave_in_a_hole_of_the_surrounding_country():
    # San Marino city sits in a hole of Italy's polygon.
    assert cl.country_at(43.9356, 12.4473) == "SM"


@pytest.mark.parametrize("lat,lon", [(45.5, -5.0), (30.0444, 31.2357)])  # Bay of Biscay, Cairo
def test_outside_all_polygons(lat, lon):
    assert cl.country_at(lat, lon) is None


@pytest.mark.parametrize("lat,lon", [(None, -3.7), (40.4, None), (None, None)])
def test_none_inputs(lat, lon):
    assert cl.country_at(lat, lon) is None


def test_data_file_shape():
    data = json.loads(cl.DEFAULT_PATH.read_text())
    codes = [f["properties"]["iso_a2"] for f in data["features"]]
    assert {"ES", "FR", "NO", "CY", "TR"} <= set(codes)
    assert all(len(c) == 2 and c.isupper() for c in codes)


def _square(x0, y0, x1, y1):
    return [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]


def test_point_in_polygon_with_hole_and_multipolygon():
    poly = {"type": "Polygon", "coordinates": [_square(0, 0, 10, 10), _square(4, 4, 6, 6)]}
    assert cl._in_geometry(1.0, 1.0, poly)
    assert not cl._in_geometry(5.0, 5.0, poly)       # inside the hole
    assert not cl._in_geometry(11.0, 5.0, poly)
    multi = {"type": "MultiPolygon",
             "coordinates": [[_square(0, 0, 1, 1)], [_square(5, 5, 6, 6)]]}
    assert cl._in_geometry(5.5, 5.5, multi)
    assert not cl._in_geometry(3.0, 3.0, multi)
