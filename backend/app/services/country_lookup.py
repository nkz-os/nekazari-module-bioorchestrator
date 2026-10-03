"""Point -> ISO 3166 alpha-2 country, over bundled European boundaries.

Pure Python (no GIS dependency): a per-feature bounding-box prefilter, then
even-odd ray casting over every ring, so holes and enclaves (e.g. San Marino
inside Italy) resolve correctly. Boundaries are Natural Earth 1:10m, simplified;
see scripts/build_country_boundaries.py. Near a border the answer is approximate.
"""

from __future__ import annotations

import functools
import json
from pathlib import Path

# backend/data locally; /app/data in the image (Dockerfile: COPY backend/data/ data/).
DEFAULT_PATH = Path(__file__).resolve().parents[2] / "data" / "europe_countries.geojson"


def _polygons(geometry: dict) -> list:
    if geometry["type"] == "Polygon":
        return [geometry["coordinates"]]
    if geometry["type"] == "MultiPolygon":
        return geometry["coordinates"]
    raise ValueError(f"unsupported geometry type {geometry['type']!r}")


def _in_ring(x: float, y: float, ring: list) -> bool:
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def _in_geometry(x: float, y: float, geometry: dict) -> bool:
    """Even-odd rule over all rings of each polygon: a point in a hole is outside."""
    for rings in _polygons(geometry):
        if sum(_in_ring(x, y, ring) for ring in rings) % 2 == 1:
            return True
    return False


def _bbox(geometry: dict) -> tuple[float, float, float, float]:
    xs = [p[0] for rings in _polygons(geometry) for p in rings[0]]
    ys = [p[1] for rings in _polygons(geometry) for p in rings[0]]
    return min(xs), min(ys), max(xs), max(ys)


@functools.lru_cache(maxsize=1)
def _features() -> tuple:
    data = json.loads(DEFAULT_PATH.read_text())
    return tuple(
        (f["properties"]["iso_a2"], _bbox(f["geometry"]), f["geometry"])
        for f in data["features"]
    )


def country_at(lat: float | None, lon: float | None) -> str | None:
    """ISO 3166 alpha-2 code of the country containing the point, or None."""
    if lat is None or lon is None:
        return None
    x, y = float(lon), float(lat)
    for iso2, (x0, y0, x1, y1), geometry in _features():
        if x0 <= x <= x1 and y0 <= y <= y1 and _in_geometry(x, y, geometry):
            return iso2
    return None
