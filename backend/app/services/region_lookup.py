"""Point -> Spanish province (INE code), over bundled Natural Earth admin-1 boundaries.

Same method and caveats as ``country_lookup`` (bounding-box prefilter, even-odd ray
casting, simplified boundaries: approximate within a few hundred metres of a border).
Data: scripts/build_es_provinces.py.
"""

from __future__ import annotations

import functools
import json
from pathlib import Path

from app.services.country_lookup import _bbox, _in_geometry

# backend/data locally; /app/data in the image (Dockerfile: COPY backend/data/ data/).
DEFAULT_PATH = Path(__file__).resolve().parents[2] / "data" / "es_provinces.geojson"
SCHEME = "es_province"


@functools.lru_cache(maxsize=1)
def _features() -> tuple:
    data = json.loads(DEFAULT_PATH.read_text(encoding="utf-8"))
    return tuple(
        (f["properties"]["ine_code"], f["properties"]["name"], _bbox(f["geometry"]), f["geometry"])
        for f in data["features"]
    )


def es_province_at(lat: float | None, lon: float | None) -> dict | None:
    """``{"scheme": "es_province", "code": INE code, "name": official name}`` or None."""
    if lat is None or lon is None:
        return None
    x, y = float(lon), float(lat)
    for code, name, (x0, y0, x1, y1), geometry in _features():
        if x0 <= x <= x1 and y0 <= y <= y1 and _in_geometry(x, y, geometry):
            return {"scheme": SCHEME, "code": code, "name": name}
    return None
