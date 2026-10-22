"""Build backend/data/europe_countries.geojson from Natural Earth (offline tool).

Source: Natural Earth 1:10m Cultural Vectors, Admin 0 - Countries, v5.1.2
  https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/
  GeoJSON mirror used here:
  https://raw.githubusercontent.com/nvkelso/natural-earth-vector/v5.1.2/geojson/ne_10m_admin_0_countries.geojson
License: public domain (https://www.naturalearthdata.com/about/terms-of-use/).

Keeps CONTINENT == "Europe" plus Cyprus and Turkey. The only property kept is
``iso_a2``, taken from ISO_A2_EH because ISO_A2 is -99 for France and Norway.
Geometries are simplified per feature (Douglas-Peucker, topology preserved
within each feature) and coordinates rounded to 4 decimals. Per-feature
simplification leaves slivers along shared borders, so lookups within a few
hundred metres of a border are approximate.

Build-time only: needs ``shapely`` (not a runtime dependency of the service).

Usage:
    python scripts/build_country_boundaries.py [SOURCE.geojson] [--out PATH]
Without SOURCE the file is downloaded from the mirror above.
"""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path

from shapely.geometry import mapping, shape

SOURCE_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/v5.1.2/"
    "geojson/ne_10m_admin_0_countries.geojson"
)
DEFAULT_OUT = Path(__file__).resolve().parents[1] / "data" / "europe_countries.geojson"
EXTRA_ISO2 = {"CY", "TR"}
TOLERANCE_DEG = 0.005
DECIMALS = 4


def _round(coords):
    if isinstance(coords[0], (int, float)):
        return [round(coords[0], DECIMALS), round(coords[1], DECIMALS)]
    return [_round(c) for c in coords]


def build(source: dict) -> dict:
    features = []
    for feat in source["features"]:
        props = feat["properties"]
        iso2 = props.get("ISO_A2_EH")
        if props.get("CONTINENT") != "Europe" and iso2 not in EXTRA_ISO2:
            continue
        if not iso2 or iso2 == "-99":
            raise ValueError(f"no ISO_A2_EH for {props.get('NAME')}")
        geom = shape(feat["geometry"]).simplify(TOLERANCE_DEG, preserve_topology=True)
        if geom.is_empty:
            raise ValueError(f"{iso2} vanished after simplification")
        gj = mapping(geom)
        features.append({
            "type": "Feature",
            "properties": {"iso_a2": iso2},
            "geometry": {"type": gj["type"], "coordinates": _round(gj["coordinates"])},
        })
    features.sort(key=lambda f: f["properties"]["iso_a2"])
    return {"type": "FeatureCollection", "features": features}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("source", nargs="?", help="local ne_10m_admin_0_countries.geojson")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    if args.source:
        source = json.loads(Path(args.source).read_text())
    else:
        with urllib.request.urlopen(SOURCE_URL, timeout=120) as resp:
            source = json.loads(resp.read())
    out = build(source)
    args.out.write_text(json.dumps(out, separators=(",", ":")))
    print(f"{len(out['features'])} features -> {args.out} ({args.out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
