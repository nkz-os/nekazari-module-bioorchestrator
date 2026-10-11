"""Build backend/data/es_provinces.geojson from Natural Earth (offline tool).

Source: Natural Earth 1:10m Cultural Vectors, Admin 1 - States, Provinces, v5.1.2
  https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-1-states-provinces/
  GeoJSON mirror used here:
  https://raw.githubusercontent.com/nvkelso/natural-earth-vector/v5.1.2/geojson/ne_10m_admin_1_states_provinces.geojson
License: public domain (https://www.naturalearthdata.com/about/terms-of-use/).

Keeps the Spanish features (adm0_a3 == "ESP") and maps their ISO 3166-2 code to the
INE province code. The names are the official INE names, not Natural Earth's (which
uses some older forms). Parts of one province are merged, then simplified (Douglas-
Peucker, topology preserved within each feature) and rounded to 4 decimals, as in
build_country_boundaries.py; lookups within a few hundred metres of a border are
approximate.

Build-time only: needs ``shapely`` (not a runtime dependency of the service).

Usage:
    python scripts/build_es_provinces.py [SOURCE.geojson] [--out PATH]
Without SOURCE the file is downloaded from the mirror above.
"""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path

from shapely.geometry import mapping, shape
from shapely.ops import unary_union

SOURCE_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/v5.1.2/"
    "geojson/ne_10m_admin_1_states_provinces.geojson"
)
DEFAULT_OUT = Path(__file__).resolve().parents[1] / "data" / "es_provinces.geojson"
TOLERANCE_DEG = 0.005
DECIMALS = 4

# ISO 3166-2:ES province subdivision -> (INE code, official INE name)
ISO_TO_INE = {
    "VI": ("01", "Araba/Álava"), "AB": ("02", "Albacete"), "A": ("03", "Alicante/Alacant"),
    "AL": ("04", "Almería"), "AV": ("05", "Ávila"), "BA": ("06", "Badajoz"),
    "PM": ("07", "Illes Balears"), "B": ("08", "Barcelona"), "BU": ("09", "Burgos"),
    "CC": ("10", "Cáceres"), "CA": ("11", "Cádiz"), "CS": ("12", "Castellón/Castelló"),
    "CR": ("13", "Ciudad Real"), "CO": ("14", "Córdoba"), "C": ("15", "A Coruña"),
    "CU": ("16", "Cuenca"), "GI": ("17", "Girona"), "GR": ("18", "Granada"),
    "GU": ("19", "Guadalajara"), "SS": ("20", "Gipuzkoa"), "H": ("21", "Huelva"),
    "HU": ("22", "Huesca"), "J": ("23", "Jaén"), "LE": ("24", "León"), "L": ("25", "Lleida"),
    "LO": ("26", "La Rioja"), "LU": ("27", "Lugo"), "M": ("28", "Madrid"),
    "MA": ("29", "Málaga"), "MU": ("30", "Murcia"), "NA": ("31", "Navarra"),
    "OR": ("32", "Ourense"), "O": ("33", "Asturias"), "P": ("34", "Palencia"),
    "GC": ("35", "Las Palmas"), "PO": ("36", "Pontevedra"), "SA": ("37", "Salamanca"),
    "TF": ("38", "Santa Cruz de Tenerife"), "S": ("39", "Cantabria"), "SG": ("40", "Segovia"),
    "SE": ("41", "Sevilla"), "SO": ("42", "Soria"), "T": ("43", "Tarragona"),
    "TE": ("44", "Teruel"), "TO": ("45", "Toledo"), "V": ("46", "Valencia/València"),
    "VA": ("47", "Valladolid"), "BI": ("48", "Bizkaia"), "ZA": ("49", "Zamora"),
    "Z": ("50", "Zaragoza"), "CE": ("51", "Ceuta"), "ML": ("52", "Melilla"),
}


def _round(coords):
    if isinstance(coords[0], (int, float)):
        return [round(coords[0], DECIMALS), round(coords[1], DECIMALS)]
    return [_round(c) for c in coords]


def build(source: dict) -> dict:
    parts: dict[str, list] = {}
    for feat in source["features"]:
        props = feat["properties"]
        if props.get("adm0_a3") != "ESP":
            continue
        iso = str(props.get("iso_3166_2", "")).removeprefix("ES-")
        if iso not in ISO_TO_INE:
            raise ValueError(f"unmapped Spanish subdivision {props.get('iso_3166_2')!r}")
        parts.setdefault(iso, []).append(shape(feat["geometry"]))
    missing = sorted(set(ISO_TO_INE) - set(parts))
    if missing:
        raise ValueError(f"provinces missing from the source: {missing}")
    features = []
    for iso, geoms in sorted(parts.items(), key=lambda kv: ISO_TO_INE[kv[0]][0]):
        code, name = ISO_TO_INE[iso]
        geom = unary_union(geoms).simplify(TOLERANCE_DEG, preserve_topology=True)
        g = mapping(geom)
        features.append({"type": "Feature", "properties": {"ine_code": code, "name": name},
                         "geometry": {"type": g["type"], "coordinates": _round(g["coordinates"])}})
    return {"type": "FeatureCollection", "features": features}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("source", nargs="?", help="local Natural Earth admin-1 GeoJSON")
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    args = parser.parse_args()
    if args.source:
        source = json.loads(Path(args.source).read_text(encoding="utf-8"))
    else:
        with urllib.request.urlopen(SOURCE_URL, timeout=120) as resp:
            source = json.loads(resp.read())
    out = build(source)
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")) + "\n",
                              encoding="utf-8")
    print(f"{len(out['features'])} provinces -> {args.out}")


if __name__ == "__main__":
    main()
