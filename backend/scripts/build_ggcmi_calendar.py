"""Build backend/data/ggcmi_calendar_europe.json from the GGCMI Phase 3 crop calendar (offline tool).

Source: GGCMI Phase 3 crop calendar, version 1.01
  Zenodo record 5062513, doi:10.5281/zenodo.5062513
  https://zenodo.org/records/5062513
License: CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/).
Citation: Jägermeyr, J., Müller, C., Ruane, A. C., et al. (2021). Climate impacts
  on global agriculture emerge earlier in new generation of climate and crop
  models. Nature Food 2, 873-885. Crop calendar dataset doi:10.5281/zenodo.5062513.

Each ``<crop>_<rf|ir>_ggcmi_crop_calendar_phase3_v1.01.nc4`` file is a 0.5°
global grid with ``planting_day`` (day of year), ``maturity_day`` (day of year)
and ``growing_season_length`` (days). Only the crops the recommender maps
(see ``app.services.ggcmi_calendar.EPPO_TO_GGCMI``) are kept, cropped to a
Europe box (lon -32..45, lat 27..72: includes the Canary Islands, the Azores
and Cyprus). Cell coordinates come from the netCDF ``lat``/``lon`` variables,
not from the raster transform; the grid is checked to be a regular 0.5°
north-up grid aligned on the box edges.

Source filter: every cell carries ``data_source_used`` (1 MIRCA2000, 2 SAGE,
3 Iizumi et al. 2019, 4 RiceAtlas, 5 Dimou et al. 2018, 6+ non-European
national sets). Only cells from sources 2-5 are kept; a cell from MIRCA2000
(1), any other index or no index is set to ``null`` in all three variables.
Reason: MIRCA2000 gives coarse, national month-start values (e.g. rainfed
barley sown on 1 May at Madrid, Paris, Warsaw and Dublin alike, and a generic
1 Nov / 23 Jun wheat-like calendar) that are wrong for most of the EU, and a
wrong typical day is worse than the season-slot fallback. The source index of
every cell that has a calendar in the file is stored as a fourth array,
``data_source`` (0 = no index), dropped cells included: the service names the
underlying dataset of a kept cell and can tell a dropped cell from a cell
without any calendar.

Output: one JSON file with the grid origin/resolution and, per ``<crop>_<rf|ir>``
layer, four arrays of grid rows (the three variables and ``data_source``) (row 0 = northernmost row). Each row is
``[first_col, [values...]]``: the leading and trailing cells without data are
dropped (``first_col`` = column of the first kept value) and ``null`` marks a
cell without data inside the kept span. A row with no data is ``[0, []]``.
Trimming the sea at both ends keeps the file near 2.3 MB while it stays plain,
reviewable JSON.

Build-time only: needs ``rasterio`` and ``numpy``; the service reads the JSON
with the standard library.

Usage:
    python scripts/build_ggcmi_calendar.py [--cache DIR] [--out PATH]
Files are downloaded from Zenodo into ``--cache`` and their md5 checked
against the checksums published by the Zenodo API.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import urllib.request
import warnings
from pathlib import Path

import numpy as np
import rasterio

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.ggcmi_calendar import ALLOWED_SOURCES, EPPO_TO_GGCMI, WHEAT_FALLBACK  # noqa: E402

RECORD_API = "https://zenodo.org/api/records/5062513"
VERSION = "1.01"
FILE_TEMPLATE = "{crop}_{regime}_ggcmi_crop_calendar_phase3_v1.01.nc4"
REGIMES = ("rf", "ir")
VARIABLES = ("planting_day", "maturity_day", "growing_season_length")
SOURCE_VAR = "data_source_used"
# data_source_used index -> dataset name (the service's constant); others (1 = MIRCA2000) are dropped.
BBOX = {"lon_west": -32.0, "lon_east": 45.0, "lat_south": 27.0, "lat_north": 72.0}
RES = 0.5
DEFAULT_OUT = Path(__file__).resolve().parents[1] / "data" / "ggcmi_calendar_europe.json"
METADATA = {
    "source": "GGCMI Phase 3 crop calendar",
    "version": VERSION,
    "doi": "10.5281/zenodo.5062513",
    "url": "https://zenodo.org/records/5062513",
    "license": "CC BY 4.0",
    "citation": "Jägermeyr et al. (2021), Nature Food 2, 873-885; "
                "GGCMI Phase 3 crop calendar v1.01, doi:10.5281/zenodo.5062513",
}


def needed_crops() -> list[str]:
    return sorted(set(EPPO_TO_GGCMI.values()) | set(WHEAT_FALLBACK.values()))


def _md5(path: Path) -> str:
    h = hashlib.md5()  # noqa: S324 - integrity check against Zenodo's published md5
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(cache: Path, names: list[str]) -> dict[str, Path]:
    with urllib.request.urlopen(RECORD_API, timeout=60) as resp:  # noqa: S310 - fixed https URL
        record = json.load(resp)
    files = {f["key"]: f for f in record["files"]}
    cache.mkdir(parents=True, exist_ok=True)
    out = {}
    for name in names:
        meta = files[name]
        algo, expected = meta["checksum"].split(":", 1)
        if algo != "md5":
            raise ValueError(f"{name}: unexpected checksum algorithm {algo}")
        path = cache / name
        if not path.exists() or _md5(path) != expected:
            with urllib.request.urlopen(meta["links"]["self"], timeout=120) as resp:  # noqa: S310
                path.write_bytes(resp.read())
        got = _md5(path)
        if got != expected:
            raise ValueError(f"{name}: md5 {got} != {expected}")
        out[name] = path
    return out


def _read_var(path: Path, var: str) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", rasterio.errors.NotGeoreferencedWarning)
        with rasterio.open(f"netcdf:{path}:{var}") as ds:
            arr = ds.read(1).astype("float64")
            if ds.nodata is not None:
                arr[arr == ds.nodata] = np.nan
    return arr


def _axes(path: Path) -> tuple[np.ndarray, np.ndarray]:
    lat = _read_var(path, "lat").ravel()
    lon = _read_var(path, "lon").ravel()
    if not (np.allclose(np.diff(lat), -RES) and np.allclose(np.diff(lon), RES)):
        raise ValueError(f"{path.name}: not a regular north-up {RES}° grid")
    return lat, lon


def crop_window(lat: np.ndarray, lon: np.ndarray) -> tuple[slice, slice]:
    rows = np.where((lat < BBOX["lat_north"]) & (lat > BBOX["lat_south"]))[0]
    cols = np.where((lon > BBOX["lon_west"]) & (lon < BBOX["lon_east"]))[0]
    rs, cs = slice(rows[0], rows[-1] + 1), slice(cols[0], cols[-1] + 1)
    # Cell centres must sit half a cell inside the box edges.
    if not (math.isclose(lat[rs][0], BBOX["lat_north"] - RES / 2)
            and math.isclose(lon[cs][0], BBOX["lon_west"] + RES / 2)):
        raise ValueError("grid not aligned on the box edges")
    return rs, cs


def _trim(values: list[int | None]) -> list:
    kept = [i for i, v in enumerate(values) if v is not None]
    if not kept:
        return [0, []]
    return [kept[0], values[kept[0]:kept[-1] + 1]]


def _to_rows(arr: np.ndarray) -> list[list]:
    return [_trim([None if math.isnan(v) else int(round(v)) for v in row]) for row in arr]


def build(paths: dict[str, Path]) -> dict:
    layers = {}
    grid = None
    for crop in needed_crops():
        for regime in REGIMES:
            path = paths[FILE_TEMPLATE.format(crop=crop, regime=regime)]
            lat, lon = _axes(path)
            rs, cs = crop_window(lat, lon)
            this_grid = {**{k: BBOX[k] for k in ("lat_north", "lon_west")}, "res": RES,
                         "nrows": int(rs.stop - rs.start), "ncols": int(cs.stop - cs.start)}
            if grid is None:
                grid = this_grid
            elif grid != this_grid:
                raise ValueError(f"{path.name}: grid differs from the other files")
            source = _read_var(path, SOURCE_VAR)[rs, cs]
            keep = np.isin(source, list(ALLOWED_SOURCES))
            layer = {}
            for var in VARIABLES:
                arr = _read_var(path, var)[rs, cs]
                layer[var] = _to_rows(np.where(keep, arr, np.nan))
            # Index of every cell that has a calendar in the file, kept or not (0 = no index),
            # so the service can tell "dropped" from "no calendar" (wheat fallback).
            planted = ~np.isnan(_read_var(path, "planting_day")[rs, cs])
            layer["data_source"] = _to_rows(np.where(planted, np.nan_to_num(source, nan=0.0), np.nan))
            layers[f"{crop}_{regime}"] = layer
    return {**METADATA, "data_sources": {str(k): v for k, v in ALLOWED_SOURCES.items()},
            "grid": grid, "layers": layers}


def write(doc: dict, out: Path) -> None:
    # One grid row per line: compact, yet a diff still points at the cell row.
    lines = ["{"]
    head = {k: v for k, v in doc.items() if k != "layers"}
    for k, v in head.items():
        lines.append(f"{json.dumps(k)}: {json.dumps(v, ensure_ascii=False)},")
    lines.append('"layers": {')
    layer_items = list(doc["layers"].items())
    for li, (name, variables) in enumerate(layer_items):
        lines.append(f"{json.dumps(name)}: {{")
        var_items = list(variables.items())
        for vi, (var, rows) in enumerate(var_items):
            body = ",\n".join(json.dumps(r, separators=(",", ":")) for r in rows)
            lines.append(f"{json.dumps(var)}: [\n{body}\n]" + ("," if vi < len(var_items) - 1 else ""))
        lines.append("}" + ("," if li < len(layer_items) - 1 else ""))
    lines.append("}")
    lines.append("}")
    text = "\n".join(lines) + "\n"
    json.loads(text)  # fail before writing a broken file
    out.write_text(text, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cache", type=Path, default=Path("/tmp/ggcmi_cache"))
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    names = [FILE_TEMPLATE.format(crop=c, regime=r) for c in needed_crops() for r in REGIMES]
    doc = build(fetch(args.cache, names))
    write(doc, args.out)
    print(f"wrote {args.out} ({args.out.stat().st_size} bytes, {len(doc['layers'])} layers)")


if __name__ == "__main__":
    main()
