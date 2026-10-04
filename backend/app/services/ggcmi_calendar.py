"""Typical sowing/maturity day per crop at a point, from the GGCMI Phase 3 crop calendar.

Source: GGCMI Phase 3 crop calendar v1.01 (Jägermeyr et al. 2021), Zenodo
record 5062513, doi:10.5281/zenodo.5062513, licensed CC BY 4.0. The data file
``backend/data/ggcmi_calendar_europe.json`` is built offline by
``scripts/build_ggcmi_calendar.py`` (0.5° grid cropped to Europe).

The calendar gives one TYPICAL day per grid cell (no range): callers must not
turn it into a sowing window. Only crops with a defensible GGCMI counterpart are
mapped; any other crop has no calendar here. Standard library only.
"""

from __future__ import annotations

import datetime
import functools
import json
import math
from pathlib import Path

# backend/data locally; /app/data in the image (Dockerfile: COPY backend/data/ data/).
DEFAULT_PATH = Path(__file__).resolve().parents[2] / "data" / "ggcmi_calendar_europe.json"
# GGCMI ``data_source_used`` index -> dataset name. Only these are served:
# MIRCA2000 (1) gives coarse national month-start values that are wrong for most
# of the EU, and other indexes are non-European sets. The build drops the rest;
# the reader re-checks against this constant, never against the data file.
ALLOWED_SOURCES = {2: "SAGE", 3: "Iizumi et al. 2019", 4: "RiceAtlas", 5: "Dimou et al. 2018"}


def citation(dataset: str, rainfed_fallback: bool = False) -> str:
    """Short citation naming the dataset GGCMI took the cell from (and a rainfed stand-in)."""
    note = "; rainfed calendar used for irrigated parcel" if rainfed_fallback else ""
    return (f"GGCMI Phase 3 crop calendar ({dataset}{note}), Jägermeyr et al. 2021, CC BY 4.0, "
            "doi:10.5281/zenodo.5062513")

# EPPO code -> GGCMI crop code. Oats, triticale, chickpea, lentil, vetches,
# grass pea, lucerne and horticulture have no GGCMI counterpart; GGCMI ``bea``
# is dry Phaseolus beans, not faba bean, so it is deliberately not mapped.
EPPO_TO_GGCMI = {
    "HORVX": "bar", "ZEAMX": "mai", "HELAN": "sun", "BRSNN": "rap", "BRPNA": "rap",
    "SECCE": "rye", "SOLTU": "pot", "PISSA": "pea", "PIBSX": "pea", "BETVU": "sgb",
    "GLXMA": "soy", "ORYSA": "ri1", "TRZAX": "wwh", "TRZAW": "wwh", "TRZDU": "wwh",
}
# Where the winter-wheat cell has no data, spring wheat is the wheat calendar there.
WHEAT_FALLBACK = {"wwh": "swh"}


@functools.lru_cache(maxsize=1)
def _default() -> dict:
    return json.loads(DEFAULT_PATH.read_text(encoding="utf-8"))


def load(path: Path | None = None) -> dict:
    if path is None:
        return _default()
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _regime(irrigation: str | None) -> str:
    return "ir" if irrigation == "regadío" else "rf"


def _cell(grid: dict, lat: float, lon: float) -> tuple[int, int] | None:
    row = math.floor((grid["lat_north"] - lat) / grid["res"])
    col = math.floor((lon - grid["lon_west"]) / grid["res"])
    if 0 <= row < grid["nrows"] and 0 <= col < grid["ncols"]:
        return row, col
    return None


def _value(rows: list, row: int, col: int) -> int | None:
    """Cell of a trimmed row ``[first_col, values]`` (cells outside the span have no data)."""
    first, values = rows[row]
    i = col - first
    return values[i] if 0 <= i < len(values) else None


_DROPPED = object()  # the cell has a calendar in GGCMI but from an excluded source


def _read(layer: dict | None, row: int, col: int) -> dict | None | object:
    """Kept cell as a result dict, ``_DROPPED`` for an excluded source, None for no calendar."""
    if layer is None:
        return None
    index = _value(layer["data_source"], row, col)
    if index is None:
        return None
    dataset = ALLOWED_SOURCES.get(index)
    planting = _value(layer["planting_day"], row, col)
    if dataset is None or planting is None:
        return _DROPPED
    return {"planting_doy": planting,
            "maturity_doy": _value(layer["maturity_day"], row, col),
            "cycle_days": _value(layer["growing_season_length"], row, col),
            "dataset": dataset}


def lookup(eppo: str, lat: float | None, lon: float | None, irrigation: str | None = None,
           *, data: dict | None = None) -> dict | None:
    """``{planting_doy, maturity_doy, cycle_days, source, layer, rainfed_fallback}`` or None.

    ``irrigation`` ``"regadío"`` reads the irrigated layer and, when it has no
    allowed value, the rainfed one (``layer`` ``"rf"``, ``rainfed_fallback``
    True, also noted in ``source``); ``"secano"`` or unknown reads only the
    rainfed layer. Wheat order: winter wheat (ir, rf), then spring wheat only
    where winter wheat has no calendar at all. None when the crop is unmapped, the point
    lies outside the grid, the cell has no data (e.g. sea) or its source was
    excluded (MIRCA2000). ``source`` names the dataset of the cell.
    """
    crop = EPPO_TO_GGCMI.get(eppo)
    if crop is None or lat is None or lon is None:
        return None
    lat, lon = float(lat), float(lon)
    if not (math.isfinite(lat) and math.isfinite(lon)):
        return None
    doc = _default() if data is None else data
    cell = _cell(doc["grid"], lat, lon)
    if cell is None:
        return None
    regime = _regime(irrigation)
    # Irrigated parcel: irrigated layer, else the rainfed one (a reliable rainfed
    # value beats no calendar, even where the irrigated cell was dropped).
    regimes = ("ir", "rf") if regime == "ir" else ("rf",)
    for code in (crop, WHEAT_FALLBACK.get(crop)):
        if code is None:
            continue
        dropped = False
        for layer_regime in regimes:
            found = _read(doc["layers"].get(f"{code}_{layer_regime}"), *cell)
            if found is _DROPPED:
                dropped = True
                continue
            if found is not None:
                fallback = layer_regime != regime
                return {"planting_doy": found["planting_doy"], "maturity_doy": found["maturity_doy"],
                        "cycle_days": found["cycle_days"],
                        "source": citation(found["dataset"], rainfed_fallback=fallback),
                        "layer": layer_regime, "rainfed_fallback": fallback}
        if dropped:
            # A dropped winter-wheat cell is still winter wheat: spring wheat must
            # not stand in for it. The fallback is only for cells with no calendar.
            return None
    return None


def sowing_type_from_doy(doy: int) -> str:
    """Sowing season of a planting day: Sep-Dec autumn, Jan-May spring, Jun-Aug summer."""
    day = min(max(int(doy), 1), 365)  # non-leap calendar
    month = (datetime.date(2001, 1, 1) + datetime.timedelta(days=day - 1)).month
    if month >= 9:
        return "autumn"
    if month <= 5:
        return "spring"
    return "summer"
