"""Eurostat regional crop statistics (apro_cpshr) → annual yield series per region and crop.

Pure functions, no I/O. Eurostat publishes area and harvested production by NUTS 2 region but
leaves the yield variable empty at that level, so the yield is derived as production / area
and labelled as such. Values stay in EU standard humidity (the dataset's basis): nothing is
converted to another moisture.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from urllib.parse import urlencode

AREA = "AR_THS_HA"                 # thousand hectares
PRODUCTION = "HPRD_HUMD_EU_THS_T"  # thousand tonnes, EU standard humidity
HUMIDITY = "HUMD_EU_PC"            # EU standard humidity, %
YIELD_METHOD = "production_over_area"
API_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/apro_cpshr"


@dataclass(frozen=True)
class Cell:
    crop: str
    strucpro: str
    geo: str
    year: int
    value: float | None
    flag: str | None


@dataclass(frozen=True)
class YieldRow:
    nuts: str
    eurostat_crop: str
    eppo: str
    year: int
    area_ha: float | None
    production_t: float | None
    humidity_pct: float | None
    yield_t_ha: float | None
    flags: tuple[str, ...]


def _codes(doc: dict, dim: str) -> list[str]:
    index = doc["dimension"][dim]["category"]["index"]
    if isinstance(index, list):
        return list(index)
    return [code for code, _pos in sorted(index.items(), key=lambda kv: kv[1])]


def parse_jsonstat(doc: dict) -> list[Cell]:
    """Cells of a JSON-stat 2.0 dataset with dimensions freq, crops, strucpro, geo, time."""
    ids: list[str] = doc["id"]
    sizes: list[int] = doc["size"]
    expected = {"crops", "strucpro", "geo", "time"}
    if not expected <= set(ids):
        raise ValueError(f"unexpected dimensions {ids}")
    extra = [dim for dim, size in zip(ids, sizes) if dim not in expected and size > 1]
    if extra:
        raise ValueError(f"unexpected multi-valued dimensions {extra}")
    codes = {dim: _codes(doc, dim) for dim in ids}
    strides = [1] * len(ids)
    for i in range(len(ids) - 2, -1, -1):
        strides[i] = strides[i + 1] * sizes[i + 1]

    values = doc.get("value") or {}
    if isinstance(values, list):
        values = {str(i): v for i, v in enumerate(values) if v is not None}
    status = doc.get("status") or {}
    if isinstance(status, list):
        status = {str(i): s for i, s in enumerate(status) if s}

    cells: list[Cell] = []
    for key in sorted({*values, *status}, key=int):
        flat = int(key)
        pos = {}
        for dim, stride, size in zip(ids, strides, sizes):
            pos[dim] = (flat // stride) % size
        value = values.get(key)
        cells.append(Cell(
            crop=codes["crops"][pos["crops"]],
            strucpro=codes["strucpro"][pos["strucpro"]],
            geo=codes["geo"][pos["geo"]],
            year=int(codes["time"][pos["time"]]),
            value=float(value) if value is not None else None,
            flag=status.get(key) or None,
        ))
    return cells


def build_series(cells: list[Cell], crop_map: dict[str, str]) -> list[YieldRow]:
    """Annual rows per (region, crop, year) for the crops in `crop_map` (Eurostat code → EPPO).

    Groups with neither area nor production (only the standard humidity) are dropped.
    """
    groups: dict[tuple[str, str, int], dict[str, Cell]] = {}
    for c in cells:
        if c.crop in crop_map and c.strucpro in (AREA, PRODUCTION, HUMIDITY):
            groups.setdefault((c.geo, c.crop, c.year), {})[c.strucpro] = c
    rows: list[YieldRow] = []
    for (geo, crop, year), by_var in sorted(groups.items()):
        area = by_var.get(AREA)
        prod = by_var.get(PRODUCTION)
        hum = by_var.get(HUMIDITY)
        area_ha = area.value * 1000 if area and area.value is not None else None
        prod_t = prod.value * 1000 if prod and prod.value is not None else None
        if area_ha is None and prod_t is None:
            continue  # humidity-only cell: no statistic for that region and year
        yield_t_ha = prod_t / area_ha if area_ha and prod_t is not None and area_ha > 0 else None
        flags = tuple(sorted({f"{k}:{v.flag}" for k, v in by_var.items() if v.flag}))
        rows.append(YieldRow(
            nuts=geo, eurostat_crop=crop, eppo=crop_map[crop], year=year,
            area_ha=area_ha, production_t=prod_t,
            humidity_pct=hum.value if hum else None,
            yield_t_ha=yield_t_ha, flags=flags,
        ))
    return rows


DATASET = "apro_cpshr"
SOURCE_ID = "eurostat_apro_cpshr"
# Eurostat does not state the NUTS version of apro_cpshr; codes of several versions coexist.
NUTS_VERSION_NOTE = "as published by Eurostat (version not stated; codes of several NUTS versions)"


def request_url(crop_codes: list[str]) -> str:
    """API request for every NUTS 2 region, the given crops, and area/production/humidity."""
    if not crop_codes:
        raise ValueError("no crop codes")
    params = [("format", "JSON"), ("lang", "EN"), ("geoLevel", "nuts2")]
    params += [("crops", c) for c in sorted(crop_codes)]
    params += [("strucpro", s) for s in (AREA, PRODUCTION, HUMIDITY)]
    return f"{API_URL}?{urlencode(params)}"


def to_node_props(row: YieldRow, *, dataset_updated: str | None, extracted_at: str,
                  nuts_version: str) -> dict:
    """Flat properties for a (:RegionalYieldStat) node: one region, crop and year."""
    return {
        "key": f"{DATASET}|{row.nuts}|{row.eurostat_crop}|{row.year}",
        "dataset": DATASET,
        "sourceId": SOURCE_ID,
        "nuts": row.nuts,
        "nutsLevel": 2,
        "nutsVersion": nuts_version,
        "eurostatCrop": row.eurostat_crop,
        "eppo": row.eppo,
        "year": row.year,
        "areaHa": row.area_ha,
        "productionT": row.production_t,
        "humidityBasis": "eu_standard",
        "humidityPct": row.humidity_pct,
        "yieldTHa": row.yield_t_ha,
        "yieldMethod": YIELD_METHOD,
        "flags": list(row.flags),
        "datasetUpdated": dataset_updated,
        "extractedAt": extracted_at,
    }


def yields_payload(nodes: list[dict], recent: int) -> dict:
    """API body: the annual series plus the median of the last `recent` years with a yield."""
    if not nodes:
        raise ValueError("no rows")
    if recent < 1:
        raise ValueError("recent must be >= 1")
    ordered = sorted(nodes, key=lambda n: n["year"])
    series = [{
        "year": n["year"], "area_ha": n.get("areaHa"), "production_t": n.get("productionT"),
        "yield_t_ha": n.get("yieldTHa"), "humidity_pct": n.get("humidityPct"),
        "flags": list(n.get("flags") or []),
    } for n in ordered]
    last = [s for s in series if s["yield_t_ha"] is not None][-recent:]
    first = ordered[0]
    return {
        "eppo": first["eppo"],
        "nuts": first["nuts"],
        "dataset": first["dataset"],
        "eurostat_crop": first["eurostatCrop"],
        "humidity_basis": first["humidityBasis"],
        "yield_method": first["yieldMethod"],
        "nuts_version": first.get("nutsVersion"),
        "dataset_updated": first.get("datasetUpdated"),
        "extracted_at": first.get("extractedAt"),
        "series": series,
        "recent_median_t_ha": statistics.median(s["yield_t_ha"] for s in last) if last else None,
        "recent_years": [s["year"] for s in last],
    }
