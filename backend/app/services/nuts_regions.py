"""NUTS 2 region of a place from official codes (no geometry).

Spain: the INE municipality code's first two digits are the province, and each province lies in
one NUTS 2 region. GISCO boundary geometries are not used: their licence excludes commercial use.
"""
from __future__ import annotations

import functools
import gzip
import json
import re
from pathlib import Path

import yaml

PROVINCE_TABLE_PATH = (Path(__file__).resolve().parents[2] / "data" / "eurostat"
                       / "nuts2_es_provinces.yaml")
_INE_MUNICIPALITY = re.compile(r"^[0-9]{5}$")


def load_province_table(path: Path | None = None) -> dict[str, str]:
    """INE province code (two digits, as text) -> NUTS 2 code."""
    doc = yaml.safe_load((path or PROVINCE_TABLE_PATH).read_text(encoding="utf-8"))
    table = {str(k): str(v) for k, v in (doc.get("provinces") or {}).items()}
    if not table:
        raise ValueError("empty province table")
    return table


def nuts2_for_municipality(ine_code: str, table: dict[str, str]) -> str | None:
    """NUTS 2 of a Spanish municipality (5-digit INE code), or None if malformed or unknown."""
    if not isinstance(ine_code, str) or not _INE_MUNICIPALITY.match(ine_code):
        return None
    return table.get(ine_code[:2])

EUROSTAT_DIR = Path(__file__).resolve().parents[2] / "data" / "eurostat"


def region_names(jsonstat: dict) -> dict[str, str]:
    """NUTS 2 code -> name from a JSON-stat document's `geo` labels; Extra-Regio (`..ZZ`) dropped."""
    category = jsonstat["dimension"]["geo"]["category"]
    labels = category.get("label") or {}
    return {code: labels.get(code, code) for code in category["index"]
            if len(code) == 4 and not code.endswith("ZZ")}


@functools.lru_cache(maxsize=1)
def load_region_names(eurostat_dir: Path | None = None) -> dict[str, str]:
    """Names from the newest committed apro_cpshr snapshot (the one the graph was seeded from)."""
    path = max((eurostat_dir or EUROSTAT_DIR).glob("apro_cpshr_nuts2_*.json.gz"), default=None)
    if path is None:
        raise FileNotFoundError("no apro_cpshr snapshot in the image")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return region_names(json.load(f)["jsonstat"])
