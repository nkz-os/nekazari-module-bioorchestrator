"""NUTS 2 region of a place from official codes (no geometry).

Spain: the INE municipality code's first two digits are the province, and each province lies in
one NUTS 2 region. GISCO boundary geometries are not used: their licence excludes commercial use.
"""
from __future__ import annotations

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
