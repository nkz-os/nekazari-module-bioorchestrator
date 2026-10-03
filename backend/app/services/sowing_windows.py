"""Curated sowing windows per crop × Köppen class × sowing type (× country).

Every row must cite its source: the trials carry no sowing date, so this table is
the only basis for the calendar view. Rows are added only after the source is
validated; an empty table is valid and makes the recommender fall back to the
coarse season slot. Practice differs by country within one climate class, so a
row may carry ``countries`` (ISO 3166 alpha-2 list); it then applies only there.
"""

from __future__ import annotations

import functools
import json
import re
from pathlib import Path

# backend/data locally; /app/data in the image (Dockerfile: COPY backend/data/ data/).
DEFAULT_PATH = Path(__file__).resolve().parents[2] / "data" / "sowing_windows.json"
REQUIRED = ("eppo", "sowing_type", "koppen", "start_month", "end_month", "source")
SOWING_TYPES = {"autumn", "spring", "summer", "perennial"}
_ISO2 = re.compile(r"^[A-Z]{2}$")


def _validate(rows: list) -> list[dict]:
    for i, row in enumerate(rows):
        for field in REQUIRED:
            if field not in row:
                raise ValueError(f"row {i}: missing {field}")
        if row["sowing_type"] not in SOWING_TYPES:
            raise ValueError(f"row {i}: sowing_type {row['sowing_type']!r}")
        for field in ("start_month", "end_month"):
            if not isinstance(row[field], int) or not 1 <= row[field] <= 12:
                raise ValueError(f"row {i}: {field} out of 1-12")
        if not isinstance(row["koppen"], list) or not row["koppen"]:
            raise ValueError(f"row {i}: koppen must be a non-empty list")
        if "countries" in row:
            countries = row["countries"]
            if not isinstance(countries, list) or not countries or not all(
                isinstance(c, str) and _ISO2.match(c) for c in countries
            ):
                raise ValueError(f"row {i}: countries must be a non-empty list of ISO alpha-2 codes")
        if not str(row["source"]).strip():
            raise ValueError(f"row {i}: empty source")
    return rows


@functools.lru_cache(maxsize=1)
def _default_rows() -> tuple:
    return tuple(_validate(json.loads(DEFAULT_PATH.read_text())["rows"]))


def load_rows(path: Path | None = None) -> list[dict]:
    if path is None:
        return list(_default_rows())
    return _validate(json.loads(Path(path).read_text())["rows"])
