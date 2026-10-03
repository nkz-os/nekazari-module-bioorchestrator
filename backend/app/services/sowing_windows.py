"""Curated sowing windows per crop × Köppen class × sowing type.

Every row must cite its source: the trials carry no sowing date, so this table is
the only basis for the calendar view. Rows are added only after the source is
validated; an empty table is valid and makes the recommender fall back to the
coarse season slot.
"""

from __future__ import annotations

import functools
import json
from pathlib import Path

# backend/data locally; /app/data in the image (Dockerfile: COPY backend/data/ data/).
DEFAULT_PATH = Path(__file__).resolve().parents[2] / "data" / "sowing_windows.json"
REQUIRED = ("eppo", "sowing_type", "koppen", "start_month", "end_month", "source")
SOWING_TYPES = {"autumn", "spring", "summer", "perennial"}


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
