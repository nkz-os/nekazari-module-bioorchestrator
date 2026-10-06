"""Mandatory source attributions attached to API responses (licence compliance).

Some sources only allow reuse with a prescribed credit line. Every response that carries
numbers derived from a source adds ``attributions`` for the sources actually present in it:
``[{source_id, text, url, licence_id, licence_url}]`` (see ``app.common.source_registry``).
The key is always present (an empty list when no source with an attribution is involved).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from app.common.source_registry import get_attributions


def attach_attributions(payload: dict[str, Any], source_ids: Iterable[str | None]) -> dict[str, Any]:
    """Copy of ``payload`` with ``attributions`` for the given source ids.

    A new dict is returned so a cached DAO result is never mutated.
    """
    return {**payload, "attributions": get_attributions(source_ids)}


def recommendation_source_ids(result: dict[str, Any]) -> set[str]:
    """Sources behind a recommendation response (``evidence.sources`` of each recommendation)."""
    return {
        sid
        for rec in result.get("recommendations") or []
        for sid in (rec.get("evidence") or {}).get("sources") or []
        if sid
    }


def rows_source_ids(rows: Iterable[dict[str, Any]] | None, key: str = "source_id") -> set[str]:
    """Distinct source ids of a list of rows, each carrying one id under ``key``."""
    return {row[key] for row in rows or [] if row.get(key)}
