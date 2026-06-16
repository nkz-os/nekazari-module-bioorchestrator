"""Canonical source registry for BioOrchestrator trial data sources.

Provides a singleton registry loaded from sources_registry.json with
attribution/disclaimer text in multiple locales.

Usage:
    from app.common.source_registry import get_source, get_attribution

    src = get_source("NAVARRA-AGRARIA")
    text = get_attribution("NAVARRA-AGRARIA", locale="es")
"""

from __future__ import annotations

import json
import logging
import os
from functools import cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Path resolution ──────────────────────────────────────────────────────────

REGISTRY_PATH = os.getenv(
    "SOURCES_REGISTRY_PATH",
    str(Path(__file__).resolve().parent.parent.parent / "data" / "sources_registry.json"),
)


# ── Type hint ────────────────────────────────────────────────────────────────

SourceInfo = dict[str, Any]


# ── Loader (cached) ──────────────────────────────────────────────────────────

@cache
def _load_registry() -> list[SourceInfo]:
    """Load and cache the source registry from JSON."""
    path = Path(REGISTRY_PATH)
    if not path.exists():
        logger.warning("Sources registry not found at %s — returning empty", path)
        return []
    with open(path) as f:
        data: list[SourceInfo] = json.load(f)
    logger.info("Loaded %d sources from registry", len(data))
    return data


def _build_index() -> dict[str, SourceInfo]:
    """Build source_id -> entry index."""
    return {s["source_id"]: s for s in _load_registry()}


# ── Public API ───────────────────────────────────────────────────────────────

def get_source(source_id: str) -> SourceInfo:
    """Return the full source entry for a given source_id.

    Raises KeyError if source_id is not found.
    """
    index = _build_index()
    if source_id not in index:
        raise KeyError(f"Unknown source_id: {source_id}")
    return index[source_id]


def get_attribution(source_id: str, locale: str = "en") -> str:
    """Return attribution text in the requested locale.

    Falls back to 'en' if the locale is not available, then to
    the first available locale if even 'en' is missing.
    """
    src = get_source(source_id)
    attr = src.get("attribution", {})
    return _resolve_localized(attr, locale)


def get_disclaimer(source_id: str, locale: str = "en") -> str:
    """Return disclaimer text in the requested locale."""
    src = get_source(source_id)
    disc = src.get("disclaimer", {})
    return _resolve_localized(disc, locale)


def all_sources() -> list[SourceInfo]:
    """Return the full list of all sources."""
    return list(_load_registry())


def all_source_ids() -> list[str]:
    """Return all registered source_id values."""
    return list(_build_index().keys())


def sources_by_license(license_class: str) -> list[SourceInfo]:
    """Return sources that match a given license_class."""
    return [s for s in _load_registry() if s.get("license_class") == license_class]


def get_combined_attribution(source_ids: list[str], locale: str = "en") -> str:
    """Join attribution text from multiple sources into a single string.

    Used when a UI component displays data from multiple sources
    (e.g. Variety Finder results).
    """
    texts: list[str] = []
    for sid in source_ids:
        try:
            texts.append(get_attribution(sid, locale))
        except KeyError:
            logger.warning("Unknown source_id %s in combined attribution", sid)
    if not texts:
        return ""
    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for t in texts:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return " ".join(unique)


def get_combined_disclaimer(source_ids: list[str], locale: str = "en") -> str:
    """Join disclaimer text from multiple sources."""
    texts: list[str] = []
    for sid in source_ids:
        try:
            texts.append(get_disclaimer(sid, locale))
        except KeyError:
            pass
    if not texts:
        return ""
    seen: set[str] = set()
    unique: list[str] = []
    for t in texts:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return " ".join(unique)


# ── Internal helpers ─────────────────────────────────────────────────────────

def _resolve_localized(texts: dict[str, str], locale: str) -> str:
    """Resolve localized text with fallback chain."""
    if locale in texts:
        return texts[locale]
    if "en" in texts:
        return texts["en"]
    # Last resort: first available value
    if texts:
        return next(iter(texts.values()))
    return ""
