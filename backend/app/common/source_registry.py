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
from collections.abc import Iterable
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
    with open(path, encoding="utf-8") as f:
        data: list[SourceInfo] = json.load(f)
    logger.info("Loaded %d sources from registry", len(data))
    return data


@cache
def _build_index() -> dict[str, SourceInfo]:
    """Build source_id -> entry index."""
    return {s["source_id"]: s for s in _load_registry()}


# ── Public API ───────────────────────────────────────────────────────────────

def get_source(source_id: str) -> SourceInfo:
    """Return the full source entry for a given source_id.

    Args:
        source_id: The unique identifier for the source
            (e.g., "NAVARRA-AGRARIA", "GENVCE").

    Returns:
        The full source metadata dictionary.

    Raises:
        KeyError: If source_id is not found in the registry.
    """
    index = _build_index()
    if source_id not in index:
        raise KeyError(f"Unknown source_id: {source_id}")
    return index[source_id]


def get_attribution(source_id: str, locale: str = "en") -> str:
    """Return attribution text in the requested locale.

    Falls back to 'en' if the locale is not available, then to
    the first available locale if even 'en' is missing.

    Args:
        source_id: The unique identifier for the source.
        locale: BCP 47 language tag (e.g., "en", "es", "fr").

    Returns:
        Attribution text string. Empty string if no text is available
        for any locale.
    """
    src = get_source(source_id)
    attr = src.get("attribution", {})
    return _resolve_localized(attr, locale)


def get_disclaimer(source_id: str, locale: str = "en") -> str:
    """Return disclaimer text in the requested locale.

    Falls back to 'en' if the locale is not available, then to
    the first available locale if even 'en' is missing.

    Args:
        source_id: The unique identifier for the source.
        locale: BCP 47 language tag (e.g., "en", "es", "fr").

    Returns:
        Disclaimer text string. Empty string if no text is available
        for any locale.
    """
    src = get_source(source_id)
    disc = src.get("disclaimer", {})
    return _resolve_localized(disc, locale)


def all_sources() -> list[SourceInfo]:
    """Return the full list of all registered sources.

    Returns:
        List of all source metadata dictionaries. Empty list
        if the registry file is not found.
    """
    return list(_load_registry())


def all_source_ids() -> list[str]:
    """Return all registered source_id values.

    Returns:
        List of source_id strings (e.g., "NAVARRA-AGRARIA",
        "GENVCE", ...). Empty list if the registry is empty.
    """
    return list(_build_index().keys())


def sources_by_license(license_class: str) -> list[SourceInfo]:
    """Return sources that match a given license_class.

    Args:
        license_class: The license class to filter by
            (e.g., "public-sector-psi", "editorial-restricted").

    Returns:
        List of sources with the matching license_class.
    """
    return [s for s in _load_registry() if s.get("license_class") == license_class]


def get_combined_attribution(source_ids: list[str], locale: str = "en") -> str:
    """Join attribution text from multiple sources into a single string.

    Used when a UI component displays data from multiple sources
    (e.g. Variety Finder results).

    Args:
        source_ids: List of source_id strings to combine.
        locale: BCP 47 language tag for localization.

    Returns:
        Space-joined attribution string with deduplicated texts.
        Empty string if no sources are valid.
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
    """Join disclaimer text from multiple sources.

    Args:
        source_ids: List of source_id strings to combine.
        locale: BCP 47 language tag for localization.

    Returns:
        Space-joined disclaimer string with deduplicated texts.
        Empty string if no sources are valid.
    """
    texts: list[str] = []
    for sid in source_ids:
        try:
            texts.append(get_disclaimer(sid, locale))
        except KeyError:
            logger.warning("Unknown source_id %s in combined disclaimer", sid)
    if not texts:
        return ""
    seen: set[str] = set()
    unique: list[str] = []
    for t in texts:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return " ".join(unique)


# ── Mandatory attribution (licence compliance) ───────────────────────────────

# Fields of the compact per-response attribution item; the registry holds them as
# ``attribution_text`` / ``attribution_url`` / ``licence_id`` / ``licence_url``.
_ATTRIBUTION_KEYS = ("attribution_text", "attribution_url", "licence_id", "licence_url")

# Source ids already reported as lacking an attribution (one warning each, not one per request).
_WARNED_NO_ATTRIBUTION: set[str] = set()


def _warn_once(source_id: str, reason: str) -> None:
    if source_id in _WARNED_NO_ATTRIBUTION:
        return
    _WARNED_NO_ATTRIBUTION.add(source_id)
    logger.warning("source_attribution_missing source_id=%s reason=%s", source_id, reason)


def _compact_attribution(src: SourceInfo) -> dict[str, Any]:
    item: dict[str, Any] = {
        "source_id": src["source_id"],
        "text": src["attribution_text"],
        "url": src["attribution_url"],
        "licence_id": src["licence_id"],
        "licence_url": src["licence_url"],
    }
    # The fidelity line (es/en) travels with the credit, so a client that is not the UI shows it too.
    if src.get("processing_note"):
        item["processing_note"] = dict(src["processing_note"])
    return item


def _has_attribution(src: SourceInfo) -> bool:
    return all(src.get(k) for k in _ATTRIBUTION_KEYS)


def get_attributions(source_ids: Iterable[str | None]) -> list[dict[str, Any]]:
    """Mandatory attributions for the sources present in a response.

    Args:
        source_ids: Source ids found in the response payload (duplicates, ``None`` and
            empty values are ignored).

    Returns:
        ``[{source_id, text, url, licence_id, licence_url, processing_note}]``, one item per
        distinct source that has a structured attribution, sorted by ``source_id``
        (``processing_note`` is the es/en fidelity line, present when the registry has it). Only the
        sources actually present are listed. A source that is unknown or has no
        structured attribution is left out and reported once in the log.
    """
    index = _build_index()
    out: list[dict[str, Any]] = []
    for sid in sorted({s for s in source_ids if s}):
        src = index.get(sid)
        if src is None:
            _warn_once(sid, "not_in_registry")
        elif not _has_attribution(src):
            _warn_once(sid, "no_structured_attribution")
        else:
            out.append(_compact_attribution(src))
    return out


def all_attributions() -> list[dict[str, Any]]:
    """Every registered source that has a structured attribution, for the public listing.

    The item of :func:`get_attributions` plus ``download_date`` and ``source_documents`` where
    the registry has them.
    """
    out: list[dict[str, Any]] = []
    for src in _load_registry():
        if not _has_attribution(src):
            continue
        item: dict[str, Any] = _compact_attribution(src)
        for key in ("download_date", "source_documents"):
            if src.get(key) is not None:
                item[key] = src[key]
        out.append(item)
    return sorted(out, key=lambda i: i["source_id"])


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
