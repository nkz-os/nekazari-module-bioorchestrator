"""Canonical species resolver.

Loads species_registry.yaml and provides resolve_species() which
accepts any identifier (slug, scientific name, EPPO code, common name
in any language) and returns the canonical slug.

Usage:
    from app.species_registry import resolve_species
    slug = resolve_species("HORVX")        # → "barley"
    slug = resolve_species("Zea mays")      # → "maize"
    slug = resolve_species("garbanzo")      # → "chickpea"
    slug = resolve_species("ciço")          # → "chickpea" (eu: garbanzo)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

_registry: dict[str, dict[str, Any]] | None = None
"""Loaded registry keyed by canonical slug."""

_aliases: dict[str, str] | None = None
"""All known identifiers → canonical slug. Built once on first lookup."""


def _build_aliases() -> dict[str, str]:
    """Build a lookup table mapping every identifier to its canonical slug."""
    global _registry  # noqa: PLW0602
    if _registry is None:
        _load()

    aliases: dict[str, str] = {}
    for slug, data in _registry.items():
        # Canonical slug itself
        aliases[slug] = slug
        aliases[slug.lower()] = slug

        # Scientific name (exact + lowercased)
        sci = data.get("scientific_name", "")
        if sci:
            aliases[sci] = slug
            aliases[sci.lower()] = slug

        # EPPO code, and the other codes of the same species (``eppo_aliases``)
        eppo = data.get("eppo_code", "")
        for code in ([eppo] if eppo else []) + list(data.get("eppo_aliases") or []):
            aliases[code] = slug
            aliases[code.lower()] = slug

        # All common names in all languages
        for name in data.get("common_names", {}).values():
            aliases[name] = slug
            aliases[name.lower()] = slug

    # Add EPPO index entries (redundant but explicit)
    for code, slug in _registry.get("_eppo_index", {}).items():
        aliases[code] = slug
        aliases[code.lower()] = slug

    return aliases


def _load() -> None:
    """Load the registry once into global state."""
    global _registry
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    path = data_dir / "species_registry.yaml"
    if path.exists():
        with path.open("r") as f:
            raw = yaml.safe_load(f)
            _registry = raw.get("species", {})
    else:
        _registry = {}


def resolve_species(identifier: str) -> str | None:
    """Resolve any species identifier to its canonical slug.

    Args:
        identifier: Any of:
            - canonical slug ("wheat")
            - scientific name ("Triticum aestivum")
            - EPPO code ("TRZAX", "HORVX") or an alias code of the same species ("ZEAMX")
            - common name in any language ("trigo", "blé", "gari")

    Returns:
        Canonical slug on success, None if not found.
        Comparison is exact, case-insensitive for codes.

    Examples:
        >>> resolve_species("HORVX")
        "barley"
        >>> resolve_species("Zea mays")
        "maize"
        >>> resolve_species("garbanzo")
        "chickpea"
    """
    global _aliases
    if _aliases is None:
        _aliases = _build_aliases()

    # Exact match first
    slug = _aliases.get(identifier)
    if slug:
        return slug

    # Case-insensitive
    slug = _aliases.get(identifier.lower())
    if slug:
        return slug

    return None


def list_species() -> list[str]:
    """Return all canonical slugs in the registry."""
    global _registry  # noqa: PLW0602
    if _registry is None:
        _load()
    return sorted(k for k in _registry if k != "_eppo_index")


# Listing merge: exact sibling codes of ONE species whose trials are the same observations
# (``{sibling: listed code}``). The crop catalog and the recommend candidates list the species
# once, under the listed code. Every other ``eppo_aliases`` entry only resolves the species and is
# NOT merged in listings (whether two codes are one crop for the farmer is an owner decision).
# ZEAMA is the registry's code for maize; the trials and the graph use ZEAMX (the ingesters
# already unify ZEAMA into ZEAMX).
CATALOG_SIBLING_CODES: dict[str, str] = {"ZEAMA": "ZEAMX"}


def catalog_code(eppo: str) -> str:
    """The code a crop is listed under: its exact sibling's listed code, else itself."""
    return CATALOG_SIBLING_CODES.get(eppo, eppo)


def get_species_info(slug: str) -> dict[str, Any] | None:
    """Return full registry entry for a canonical slug."""
    global _registry  # noqa: PLW0602
    if _registry is None:
        _load()
    return _registry.get(slug)


def get_crop_group(slug: str) -> str | None:
    """Return the crop_group for a canonical species slug, or None."""
    global _registry  # noqa: PLW0602
    if _registry is None:
        _load()
    data = _registry.get(slug)
    return data.get("crop_group") if data else None
