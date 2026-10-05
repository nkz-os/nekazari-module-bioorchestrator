"""KG registries: versioned YAML data under ``backend/data/registries/``.

``load_registries(path)`` reads and validates every registry file (pydantic v2, strict) and
returns an immutable :class:`Registries` with the lookup APIs the rest of ``app.kg`` uses.
Nothing here touches Neo4j or the network, and nothing is guessed: an unknown value is an
explicit ``None`` (or a :class:`UnknownEntryError` for the lookups that must succeed).

Lookups are exact after Unicode NFC, case-folding and whitespace collapsing. There is no fuzzy
matching anywhere; resolving two spellings to one entry is always an explicit alias in a file.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

DEFAULT_REGISTRIES_PATH = Path(__file__).resolve().parents[2] / "data" / "registries"

# File name per registry, in the fixed order used for the registries hash.
REGISTRY_FILES: tuple[str, ...] = (
    "crops.yaml",
)


class RegistryError(Exception):
    """A registry file is missing, malformed or internally inconsistent."""


class UnknownEntryError(RegistryError, KeyError):
    """A lookup that must succeed named an entry that is not registered."""

    def __str__(self) -> str:  # KeyError would repr() the message
        return str(self.args[0]) if self.args else ""


def lookup_key(value: str) -> str:
    """The comparison form of a name: NFC, case-folded, whitespace collapsed."""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", value).strip().casefold())


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# ── crops ────────────────────────────────────────────────────────────────────

class Crop(_Model):
    eppo: str = Field(pattern=r"^[A-Z0-9]{5,6}$")
    scientific_name: str = Field(min_length=1)
    family: str = Field(min_length=1)
    main_product: str = Field(min_length=1)
    purposes: tuple[str, ...] = Field(min_length=1)
    seen_in: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    notes: str | None = None


class CropsFile(_Model):
    version: int
    crops: tuple[Crop, ...]


# ── loading helpers ──────────────────────────────────────────────────────────

def _read_yaml(path: Path) -> Any:
    if not path.is_file():
        raise RegistryError(f"missing registry file: {path}")
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise RegistryError(f"{path.name}: invalid YAML: {exc}") from exc


def _parse(model: type[_Model], path: Path) -> Any:
    try:
        return model.model_validate(_read_yaml(path))
    except ValidationError as exc:
        raise RegistryError(f"{path.name}: schema validation failed:\n{exc}") from exc


def _unique_index(entries: Iterable[tuple[str, Any]], what: str) -> dict[str, Any]:
    """``{lookup_key: entry}``; the same key for two different entries is an error."""
    index: dict[str, Any] = {}
    for name, entry in entries:
        key = lookup_key(name)
        if key in index and index[key] is not entry:
            raise RegistryError(f"{what}: {name!r} resolves to more than one entry")
        index[key] = entry
    return index


def _no_duplicates(values: Iterable[str], what: str) -> None:
    seen: set[str] = set()
    for value in values:
        if value in seen:
            raise RegistryError(f"duplicate {what}: {value!r}")
        seen.add(value)


# ── the registries ───────────────────────────────────────────────────────────

class Registries:
    """Immutable view over every registry file, with the lookup APIs."""

    def __init__(self, *, crops: CropsFile, registries_hash: str, path: Path) -> None:
        self.path = path
        self.registries_hash = registries_hash
        self.crops: tuple[Crop, ...] = crops.crops

        _no_duplicates((c.eppo for c in self.crops), "crop eppo")
        self._crop_index: dict[str, Crop] = _unique_index(
            ((name, c) for c in self.crops for name in (c.eppo, *c.aliases)),
            "crop alias",
        )

    def crop(self, eppo_or_alias: str | None) -> Crop | None:
        """The crop for an EPPO code (``eppo:`` prefix allowed), alias or raw label; else None."""
        if not eppo_or_alias:
            return None
        key = lookup_key(eppo_or_alias)
        if key.startswith("eppo:"):
            key = key[5:].strip()
        return self._crop_index.get(key)


def registries_hash(path: Path) -> str:
    """sha256 over the registry files (name and bytes, fixed order)."""
    digest = hashlib.sha256()
    for name in REGISTRY_FILES:
        file = path / name
        if not file.is_file():
            raise RegistryError(f"missing registry file: {file}")
        digest.update(name.encode("utf-8") + b"\0" + file.read_bytes() + b"\0")
    return digest.hexdigest()


def load_registries(path: str | Path | None = None) -> Registries:
    """Load and validate every registry file under ``path`` (default: the repo data directory)."""
    base = Path(path) if path is not None else DEFAULT_REGISTRIES_PATH
    return Registries(
        crops=_parse(CropsFile, base / "crops.yaml"),
        registries_hash=registries_hash(base),
        path=base,
    )


__all__ = [
    "DEFAULT_REGISTRIES_PATH",
    "Crop",
    "Registries",
    "RegistryError",
    "UnknownEntryError",
    "load_registries",
    "lookup_key",
]
