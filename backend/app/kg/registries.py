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
import math
import re
import unicodedata
from collections.abc import Iterable
from fractions import Fraction
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

DEFAULT_REGISTRIES_PATH = Path(__file__).resolve().parents[2] / "data" / "registries"

# File name per registry, in the fixed order used for the registries hash.
REGISTRY_FILES: tuple[str, ...] = (
    "crops.yaml",
    "units.yaml",
    "vocabularies.yaml",
)

# Vocabulary kinds every vocabularies.yaml must define (and nothing else).
VOCAB_KINDS: tuple[str, ...] = (
    "irrigation", "production_system", "purpose", "yield_metric", "yield_basis",
    "site_kind", "study_type",
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


# ── units ────────────────────────────────────────────────────────────────────

class Unit(_Model):
    code: str = Field(min_length=1)
    ucum: str = Field(min_length=1)
    name: str = Field(min_length=1)
    dimension: str = Field(min_length=1)
    factor: str
    aliases: tuple[str, ...] = ()

    @field_validator("factor")
    @classmethod
    def _factor_is_positive_decimal(cls, value: str) -> str:
        try:
            parsed = Fraction(value)
        except (ValueError, ZeroDivisionError) as exc:
            raise ValueError(f"factor {value!r} is not a decimal") from exc
        if parsed <= 0:
            raise ValueError("factor must be positive")
        return value

    @property
    def factor_fraction(self) -> Fraction:
        return Fraction(self.factor)


class UnitsFile(_Model):
    version: int
    units: tuple[Unit, ...]
    source_units: dict[str, dict[str, str]]


# ── vocabularies ─────────────────────────────────────────────────────────────

class VocabEntry(_Model):
    id: str = Field(min_length=1)
    value: str | None = None  # persisted form; defaults to ``id``
    label: dict[str, str]
    aliases: tuple[str, ...] = ()
    purpose: str | None = None  # yield_metric only: the purpose the evidence policy derives

    @field_validator("label")
    @classmethod
    def _label_has_english(cls, value: dict[str, str]) -> dict[str, str]:
        if not value.get("en"):
            raise ValueError("label needs an 'en' text")
        return value

    @property
    def stored(self) -> str:
        return self.value if self.value is not None else self.id


class VocabFile(_Model):
    version: int
    vocabularies: dict[str, tuple[VocabEntry, ...]]


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

    def __init__(
        self, *, crops: CropsFile, units: UnitsFile, vocabularies: VocabFile,
        registries_hash: str, path: Path,
    ) -> None:
        self.path = path
        self.registries_hash = registries_hash

        # vocabularies (first: crops and metrics refer to them)
        if set(vocabularies.vocabularies) != set(VOCAB_KINDS):
            raise RegistryError(
                f"vocabularies.yaml kinds must be exactly {sorted(VOCAB_KINDS)}, "
                f"got {sorted(vocabularies.vocabularies)}")
        self._vocab: dict[str, tuple[VocabEntry, ...]] = dict(vocabularies.vocabularies)
        self._vocab_index: dict[str, dict[str, VocabEntry]] = {}
        for kind, entries in self._vocab.items():
            _no_duplicates((e.id for e in entries), f"{kind} vocabulary id")
            _no_duplicates((e.stored for e in entries), f"{kind} vocabulary value")
            self._vocab_index[kind] = _unique_index(
                ((name, e) for e in entries for name in (e.id, e.stored, *e.aliases)),
                f"{kind} vocabulary alias")
        purposes = {e.id for e in self._vocab["purpose"]}
        for entry in self._vocab["yield_metric"]:
            if entry.purpose is not None and entry.purpose not in purposes:
                raise RegistryError(f"yield_metric {entry.id!r}: unknown purpose {entry.purpose!r}")

        # units
        self.units: tuple[Unit, ...] = units.units
        _no_duplicates((u.code for u in self.units), "unit code")
        self._unit_index: dict[str, Unit] = _unique_index(
            ((name, u) for u in self.units for name in (u.code, *u.aliases)), "unit alias")
        self._source_units: dict[str, dict[str, Unit]] = {}
        for source_id, mapping in units.source_units.items():
            resolved: dict[str, Unit] = {}
            for raw, code in mapping.items():
                if lookup_key(code) not in self._unit_index:
                    raise RegistryError(f"source_units {source_id}: {raw!r} -> unknown unit {code!r}")
                target = self._unit_index[lookup_key(code)]
                key = lookup_key(raw)
                if key in resolved and resolved[key] is not target:
                    raise RegistryError(f"source_units {source_id}: {raw!r} maps to two units")
                resolved[key] = target
            self._source_units[source_id] = resolved

        # crops
        self.crops: tuple[Crop, ...] = crops.crops
        _no_duplicates((c.eppo for c in self.crops), "crop eppo")
        self._crop_index: dict[str, Crop] = _unique_index(
            ((name, c) for c in self.crops for name in (c.eppo, *c.aliases)),
            "crop alias",
        )
        metrics = {e.id for e in self._vocab["yield_metric"]}
        for crop in self.crops:
            if crop.main_product not in metrics:
                raise RegistryError(
                    f"crop {crop.eppo}: main_product {crop.main_product!r} is not a yield_metric")
            for purpose in crop.purposes:
                if purpose not in purposes:
                    raise RegistryError(f"crop {crop.eppo}: unknown purpose {purpose!r}")

    def crop(self, eppo_or_alias: str | None) -> Crop | None:
        """The crop for an EPPO code (``eppo:`` prefix allowed), alias or raw label; else None."""
        if not eppo_or_alias:
            return None
        key = lookup_key(eppo_or_alias)
        if key.startswith("eppo:"):
            key = key[5:].strip()
        return self._crop_index.get(key)

    # ── units ────────────────────────────────────────────────────────────────

    def unit(self, code: str) -> Unit:
        """The unit for a code or alias; raises :class:`UnknownEntryError` when unregistered."""
        found = self._unit_index.get(lookup_key(code)) if isinstance(code, str) else None
        if found is None:
            raise UnknownEntryError(f"unregistered unit: {code!r}")
        return found

    def source_unit(self, source_id: str, raw_unit: str) -> Unit | None:
        """The unit a source prints as ``raw_unit`` (e.g. CREA ``q/ha``); None if not mapped."""
        return self._source_units.get(source_id, {}).get(lookup_key(raw_unit))

    def convert(self, value: float, from_unit: str, to_unit: str) -> float:
        """Convert between two units of the same dimension, exactly in decimal arithmetic."""
        if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
            raise ValueError(f"cannot convert {value!r}")
        src, dst = self.unit(from_unit), self.unit(to_unit)
        if src.dimension != dst.dimension:
            raise RegistryError(
                f"cannot convert {src.code} ({src.dimension}) to {dst.code} ({dst.dimension})")
        exact = Fraction(value) if isinstance(value, int) else Fraction(repr(value))
        return float(exact * src.factor_fraction / dst.factor_fraction)

    # ── vocabularies ─────────────────────────────────────────────────────────

    def vocab_entries(self, kind: str) -> tuple[VocabEntry, ...]:
        if kind not in self._vocab:
            raise UnknownEntryError(f"unknown vocabulary kind: {kind!r}")
        return self._vocab[kind]

    def vocab_entry(self, kind: str, value: str | None) -> VocabEntry | None:
        """The entry whose id, stored value or alias is ``value``; None if unrecognised."""
        self.vocab_entries(kind)
        if not isinstance(value, str) or not value.strip():
            return None
        return self._vocab_index[kind].get(lookup_key(value))

    def vocab(self, kind: str, value: str | None) -> str | None:
        """The stored form of a vocabulary value (AGROVOC URI for irrigation), or None."""
        entry = self.vocab_entry(kind, value)
        return entry.stored if entry else None


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
        units=_parse(UnitsFile, base / "units.yaml"),
        vocabularies=_parse(VocabFile, base / "vocabularies.yaml"),
        registries_hash=registries_hash(base),
        path=base,
    )


__all__ = [
    "DEFAULT_REGISTRIES_PATH",
    "Crop",
    "Registries",
    "RegistryError",
    "Unit",
    "UnknownEntryError",
    "VocabEntry",
    "load_registries",
    "lookup_key",
]
