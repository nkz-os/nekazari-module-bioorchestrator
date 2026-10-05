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
from datetime import date
from fractions import Fraction
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

DEFAULT_REGISTRIES_PATH = Path(__file__).resolve().parents[2] / "data" / "registries"

# File name per registry, in the fixed order used for the registries hash.
REGISTRY_FILES: tuple[str, ...] = (
    "crops.yaml",
    "units.yaml",
    "vocabularies.yaml",
    "variables.yaml",
    "sources.yaml",
)

# Values of ``commercial_use`` that let a source be loaded into a production-targeted build.
LOADABLE_COMMERCIAL_USE: frozenset[str] = frozenset({"allowed", "permission_granted"})

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


# ── variables ────────────────────────────────────────────────────────────────

class Variable(_Model):
    id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    trait: str = Field(min_length=1)
    method: str | None = None
    scale: Literal["ratio", "percent", "ordinal", "categorical", "date"]
    unit: str | None = None
    domain: tuple[float, float] | None = None
    direction: Literal["higher_better", "lower_better", "neutral"]
    denormalize: bool = False
    crop_ontology_id: str | None = Field(default=None, pattern=r"^CO_\d+:\d{7}$")
    raw_keys: dict[str, tuple[str, ...]] = {}
    notes: str | None = None

    @model_validator(mode="after")
    def _scale_is_consistent(self) -> Variable:
        if self.scale in ("ratio", "percent") and not self.unit:
            raise ValueError(f"{self.id}: a {self.scale} scale needs a unit")
        if self.scale in ("ordinal", "categorical", "date") and self.unit is not None:
            raise ValueError(f"{self.id}: a {self.scale} scale has no unit")
        if self.scale == "percent" and self.unit != "%":
            raise ValueError(f"{self.id}: a percent scale uses the unit '%'")
        if self.domain is not None:
            if self.scale != "ordinal":
                raise ValueError(f"{self.id}: only an ordinal scale has a domain")
            if self.domain[0] >= self.domain[1]:
                raise ValueError(f"{self.id}: domain must be [min, max] with min < max")
        return self


class VariablesFile(_Model):
    version: int
    variables: tuple[Variable, ...]


# ── sources ──────────────────────────────────────────────────────────────────

class SourceDocument(_Model):
    title: str = Field(min_length=1)
    year: int
    url: str = Field(min_length=1)


class Licence(_Model):
    licence_id: str = Field(min_length=1)
    terms_url: str | None = None
    quote: str | None = None  # literal, in the original language
    quote_language: str | None = None
    checked_at: date
    commercial_use: Literal["allowed", "permission_granted", "denied", "unknown"]
    tdm_reserved: bool | None = None  # None: not assessed
    attribution_text: str | None = None
    attribution_url: str | None = None
    download_date: date | None = None
    permission_ref: str | None = None
    conditions: tuple[str, ...] = ()
    source_documents: tuple[SourceDocument, ...] = ()
    processing_note: dict[str, str] = {}
    notes: str | None = None

    @model_validator(mode="after")
    def _evidence_matches_the_verdict(self) -> Licence:
        if self.commercial_use in LOADABLE_COMMERCIAL_USE and not (self.attribution_text or "").strip():
            raise ValueError("a loadable source needs a mandatory attribution_text")
        if self.commercial_use == "permission_granted" and not (self.permission_ref or "").strip():
            raise ValueError("permission_granted needs a permission_ref (the written permission)")
        if self.commercial_use not in LOADABLE_COMMERCIAL_USE and not (
                (self.quote or "").strip() or (self.notes or "").strip()):
            raise ValueError("a denied or unknown source needs its quote or a note saying why")
        if self.quote and not self.quote_language:
            raise ValueError("a quote needs its quote_language")
        return self


class Source(_Model):
    source_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    name: str = Field(min_length=1)
    institution: str = Field(min_length=1)
    country: str = Field(min_length=2, max_length=3)
    url: str | None = None
    licence: Licence

    @property
    def loadable(self) -> bool:
        """May be loaded into a production-targeted build (the gate's licence rule)."""
        return self.licence.commercial_use in LOADABLE_COMMERCIAL_USE


class SourcesFile(_Model):
    version: int
    sources: tuple[Source, ...]


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
        variables: VariablesFile, sources: SourcesFile, registries_hash: str, path: Path,
    ) -> None:
        self.path = path
        self.registries_hash = registries_hash

        # sources (referenced by units, variables and crops)
        self.sources: tuple[Source, ...] = sources.sources
        _no_duplicates((src.source_id for src in self.sources), "source id")
        self._source_index: dict[str, Source] = {src.source_id: src for src in self.sources}

        # vocabularies (crops and metrics refer to them)
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
        for source_id in units.source_units:
            self._require_source(source_id, "units.source_units")

        # variables
        self.variables: tuple[Variable, ...] = variables.variables
        _no_duplicates((v.id for v in self.variables), "variable id")
        self._variable_index: dict[str, Variable] = {v.id: v for v in self.variables}
        self._raw_key_index: dict[str, dict[str, Variable]] = {}
        for variable in self.variables:
            if variable.unit is not None and lookup_key(variable.unit) not in self._unit_index:
                raise RegistryError(f"variable {variable.id}: unregistered unit {variable.unit!r}")
            for source_id, keys in variable.raw_keys.items():
                self._require_source(source_id, f"variable {variable.id} raw_keys")
                index = self._raw_key_index.setdefault(source_id, {})
                for key in keys:
                    if key in index:
                        raise RegistryError(
                            f"raw key {source_id}.{key} maps to both {index[key].id} and {variable.id}")
                    index[key] = variable

        # crops
        self.crops: tuple[Crop, ...] = crops.crops
        _no_duplicates((c.eppo for c in self.crops), "crop eppo")
        self._crop_index: dict[str, Crop] = _unique_index(
            ((name, c) for c in self.crops for name in (c.eppo, *c.aliases)),
            "crop alias",
        )
        metrics = {e.id for e in self._vocab["yield_metric"]}
        for crop in self.crops:
            for source_id in crop.seen_in:
                self._require_source(source_id, f"crop {crop.eppo} seen_in")
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

    def _require_source(self, source_id: str, where: str) -> None:
        if source_id not in self._source_index:
            raise RegistryError(f"{where}: unknown source {source_id!r}")

    # ── sources ──────────────────────────────────────────────────────────────

    def source(self, source_id: str) -> Source:
        """The source for an id; raises :class:`UnknownEntryError` when unregistered."""
        found = self._source_index.get(source_id) if isinstance(source_id, str) else None
        if found is None:
            raise UnknownEntryError(f"unregistered source: {source_id!r}")
        return found

    def loadable_sources(self) -> tuple[Source, ...]:
        """Sources whose licence lets them into a production-targeted build."""
        return tuple(src for src in self.sources if src.loadable)

    # ── variables ────────────────────────────────────────────────────────────

    def variable(self, variable_id: str) -> Variable:
        """The variable for a stable id; raises :class:`UnknownEntryError` when unregistered."""
        found = self._variable_index.get(variable_id) if isinstance(variable_id, str) else None
        if found is None:
            raise UnknownEntryError(f"unregistered variable: {variable_id!r}")
        return found

    def variable_for_raw_key(self, source_id: str, raw_key: str) -> Variable | None:
        """The variable a source's raw field name publishes (discovery evidence), else None."""
        return self._raw_key_index.get(source_id, {}).get(raw_key)

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
        variables=_parse(VariablesFile, base / "variables.yaml"),
        sources=_parse(SourcesFile, base / "sources.yaml"),
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
