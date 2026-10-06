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
import numbers
import re
import unicodedata
from collections.abc import Iterable, Mapping
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
    "sites.yaml",
    "ranges.yaml",
    "varieties.yaml",
    "irrigation_thresholds.yaml",
)

# Id of the one derivation method of the irrigation regime from a yield (``irrigation_thresholds.yaml``).
# The version is part of the name: another rule is another method, never a silent change of this one.
IRRIGATION_DERIVATION_V1 = "yield_threshold_v1"

# Condition keys a contextual range may use, and the vocabulary each draws its values from
# (``None``: free-form, a Koppen climate code).
RANGE_CONDITION_KINDS: dict[str, str | None] = {
    "irrigation": "irrigation",
    "production_system": "production_system",
    "purpose": "purpose",
    "climate_class": None,
}

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


# ── sites ────────────────────────────────────────────────────────────────────

class Site(_Model):
    id: str = Field(pattern=r"^[A-Z]{2}-[A-Z0-9][A-Z0-9-]*$")
    name: str = Field(min_length=1)
    site_kind: str = Field(min_length=1)
    country: str = Field(pattern=r"^[A-Z]{2}$")
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    coordinate_source: str | None = None
    aliases: tuple[str, ...] = ()
    sources: tuple[str, ...] = ()
    status: Literal["reviewed", "assumption"]
    note: str | None = None

    @model_validator(mode="after")
    def _coordinates_and_assumptions_are_documented(self) -> Site:
        if (self.latitude is None) != (self.longitude is None):
            raise ValueError(f"{self.id}: latitude and longitude come together")
        if self.latitude is not None and not self.coordinate_source:
            raise ValueError(f"{self.id}: coordinates need their coordinate_source")
        if self.status == "assumption" and not (self.note or "").strip():
            raise ValueError(f"{self.id}: an assumption needs its note")
        return self


class SitesFile(_Model):
    version: int
    sites: tuple[Site, ...]


# ── ranges ───────────────────────────────────────────────────────────────────

class RangeObserved(_Model):
    min: float
    max: float
    n: int = Field(ge=1)


class Range(_Model):
    id: str = Field(min_length=1)
    crop: str = Field(min_length=1)
    variable: str = Field(min_length=1)
    conditions: dict[str, str] = {}
    min: float
    max: float
    unit: str = Field(min_length=1)
    status: Literal["reviewed", "assumption"]
    reviewer: str | None = None
    evidence: str | None = None  # required once reviewed: what the reviewer relied on
    note: str | None = None
    observed: RangeObserved | None = None

    @model_validator(mode="after")
    def _bounds_and_review_are_consistent(self) -> Range:
        if self.min >= self.max:
            raise ValueError(f"{self.id}: min must be below max")
        if self.status == "reviewed" and not (self.reviewer or "").strip():
            raise ValueError(f"{self.id}: a reviewed range names its reviewer")
        if self.status == "reviewed" and not (self.evidence or "").strip():
            raise ValueError(f"{self.id}: a reviewed range carries its evidence")
        if self.status == "assumption" and "pending agronomist review" not in (self.note or ""):
            raise ValueError(f"{self.id}: an assumption range says 'pending agronomist review'")
        return self

    @property
    def specificity(self) -> int:
        return len(self.conditions)


class RangesFile(_Model):
    version: int
    ranges: tuple[Range, ...]


# ── varieties ────────────────────────────────────────────────────────────────

class Variety(_Model):
    id: str = Field(pattern=r"^[A-Z0-9]{5,6}:[a-z0-9][a-z0-9-]*$")
    crop: str = Field(min_length=1)
    name: str = Field(min_length=1)
    status: Literal["reviewed", "assumption", "candidate"]
    aliases: tuple[str, ...] = ()
    evidence: str | None = None
    reviewer: str | None = None  # required once reviewed
    sources: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _aliases_need_evidence(self) -> Variety:
        if self.status == "reviewed" and not (self.reviewer or "").strip():
            raise ValueError(f"{self.id}: a reviewed variety names its reviewer")
        if self.status == "reviewed" and not (self.evidence or "").strip():
            raise ValueError(f"{self.id}: a reviewed variety carries its evidence")
        if self.aliases and not (self.evidence or "").strip():
            raise ValueError(f"{self.id}: aliases need the evidence that groups them")
        if self.aliases and self.status == "candidate":
            raise ValueError(f"{self.id}: an alias group is an assumption or reviewed, not a candidate")
        if not self.id.startswith(f"{self.crop}:"):
            raise ValueError(f"{self.id}: the id starts with its crop {self.crop}")
        return self


class VarietiesFile(_Model):
    version: int
    varieties: tuple[Variety, ...]


# ── irrigation yield thresholds ──────────────────────────────────────────────

class ThresholdEvidence(_Model):
    """What a threshold pair was calibrated on: the rows whose regime the source itself states."""

    method: str = Field(min_length=1)
    epsilon: float | None = Field(default=None, gt=0, lt=0.5)
    labelled_sources: tuple[str, ...] = ()
    n_rainfed: int = Field(ge=0)
    n_irrigated: int = Field(ge=0)
    rainfed_quantiles: dict[str, float] = {}
    irrigated_quantiles: dict[str, float] = {}
    misclassified_rainfed: int | None = Field(default=None, ge=0)
    misclassified_irrigated: int | None = Field(default=None, ge=0)
    error_rate: float | None = Field(default=None, ge=0, le=1)
    raw_data_commit: str | None = None
    reason: str | None = None  # why a crop has no cutoff


class IrrigationThreshold(_Model):
    """Per-crop yield cutoffs (kg/ha) that derive an irrigation regime where the source states none.

    yield <= low_kg_ha is rainfed; yield >= high_kg_ha is irrigated; in between the regime stays
    unknown. ``assumption`` values are calibrated but not approved: ``owner_approval`` is empty until
    the owner signs them off. ``not_calibrated`` has no cutoff and says why.
    """

    crop: str = Field(pattern=r"^[A-Z0-9]{5,6}$")
    status: Literal["assumption", "not_calibrated"]
    low_kg_ha: float | None = Field(default=None, gt=0)
    high_kg_ha: float | None = Field(default=None, gt=0)
    owner_approval: str | None = None
    evidence: ThresholdEvidence
    note: str | None = None

    @model_validator(mode="after")
    def _values_follow_the_status(self) -> IrrigationThreshold:
        if self.status == "not_calibrated":
            if self.low_kg_ha is not None or self.high_kg_ha is not None:
                raise ValueError(f"{self.crop}: a crop without a cutoff has no threshold values")
            if not (self.evidence.reason or "").strip():
                raise ValueError(f"{self.crop}: a crop without a cutoff says why")
            if (self.owner_approval or "").strip():
                raise ValueError(f"{self.crop}: there is nothing to approve without a cutoff")
            return self
        if self.low_kg_ha is None or self.high_kg_ha is None:
            raise ValueError(f"{self.crop}: a calibrated crop has both thresholds")
        if self.low_kg_ha >= self.high_kg_ha:
            raise ValueError(f"{self.crop}: low must be below high (the two bands must not overlap)")
        if not (self.evidence.n_rainfed and self.evidence.n_irrigated):
            raise ValueError(f"{self.crop}: the calibration evidence counts rows of both regimes")
        return self


class IrrigationThresholdsFile(_Model):
    version: int
    method: Literal["yield_threshold_v1"]
    thresholds: tuple[IrrigationThreshold, ...]


# ── loading helpers ──────────────────────────────────────────────────────────

_YAML_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)  # the C loader gives the same result, faster


def _read_yaml(path: Path) -> Any:
    if not path.is_file():
        raise RegistryError(f"missing registry file: {path}")
    try:
        return yaml.load(path.read_text(encoding="utf-8"), Loader=_YAML_LOADER)
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
        variables: VariablesFile, sources: SourcesFile, sites: SitesFile, ranges: RangesFile,
        varieties: VarietiesFile, irrigation_thresholds: IrrigationThresholdsFile,
        registries_hash: str, path: Path,
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

        # sites
        self.sites: tuple[Site, ...] = sites.sites
        _no_duplicates((st.id for st in self.sites), "site id")
        kinds = {e.id for e in self._vocab["site_kind"]}
        for site in self.sites:
            if site.site_kind not in kinds:
                raise RegistryError(f"site {site.id}: unknown site_kind {site.site_kind!r}")
            if site.site_kind != "field" and site.latitude is not None:
                raise RegistryError(f"site {site.id}: an {site.site_kind} site has no coordinates")
            for source_id in site.sources:
                self._require_source(source_id, f"site {site.id} sources")
        self._site_index: dict[str, Site] = _unique_index(
            ((name, st) for st in self.sites for name in (st.id, st.name, *st.aliases)), "site alias")

        # ranges (reference crops and variables, so they are validated after both)
        self.ranges: tuple[Range, ...] = ranges.ranges
        _no_duplicates((r.id for r in self.ranges), "range id")
        self._ranges_by_cv: dict[tuple[str, str], list[Range]] = {}
        for rng in self.ranges:
            crop_entry = next((c for c in crops.crops if c.eppo == rng.crop), None)
            if crop_entry is None:
                raise RegistryError(f"range {rng.id}: crop {rng.crop!r} is not a canonical EPPO code in crops.yaml")
            if rng.variable not in self._variable_index:
                raise RegistryError(f"range {rng.id}: unregistered variable {rng.variable!r}")
            variable = self._variable_index[rng.variable]
            if variable.scale not in ("ratio", "percent"):
                raise RegistryError(f"range {rng.id}: {variable.id} has no numeric scale")
            if rng.unit != variable.unit:
                raise RegistryError(f"range {rng.id}: unit {rng.unit!r} differs from the variable's {variable.unit!r}")
            for key, value in rng.conditions.items():
                if key not in RANGE_CONDITION_KINDS:
                    raise RegistryError(f"range {rng.id}: unknown condition {key!r}")
                kind = RANGE_CONDITION_KINDS[key]
                if kind is not None and value not in {e.id for e in self._vocab[kind]}:
                    raise RegistryError(f"range {rng.id}: condition {key}={value!r} is not a {kind} vocabulary id")
            self._ranges_by_cv.setdefault((rng.crop, rng.variable), []).append(rng)
        for group in self._ranges_by_cv.values():
            for i, first in enumerate(group):
                for second in group[i + 1:]:
                    if first.conditions == second.conditions:
                        raise RegistryError(f"ranges {first.id} and {second.id} have the same conditions")
                    shared = first.conditions.keys() & second.conditions.keys()
                    exclusive = any(first.conditions[k] != second.conditions[k] for k in shared)
                    if first.specificity == second.specificity and not exclusive:
                        raise RegistryError(
                            f"ranges {first.id} and {second.id} are equally specific and can both match")

        # varieties
        self.varieties: tuple[Variety, ...] = varieties.varieties
        _no_duplicates((v.id for v in self.varieties), "variety id")
        canonical_crops = {c.eppo for c in crops.crops}
        self._variety_index: dict[tuple[str, str], Variety] = {}
        for variety in self.varieties:
            if variety.crop not in canonical_crops:
                raise RegistryError(
                    f"variety {variety.id}: crop {variety.crop!r} is not a canonical EPPO code in crops.yaml")
            for source_id in variety.sources:
                self._require_source(source_id, f"variety {variety.id} sources")
            for name in (variety.name, *variety.aliases):
                key = (variety.crop, lookup_key(name))
                if self._variety_index.setdefault(key, variety) is not variety:
                    raise RegistryError(
                        f"variety alias: {name!r} ({variety.crop}) resolves to more than one variety")

        # irrigation thresholds (per crop, canonical EPPO codes only)
        self.irrigation_thresholds: tuple[IrrigationThreshold, ...] = irrigation_thresholds.thresholds
        _no_duplicates((t.crop for t in self.irrigation_thresholds), "irrigation threshold crop")
        self._threshold_index: dict[str, IrrigationThreshold] = {}
        for threshold in self.irrigation_thresholds:
            if threshold.crop not in canonical_crops:
                raise RegistryError(
                    f"irrigation threshold {threshold.crop}: not a canonical EPPO code in crops.yaml")
            self._threshold_index[threshold.crop] = threshold

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

    # ── varieties ────────────────────────────────────────────────────────────

    def variety(self, crop_eppo: str | None, raw_name: str | None) -> Variety | None:
        """The registered variety for a crop and a raw name or alias; None when unregistered.

        Exact match only (NFC, case-folded, whitespace collapsed). A name that is not registered is
        never matched to a similar one: the build queues it for review and loads a candidate.
        """
        crop = self.crop(crop_eppo)
        if crop is None or not raw_name:
            return None
        return self._variety_index.get((crop.eppo, lookup_key(raw_name)))

    # ── ranges ───────────────────────────────────────────────────────────────

    def _canonical_condition(self, key: str, value: str | None) -> str | None:
        """A query condition value in the form ranges store it; None when absent or unrecognised."""
        if value is None:
            return None
        kind = RANGE_CONDITION_KINDS[key]
        if kind is None:
            return value.strip() or None
        entry = self.vocab_entry(kind, value)
        return entry.id if entry else None

    def range_for(
        self, eppo: str, variable_id: str, conditions: Mapping[str, str | None] | None = None,
    ) -> Range | None:
        """The most specific plausible range for a crop and variable under ``conditions``.

        A range applies when each of its conditions equals the query's (vocabulary literals and
        AGROVOC URIs are accepted and canonicalised). The one with the most conditions wins; the
        registry guarantees no two ranges of equal specificity can both match. No match, an unknown
        crop or a missing condition value gives None (no check), never a guess.
        """
        self.variable(variable_id)
        crop = self.crop(eppo)
        if crop is None:
            return None
        query: dict[str, str] = {}
        for key, value in (conditions or {}).items():
            if key not in RANGE_CONDITION_KINDS:
                raise ValueError(f"unknown range condition {key!r}; expected one of {sorted(RANGE_CONDITION_KINDS)}")
            canonical = self._canonical_condition(key, value)
            if canonical is not None:
                query[key] = canonical
        candidates = [
            r for r in self._ranges_by_cv.get((crop.eppo, variable_id), ())
            if all(query.get(k) == v for k, v in r.conditions.items())
        ]
        return max(candidates, key=lambda r: r.specificity, default=None)

    # ── irrigation thresholds ────────────────────────────────────────────────

    def irrigation_threshold(self, eppo_or_alias: str | None) -> IrrigationThreshold | None:
        """The calibrated yield cutoffs of a crop, or None when it has none (never a default)."""
        crop = self.crop(eppo_or_alias)
        if crop is None:
            return None
        found = self._threshold_index.get(crop.eppo)
        return found if found is not None and found.status == "assumption" else None

    # ── sites ────────────────────────────────────────────────────────────────

    def site(self, name_or_alias: str | None) -> Site | None:
        """The canonical site for a name, alias or site id; None when unresolved."""
        if not name_or_alias:
            return None
        return self._site_index.get(lookup_key(name_or_alias))

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
        if isinstance(value, bool) or not isinstance(value, numbers.Real) or not math.isfinite(value):
            raise ValueError(f"cannot convert {value!r}")
        src, dst = self.unit(from_unit), self.unit(to_unit)
        if src.dimension != dst.dimension:
            raise RegistryError(
                f"cannot convert {src.code} ({src.dimension}) to {dst.code} ({dst.dimension})")
        exact = Fraction(str(float(value)))  # shortest decimal of the float; numpy scalars work too
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
        sites=_parse(SitesFile, base / "sites.yaml"),
        ranges=_parse(RangesFile, base / "ranges.yaml"),
        varieties=_parse(VarietiesFile, base / "varieties.yaml"),
        irrigation_thresholds=_parse(IrrigationThresholdsFile, base / "irrigation_thresholds.yaml"),
        registries_hash=registries_hash(base),
        path=base,
    )


__all__ = [
    "DEFAULT_REGISTRIES_PATH",
    "IRRIGATION_DERIVATION_V1",
    "Crop",
    "Registries",
    "RegistryError",
    "Unit",
    "UnknownEntryError",
    "VocabEntry",
    "load_registries",
    "lookup_key",
]
