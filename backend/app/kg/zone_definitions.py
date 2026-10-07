"""GENVCE's own climatic zone definitions, applied to a parcel (no I/O beyond reading the registry).

Registry: ``data/registries/genvce_zone_definitions.yaml``. Each definition is one GENVCE report (campaign)
with the thresholds the report publishes and a citation. GENVCE classifies a trial by the weather of its
campaign; a parcel is classified here by its climatology (April mean air temperature, annual precipitation),
so every answer built on this carries ``ZONE_MATCH_BASIS`` and ``ZONE_MATCH_CAVEAT``.

Two directions:

* build side: ``ZoneDefinitions.zone_key`` gives a unit its ``zoneKey`` (definition id + zone label) when,
  and only when, its document family and campaign have a published climatic definition, its crop is one the
  definition covers and its zone label is a registered one that states the classes the definition needs.
  Everything else gets no key and stays at the country level.
* read side: ``ZoneDefinitions.classify_parcel`` gives, for a parcel's climate, the zone keys that are the
  parcel's zone (``allow``), decidably another zone (``deny``) and undecidable (neither: the parcel lacks a
  class, e.g. a precipitation that sits exactly on a published limit, or the irrigation regime is unknown).
"""
from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, model_validator

DEFAULT_ZONE_DEFINITIONS_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "registries" / "genvce_zone_definitions.yaml"
)

ZONE_MATCH_BASIS = "chelsa_1981_2010_climatology"
ZONE_MATCH_CAVEAT = (
    "GENVCE classifies each trial by the weather of its campaign; the parcel is classified by its "
    "1981-2010 climatology (CHELSA v2.1), so the zone is an approximation of the one GENVCE would assign."
)

AXES = ("temperature", "rainfall")

# What a zone label has to say to state a class (accent- and case-folded, whole words).
_STATES: dict[tuple[str, str], tuple[str, ...]] = {
    ("temperature", "cold"): (r"\bfri[oa]s?\b",),
    ("temperature", "temperate"): (r"\btemplad[oa]s?\b",),
    ("temperature", "warm"): (r"\bcalid[oa]s?\b",),
    ("rainfall", "semiarid"): (r"\bsemiarid[oa]s?\b",),
    ("rainfall", "arid_semiarid"): (r"\baridos y semiaridos\b",),
    ("rainfall", "subhumid"): (r"\bsubhumed[oa]s?\b",),
    ("rainfall", "humid"): (r"\bhumed[oa]s?\b",),
}
_REGIME_STATES = {"secano": r"\bsecanos?\b", "regadio": r"\bregadios?\b"}


class ZoneDefinitionError(ValueError):
    """The zone definitions registry is not valid."""


def fold(text: str | None) -> str:
    """NFC, case-folded, whitespace-collapsed (the registries' exact-lookup normal form)."""
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", text or "").strip().casefold())


def _plain(text: str) -> str:
    """Accent-free fold, for the keyword check only."""
    decomposed = unicodedata.normalize("NFD", fold(text))
    return "".join(c for c in decomposed if not unicodedata.combining(c))


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Interval(_Frozen):
    """One class of a threshold scale: ``min``/``max`` with explicit inclusiveness (a missing bound is open)."""

    # "class" is a Python keyword: the registry spells it that way, the model aliases it
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)
    class_: str
    min: float | None = None
    min_inclusive: bool = False
    max: float | None = None
    max_inclusive: bool = False

    def contains(self, value: float) -> bool:
        if self.min is not None and not (value > self.min or (self.min_inclusive and value == self.min)):
            return False
        return not (self.max is not None and not (value < self.max or (self.max_inclusive and value == self.max)))


class Citation(_Frozen):
    document: str
    section: str
    pdf_page: int
    table: str
    axes_basis: str


class LabelEntry(_Frozen):
    label: str
    temperature: tuple[str, ...] = ()
    rainfall: tuple[str, ...] = ()
    regime: Literal["secano", "regadio"] | None = None
    waives: tuple[Literal["temperature", "rainfall"], ...] = ()

    def classes(self, axis: str) -> tuple[str, ...]:
        return getattr(self, axis)


class TableException(_Frozen):
    label: str
    tables: tuple[str, ...]


class Definition(_Frozen):
    id: str
    family: str
    campaigns: tuple[str, ...]
    crops: tuple[str, ...]
    required_axes: tuple[Literal["temperature", "rainfall"], ...]
    temperature: tuple[Interval, ...]
    rainfall: tuple[Interval, ...] = ()
    labels: tuple[LabelEntry, ...]
    except_tables: tuple[TableException, ...] = ()
    citation: Citation

    @model_validator(mode="after")
    def _consistent(self) -> Definition:
        classes = {"temperature": {i.class_ for i in self.temperature}, "rainfall": {i.class_ for i in self.rainfall}}
        for axis in self.required_axes:
            if not classes[axis]:
                raise ValueError(f"{self.id}: axis {axis!r} is required but the definition has no thresholds for it")
        seen: set[str] = set()
        for entry in self.labels:
            key = fold(entry.label)
            if key in seen:
                raise ValueError(f"{self.id}: label {entry.label!r} appears twice")
            seen.add(key)
            text = _plain(entry.label)
            for axis in AXES:
                for cls in entry.classes(axis):
                    if cls not in classes[axis]:
                        raise ValueError(f"{self.id}: label {entry.label!r} names {axis} class {cls!r}, "
                                         "which this definition does not define")
                    if not any(re.search(p, text) for p in _STATES[(axis, cls)]):
                        raise ValueError(f"{self.id}: label {entry.label!r} does not state {axis} class {cls!r}")
                stated = {cls for (ax, cls), pats in _STATES.items()
                          if ax == axis and cls in classes[axis] and any(re.search(p, text) for p in pats)}
                missing = stated - set(entry.classes(axis))
                if missing and axis not in entry.waives:
                    raise ValueError(f"{self.id}: label {entry.label!r} states {axis} {sorted(missing)} "
                                     "but the entry does not constrain it")
                if axis in self.required_axes and not entry.classes(axis) and axis not in entry.waives:
                    raise ValueError(f"{self.id}: label {entry.label!r} does not state required axis {axis!r}")
            if entry.regime is not None and not re.search(_REGIME_STATES[entry.regime], text):
                raise ValueError(f"{self.id}: label {entry.label!r} does not state regime {entry.regime!r}")
        known = {fold(e.label) for e in self.labels}
        for exc in self.except_tables:
            if fold(exc.label) not in known:
                raise ValueError(f"{self.id}: except_tables names unregistered label {exc.label!r}")
        return self

    def classify(self, axis: str, value: float | None) -> str | None:
        """Class of ``value`` on ``axis``; None when the value is missing or on a gap of the published scale."""
        if value is None:
            return None
        for interval in getattr(self, axis):
            if interval.contains(value):
                return interval.class_
        return None


class DocumentFamily(_Frozen):
    family: str
    title_prefixes: tuple[str, ...]


def zone_key(definition_id: str, label: str) -> str:
    return f"{definition_id}::{fold(label)}"


@dataclass(frozen=True)
class ParcelZones:
    """A parcel's zone under every definition: the zone keys that match, mismatch, or cannot be decided."""

    allow: frozenset[str]
    deny: frozenset[str]
    undecided: frozenset[str]
    classes: Mapping[str, Mapping[str, str | None]]  # definition id -> {temperature, rainfall}
    regime: str | None


class ZoneDefinitions(_Frozen):
    version: int
    source_id: str
    document_families: tuple[DocumentFamily, ...]
    definitions: tuple[Definition, ...]

    @model_validator(mode="after")
    def _unique(self) -> ZoneDefinitions:
        ids = [d.id for d in self.definitions]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate definition ids")
        families = {f.family for f in self.document_families}
        seen: set[tuple[str, str]] = set()
        for d in self.definitions:
            if d.family not in families:
                raise ValueError(f"{d.id}: unknown family {d.family!r}")
            for camp in d.campaigns:
                if (d.family, camp) in seen:
                    raise ValueError(f"{d.id}: campaign {camp!r} of family {d.family!r} has two definitions")
                seen.add((d.family, camp))
        return self

    def family_of(self, title: str | None) -> str | None:
        text = fold(title)
        for fam in self.document_families:
            if any(text.startswith(fold(p)) for p in fam.title_prefixes):
                return fam.family
        return None

    def definition_for(self, source_id: str | None, title: str | None, issue: str | None) -> Definition | None:
        if source_id != self.source_id:
            return None
        family = self.family_of(title)
        if family is None or issue is None:
            return None
        for d in self.definitions:
            if d.family == family and issue.strip() in d.campaigns:
                return d
        return None

    def by_id(self, definition_id: str) -> Definition:
        for d in self.definitions:
            if d.id == definition_id:
                return d
        raise KeyError(definition_id)

    def zone_key_for(self, *, source_id: str | None, title: str | None, issue: str | None,
                     crop_eppo: str | None, raw_site: str | None, table: str | None) -> str | None:
        """The unit's ``zoneKey``, or None when no published climatic definition covers it."""
        definition = self.definition_for(source_id, title, issue)
        if definition is None or crop_eppo not in definition.crops or not raw_site:
            return None
        label = fold(raw_site)
        entry = next((e for e in definition.labels if fold(e.label) == label), None)
        if entry is None:
            return None
        if any(fold(x.label) == label and table is not None and table.strip() in x.tables
               for x in definition.except_tables):
            return None
        return zone_key(definition.id, entry.label)

    def classify_parcel(self, *, april_tas_c: float | None, annual_rain_mm: float | None,
                        regime: str | None) -> ParcelZones:
        """Allow/deny/undecided zone keys of a parcel with this climate (``regime``: secano | regadio | None)."""
        allow: set[str] = set()
        deny: set[str] = set()
        undecided: set[str] = set()
        classes: dict[str, dict[str, str | None]] = {}
        values = {"temperature": april_tas_c, "rainfall": annual_rain_mm}
        for d in self.definitions:
            parcel = {axis: d.classify(axis, values[axis]) for axis in AXES}
            classes[d.id] = parcel
            for entry in d.labels:
                key = zone_key(d.id, entry.label)
                verdicts: list[bool | None] = []
                for axis in AXES:
                    if entry.classes(axis):
                        verdicts.append(None if parcel[axis] is None else parcel[axis] in entry.classes(axis))
                if entry.regime is not None:
                    verdicts.append(None if regime is None else regime == entry.regime)
                if any(v is False for v in verdicts):
                    deny.add(key)
                elif any(v is None for v in verdicts):
                    undecided.add(key)
                else:
                    allow.add(key)
        return ParcelZones(frozenset(allow), frozenset(deny), frozenset(undecided), classes, regime)

    def describe_keys(self, keys: Iterable[str]) -> list[dict[str, Any]]:
        """Definition id, citation and zone label of each key (response transparency), sorted."""
        out = []
        for key in sorted(set(keys)):
            definition_id, _, label = key.partition("::")
            try:
                d = self.by_id(definition_id)
            except KeyError:
                continue
            entry = next((e for e in d.labels if fold(e.label) == label), None)
            out.append({
                "definition_id": d.id,
                "zone_label": entry.label if entry else label,
                "citation": f"{d.citation.document}, section {d.citation.section}, PDF page {d.citation.pdf_page}",
            })
        return out


def _prepare(raw: dict[str, Any]) -> dict[str, Any]:
    """Registry spelling -> model spelling (``class`` -> ``class_``; helper sections dropped)."""
    def fix(node: Any) -> Any:
        if isinstance(node, list):
            return [fix(x) for x in node]
        if isinstance(node, dict):
            return {("class_" if k == "class" else k): fix(v) for k, v in node.items()}
        return node
    keep = {k: v for k, v in raw.items() if k not in ("thresholds", "label_sets")}
    return fix(keep)


def load_zone_definitions(path: str | Path | None = None) -> ZoneDefinitions:
    file = Path(path) if path is not None else DEFAULT_ZONE_DEFINITIONS_PATH
    try:
        raw = yaml.safe_load(file.read_text(encoding="utf-8"))
        return ZoneDefinitions.model_validate(_prepare(raw))
    except (OSError, yaml.YAMLError, ValueError) as exc:
        raise ZoneDefinitionError(f"{file.name}: {exc}") from exc


@lru_cache(maxsize=1)
def default_zone_definitions() -> ZoneDefinitions:
    return load_zone_definitions()
