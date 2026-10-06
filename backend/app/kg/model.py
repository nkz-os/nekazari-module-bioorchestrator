"""Canonical row models of the KG ingestion pipeline (MIAPPE-aligned, spec section 2.1).

An adapter turns raw source rows into these typed rows; the contract engine fills the derived
fields from the registries; the gate checks the bundle; the loader writes it. A row says what the
source said and nothing else: a value the source does not give is ``None`` plus an explicit
:class:`Gap` naming the field and the reason, never a guess (no value, basis or metric is inferred
from magnitude).

Two kinds of field, and the split is part of each row's contract (see the ``*_KEY_FIELDS`` /
``*_NON_KEY_FIELDS`` tuples at the bottom, checked at import):

* **observed** fields are what the source printed, as printed. They are the only inputs of the
  deterministic keys in :mod:`app.kg.identity`.
* **derived** fields are normalisations, registry lookups and links. They can change when a
  registry changes; the keys never do.

The models validate shape and internal consistency only. Whether a vocabulary value is registered,
a number is plausible or a source is licensed is the contract engine's and the gate's business, so
that no row model depends on registry data.
"""
from __future__ import annotations

import datetime as dt
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

KEY_PATTERN = r"^[0-9a-f]{64}$"
EPPO_PATTERN = r"^[A-Z0-9]{5,6}$"
SITE_ID_PATTERN = r"^[A-Z]{2}-[A-Z0-9][A-Z0-9-]*$"
SOURCE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
VARIABLE_ID_PATTERN = r"^[a-z][a-z0-9_]*$"

# Id of the ``yield_basis`` vocabulary entry for "at the standard moisture stated by the source".
# That basis is only meaningful together with the moisture percentage the source states (13, 14 or
# 9 % for GENVCE, 15.5 % for CREA): the percentage is its own field and is never collapsed.
STANDARD_MOISTURE_BASIS = "standard_moisture"


def _blank_to_none(value: object) -> object:
    """Blank text is a missing value; anything else is kept exactly as observed."""
    if isinstance(value, str) and not value.strip():
        return None
    return value


# Optional text taken from the source. Kept as printed (no strip, no NFC: that is the job of the
# key), except that blank means missing.
Text = Annotated[str | None, BeforeValidator(_blank_to_none)]
NonBlank = Annotated[str, StringConstraints(pattern=r"\S")]
Sha256Hex = Annotated[str, StringConstraints(pattern=KEY_PATTERN)]
EppoCode = Annotated[str, StringConstraints(pattern=EPPO_PATTERN)]
SourceId = Annotated[str, StringConstraints(pattern=SOURCE_ID_PATTERN)]
SiteId = Annotated[str, StringConstraints(pattern=SITE_ID_PATTERN)]
VariableId = Annotated[str, StringConstraints(pattern=VARIABLE_ID_PATTERN)]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)


class Gap(_Frozen):
    """A field left ``None`` because the source does not give it, with the reason."""

    field: NonBlank
    reason: NonBlank


class _Row(_Frozen):
    gaps: tuple[Gap, ...] = ()

    @model_validator(mode="after")
    def _gaps_name_missing_optional_fields_once(self) -> _Row:
        seen: set[str] = set()
        fields = type(self).model_fields
        for gap in self.gaps:
            if gap.field in seen:
                raise ValueError(f"gap declared twice for field {gap.field!r}")
            seen.add(gap.field)
            info = fields.get(gap.field)
            if info is None or gap.field == "gaps":
                raise ValueError(f"gap names {gap.field!r}, which is not a field of {type(self).__name__}")
            if info.is_required():
                raise ValueError(f"gap names {gap.field!r}, a required field: it cannot be missing")
            if getattr(self, gap.field) not in (None, ()):
                raise ValueError(f"gap names {gap.field!r}, which has a value")
        return self


# ── document ─────────────────────────────────────────────────────────────────

class DocumentRow(_Row):
    """A concrete document of a source (``ArticleSource``): a report, a booklet, a paper.

    Identity is the source, the title as printed, the issue and the year. The URL, the file name and
    the raw-file fingerprint describe where it was read from and may change without it being a
    different document.
    """

    source_id: SourceId
    title: NonBlank
    issue: Text = None
    year: int | None = None
    url: Text = None
    file_name: Text = None
    raw_sha256: Sha256Hex | None = None


# ── study ────────────────────────────────────────────────────────────────────

class StudyRow(_Row):
    """A series of trials of one source (``Study``), e.g. "GENVCE, barley, 2021, rainfed"."""

    source_id: SourceId
    study_type: NonBlank  # study_type vocabulary id
    crop_eppo: EppoCode | None = None
    raw_season: Text = None
    raw_scope: Text = None  # what the source splits the series by (zone, network, regime) as printed
    name: NonBlank  # display label
    year: int | None = None
    design: Text = None


# ── site ─────────────────────────────────────────────────────────────────────

class SiteRow(_Row):
    """A place (``TrialSite``), always keyed by the canonical id of the sites registry."""

    site_id: SiteId
    name: NonBlank
    site_kind: NonBlank  # site_kind vocabulary id: field / aggregate / region
    country: Annotated[str, StringConstraints(pattern=r"^[A-Z]{2}$")]
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)
    coordinate_source: Text = None
    climate_class: Text = None
    source_ids: tuple[SourceId, ...] = ()

    @model_validator(mode="after")
    def _coordinates_are_paired_sourced_and_only_for_fields(self) -> SiteRow:
        if (self.latitude is None) != (self.longitude is None):
            raise ValueError("latitude and longitude come together")
        if self.latitude is not None:
            if self.site_kind != "field":
                raise ValueError(f"a {self.site_kind} site never carries coordinates")
            if not self.coordinate_source:
                raise ValueError("coordinates need their coordinate_source")
        return self


# ── variety ──────────────────────────────────────────────────────────────────

class VarietyRow(_Row):
    """A variety of a crop (``Variety``); ``name`` is the registry name of its alias group."""

    crop_eppo: EppoCode
    name: NonBlank
    registry_id: Text = None
    status: Literal["reviewed", "assumption", "candidate"]
    aliases: tuple[NonBlank, ...] = ()
    source_ids: tuple[SourceId, ...] = ()

    @model_validator(mode="after")
    def _registry_id_and_aliases_are_consistent(self) -> VarietyRow:
        if self.registry_id is not None and not self.registry_id.startswith(f"{self.crop_eppo}:"):
            raise ValueError(f"registry_id {self.registry_id!r} must start with {self.crop_eppo}:")
        if self.aliases and self.status == "candidate":
            raise ValueError("a candidate variety has no aliases: an alias group is assumption or reviewed")
        return self


# ── observation unit ─────────────────────────────────────────────────────────

class FactorLevel(_Frozen):
    """One experimental factor at one level, as the source prints it (``N_dose = 120 kg/ha``)."""

    factor: NonBlank
    level: NonBlank | int | float
    unit: Text = None

    @field_validator("level")
    @classmethod
    def _level_is_not_a_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("a factor level is text or a number, not a boolean")  # noqa: TRY004 - pydantic only turns ValueError into a ValidationError
        return value


class UnitRow(_Row):
    """An observation unit (``ObservationUnit``): one plot, entry or table row of a trial.

    The yield fields are the derived copy of the unit's ``crop_yield`` Observation (amendment E4):
    the Observation is the truth, the loader writes the copy for the hot queries.
    """

    # observed (the only inputs of unit_key)
    source_id: SourceId
    document_key: Sha256Hex
    crop_eppo: EppoCode  # canonical: an EPPO code, not an alias
    raw_variety: Text = None
    raw_site: Text = None
    raw_season: Text = None
    raw_irrigation: Text = None
    raw_production_system: Text = None
    factor_levels: tuple[FactorLevel, ...] = ()
    rootstock: Text = None
    clone: Text = None
    planting_year: int | None = None
    row_discriminator: Text = None  # only when the source itself distinguishes otherwise equal rows

    # derived and provenance (never part of a key)
    study_key: Sha256Hex | None = None
    site_key: SiteId | None = None
    variety_key: Sha256Hex | None = None
    year: int | None = None
    irrigation_regime: Text = None  # stored form of the irrigation vocabulary (AGROVOC URI)
    production_system: Text = None
    purpose: Text = None
    yield_kg_ha: float | None = Field(default=None, ge=0)
    yield_metric: Text = None
    yield_basis: Text = None
    yield_moisture_pct: float | None = Field(default=None, gt=0, lt=100)
    yield_value_original: float | None = None
    yield_unit_original: Text = None
    derivation_method: Text = None
    locator: Text = None  # page / table of the document
    # How irrigation_regime was derived where the source states none (a yield cutoff, see
    # irrigation_thresholds.yaml). Set whenever the rule was evaluated, so a derived regime (or an
    # ambiguous yield, regime None) is never mistaken for an observed one; an observed regime has none.
    irrigation_derivation: Text = None
    irrigation_yield_low_kg_ha: float | None = Field(default=None, gt=0)
    irrigation_yield_high_kg_ha: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _irrigation_derivation_is_complete_or_absent(self) -> UnitRow:
        bounds = (self.irrigation_yield_low_kg_ha, self.irrigation_yield_high_kg_ha)
        if self.irrigation_derivation is None:
            if any(value is not None for value in bounds):
                raise ValueError("irrigation thresholds are set without irrigation_derivation")
            return self
        if self.raw_irrigation is not None:
            raise ValueError("irrigation_derivation is set but the source states a regime: the source wins")
        if any(value is None for value in bounds) or bounds[0] >= bounds[1]:
            raise ValueError("irrigation_derivation needs both thresholds, low below high")
        if self.yield_kg_ha is None:
            raise ValueError("irrigation_derivation is set without a yield to derive from")
        return self

    @model_validator(mode="after")
    def _factor_names_are_unique(self) -> UnitRow:
        seen: set[str] = set()
        for level in self.factor_levels:
            name = level.factor.strip()
            if name in seen:
                raise ValueError(f"factor {name!r} appears twice: one level per factor")
            seen.add(name)
        return self

    @model_validator(mode="after")
    def _yield_is_complete_or_absent(self) -> UnitRow:
        group = {
            "yield_metric": self.yield_metric,
            "yield_basis": self.yield_basis,
            "yield_value_original": self.yield_value_original,
            "yield_unit_original": self.yield_unit_original,
            "purpose": self.purpose,
        }
        if self.yield_kg_ha is None:
            # purpose is a property of the unit, not of the yield: it may stand alone.
            for name in ("yield_metric", "yield_basis", "yield_moisture_pct", "yield_value_original",
                         "yield_unit_original", "derivation_method"):
                if getattr(self, name) is not None:
                    raise ValueError(f"{name} is set without yield_kg_ha")
            return self
        for name, value in group.items():
            if value is None:
                raise ValueError(f"a yield needs {name}")
        _check_moisture(self.yield_basis, self.yield_moisture_pct, "yield_moisture_pct")
        return self


# ── observation ──────────────────────────────────────────────────────────────

class ObservationRow(_Row):
    """One observed value of one variable on one unit (``Observation``).

    Yield is an Observation like any trait. When ``metric`` is set (a yield-like observation) the
    purpose, the basis, the unit and the value as printed are mandatory; ``moisture_pct`` goes with
    the standard-moisture basis and nothing else.
    """

    # observed (the only inputs of obs_key)
    unit_key: Sha256Hex
    variable_id: VariableId
    stage: Text = None
    date: dt.date | None = None
    qualifier: Text = None  # tells apart observations of one variable on one unit (reference variety, period)

    # value and its context
    value: float | None = None
    value_text: Text = None
    unit: Text = None
    basis: Text = None
    moisture_pct: float | None = Field(default=None, gt=0, lt=100)
    metric: Text = None
    purpose: Text = None
    value_original: float | None = None
    unit_original: Text = None
    derivation_method: Text = None
    raw_key: Text = None  # the source's own name for the field, as printed
    locator: Text = None

    @model_validator(mode="after")
    def _one_value(self) -> ObservationRow:
        if (self.value is None) == (self.value_text is None):
            raise ValueError("an observation carries exactly one of value and value_text")
        return self

    @model_validator(mode="after")
    def _yield_context_is_complete(self) -> ObservationRow:
        if self.metric is None:
            if self.purpose is not None:
                raise ValueError("purpose is set without a metric")
        else:
            needed = {
                "value": self.value,
                "unit": self.unit,
                "basis": self.basis,
                "purpose": self.purpose,
                "value_original": self.value_original,
                "unit_original": self.unit_original,
            }
            for name, value in needed.items():
                if value is None:
                    raise ValueError(f"an observation with a metric needs {name}")
        _check_moisture(self.basis, self.moisture_pct, "moisture_pct")
        return self


def _check_moisture(basis: str | None, moisture: float | None, name: str) -> None:
    if basis == STANDARD_MOISTURE_BASIS and moisture is None:
        raise ValueError(f"the {STANDARD_MOISTURE_BASIS} basis needs {name}")
    if basis != STANDARD_MOISTURE_BASIS and moisture is not None:
        raise ValueError(f"{name} only goes with the {STANDARD_MOISTURE_BASIS} basis")


# ── key classification ───────────────────────────────────────────────────────
# Every field of every row is classified: part of the natural key (observed) or not (derived,
# link or provenance). identity.py hashes exactly the key fields; adding a field to a row without
# classifying it fails at import, so a new field can never silently join or leave a key.

DOCUMENT_KEY_FIELDS = ("source_id", "title", "issue", "year")
DOCUMENT_NON_KEY_FIELDS = ("url", "file_name", "raw_sha256", "gaps")

STUDY_KEY_FIELDS = ("source_id", "study_type", "crop_eppo", "raw_season", "raw_scope")
STUDY_NON_KEY_FIELDS = ("name", "year", "design", "gaps")

SITE_KEY_FIELDS = ("site_id",)
SITE_NON_KEY_FIELDS = (
    "name", "site_kind", "country", "latitude", "longitude", "coordinate_source", "climate_class",
    "source_ids", "gaps",
)

VARIETY_KEY_FIELDS = ("crop_eppo", "name")
VARIETY_NON_KEY_FIELDS = ("registry_id", "status", "aliases", "source_ids", "gaps")

UNIT_KEY_FIELDS = (
    "source_id", "document_key", "crop_eppo", "raw_variety", "raw_site", "raw_season", "raw_irrigation",
    "raw_production_system", "factor_levels", "rootstock", "clone", "planting_year", "row_discriminator",
)
UNIT_NON_KEY_FIELDS = (
    "study_key", "site_key", "variety_key", "year", "irrigation_regime", "production_system", "purpose",
    "yield_kg_ha", "yield_metric", "yield_basis", "yield_moisture_pct", "yield_value_original",
    "yield_unit_original", "derivation_method", "locator", "irrigation_derivation",
    "irrigation_yield_low_kg_ha", "irrigation_yield_high_kg_ha", "gaps",
)

OBSERVATION_KEY_FIELDS = ("unit_key", "variable_id", "stage", "date", "qualifier")
OBSERVATION_NON_KEY_FIELDS = (
    "value", "value_text", "unit", "basis", "moisture_pct", "metric", "purpose", "value_original",
    "unit_original", "derivation_method", "raw_key", "locator", "gaps",
)

_CLASSIFICATION: dict[type[_Row], tuple[tuple[str, ...], tuple[str, ...]]] = {
    DocumentRow: (DOCUMENT_KEY_FIELDS, DOCUMENT_NON_KEY_FIELDS),
    StudyRow: (STUDY_KEY_FIELDS, STUDY_NON_KEY_FIELDS),
    SiteRow: (SITE_KEY_FIELDS, SITE_NON_KEY_FIELDS),
    VarietyRow: (VARIETY_KEY_FIELDS, VARIETY_NON_KEY_FIELDS),
    UnitRow: (UNIT_KEY_FIELDS, UNIT_NON_KEY_FIELDS),
    ObservationRow: (OBSERVATION_KEY_FIELDS, OBSERVATION_NON_KEY_FIELDS),
}


def _check_classification() -> None:
    for row, (key_fields, non_key_fields) in _CLASSIFICATION.items():
        classified = [*key_fields, *non_key_fields]
        if len(classified) != len(set(classified)) or set(classified) != set(row.model_fields):
            raise RuntimeError(
                f"{row.__name__}: every field must be classified exactly once as key or non-key; "
                f"unclassified={sorted(set(row.model_fields) - set(classified))}, "
                f"unknown={sorted(set(classified) - set(row.model_fields))}"
            )


_check_classification()

__all__ = [
    "STANDARD_MOISTURE_BASIS",
    "DocumentRow",
    "FactorLevel",
    "Gap",
    "ObservationRow",
    "SiteRow",
    "StudyRow",
    "UnitRow",
    "VarietyRow",
]
