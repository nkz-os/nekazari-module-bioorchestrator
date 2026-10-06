"""Source contracts: the declarative mapping from a source's raw rows to the canonical KG rows.

A contract is a closed YAML document (pydantic v2, ``extra="forbid"``: an unknown key is an error,
never ignored). :func:`run_contract` applies it to the raw rows an adapter produced and returns a
:class:`Bundle` of canonical rows, or fails; it never drops a raw field silently.

Contract shape (every key is validated; ``from`` is a dotted path into the raw row)::

    source_id: GENVCE                      # registered in sources.yaml
    raw: {repo: ..., paths: [...], extraction_version: ...}
    adapter: app.kg.adapters.genvce        # dotted module path; the engine does not import it
    document:                              # provenance of each raw row
      title: {from: doc.title}             # required; issue, year, url, file_name, raw_sha256 optional
      locator: [{label: page, from: doc.page}, {label: table, from: doc.table}]
    study:
      type: variety                        # study_type vocabulary id
      group_by: [network]                  # raw fields the series is split by; raw_scope = their printed values
    unit:
      fields:                              # observed fields, kept as printed (they are the unit's key)
        crop: {from: crop}                 # required; resolved through the crops registry
        variety: {from: variety}
        site: {from: zone}                 # an OBSERVED zone, stratum or place; never a row index
        season: {from: year}
        irrigation: {from: irrigation, vocab: irrigation}
        production_system: {from: production_system, vocab: production_system}
        # also rootstock, clone, planting_year, row_discriminator
      factors: [{factor: n_dose, from: n_dose, unit: kg/ha}]
      purpose: {default: grain, justification: "<quote>"}      # required whenever `yield` is declared
      yield:
        from: yield_kg_ha
        unit: kg/ha                        # as the source prints it (or q/ha, ...)
        metric: {default: grain, justification: "<quote>"}
        basis: {default: standard_moisture, justification: "<quote or 'unknown'>"}
        moisture_pct:                      # needed by the standard_moisture basis; never one constant
          by_crop:
            HORVX: {default: 13, justification: "<quote>"}
            ZEAMX: {default: 14, justification: "<quote>"}
    observations:
      - {from: quality.protein_pct, variable: grain_protein_content, unit: "%", qualifier: {from: ref}}
    ignore:
      - {field: _validation, reason: "extraction metadata"}      # a reason is mandatory
    sites: {aggregate_patterns: ["^Media \\d+ "]}
    expected: {units: 3862, observations: 9000, sites: 4}

A value the contract states itself is a ``default`` and needs its ``justification`` (the quote of
the source that says so, or ``unknown``): metric, basis, moisture and purpose are never inferred
from magnitude or from the crop. ``by_crop`` states a different justified value per canonical crop,
because the moisture of the standard basis differs by crop (13, 14 or 9 % for GENVCE, 15.5 % for
CREA) and must never be collapsed into one constant.

What the engine does, in order:

1. validates the contract against the registries (source, variables, units and their dimensions,
   vocabularies, crops); any problem is a :class:`ContractError` before a single row is read;
2. refuses the build with :class:`UnmappedFieldError` when a raw field is neither mapped nor ignored;
3. maps every row (:func:`run_contract`): raw values kept as printed, canonical values derived from
   the registries, units converted keeping ``value_original`` and ``unit_original``, vocabularies
   applied, keys built with :mod:`app.kg.identity`; a missing value is ``None`` plus a ``Gap``;
4. yield produces BOTH an ``ObservationRow`` (the truth) and the unit's derived yield copy (E4);
5. reports, never hides: unresolved sites and vocabulary values, unregistered varieties, gap and
   missing-field counts, collapsed duplicates and the range checks performed and SKIPPED.

Plausibility *severity* (warning or error, review queues, licence) stays with the gate; the engine
only measures and reports.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from .model import SourceId, VariableId

# The variable the ``unit.yield`` section produces; it cannot also be listed under ``observations``.
YIELD_VARIABLE = "crop_yield"

_EPPO = r"^[A-Z0-9]{5,6}$"


class ContractError(ValueError):
    """The contract is invalid, or inconsistent with the registries."""


class UnmappedFieldError(ContractError):
    """A raw field is neither mapped nor ignored by the contract: the build stops (nothing is dropped)."""

    def __init__(self, fields: Mapping[str, int]) -> None:
        self.fields: dict[str, int] = dict(sorted(fields.items()))
        listed = ", ".join(f"{path} (in {rows} rows)" for path, rows in self.fields.items())
        super().__init__(
            f"{len(self.fields)} raw field(s) neither mapped nor ignored by the contract: {listed}; "
            "map each to a unit field or observation, or list it under `ignore` with a reason")


class ContractDataError(ContractError):
    """One or more raw rows cannot be mapped under the contract (values are never guessed)."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors: tuple[str, ...] = tuple(errors)
        shown = "\n  ".join(self.errors[:20])
        more = f"\n  ... and {len(self.errors) - 20} more" if len(self.errors) > 20 else ""
        super().__init__(f"{len(self.errors)} raw row problem(s):\n  {shown}{more}")


class ExpectedCountError(ContractError):
    """The bundle's counts differ from the counts the contract declares it must produce."""


# ═════════════════════════════════════════════════════════════════════════════
# schema
# ═════════════════════════════════════════════════════════════════════════════

def _valid_path(value: str) -> str:
    if any(not part or part != part.strip() for part in value.split(".")):
        raise ValueError(f"{value!r} is not a raw field path (dot-separated, no blank segment)")
    return value


def _valid_regex(value: str) -> str:
    try:
        re.compile(value)
    except re.error as exc:
        raise ValueError(f"{value!r} is not a valid regular expression: {exc}") from exc
    return value


RawPath = Annotated[str, AfterValidator(_valid_path)]
NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Regex = Annotated[str, AfterValidator(_valid_regex)]
Scalar = StrictStr | StrictInt | StrictFloat


class _Closed(BaseModel):
    """Closed, immutable: an unknown key is rejected."""

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class RawSpec(_Closed):
    repo: NonBlank
    paths: tuple[NonBlank, ...] = Field(min_length=1)
    extraction_version: NonBlank


class Ref(_Closed):
    """A value read from one raw field, as printed."""

    from_: RawPath = Field(alias="from")


class LocatorPart(_Closed):
    label: NonBlank
    from_: RawPath = Field(alias="from")


class DocumentSpec(_Closed):
    title: Ref
    issue: Ref | None = None
    year: Ref | None = None
    url: Ref | None = None
    file_name: Ref | None = None
    raw_sha256: Ref | None = None
    locator: tuple[LocatorPart, ...] = ()


class StudySpec(_Closed):
    type: NonBlank  # study_type vocabulary id
    design: NonBlank | None = None
    group_by: tuple[RawPath, ...] = ()


class Fixed(_Closed):
    default: Scalar
    justification: str | None = None


class Declared(_Closed):
    """A value read from a raw field, fixed by the contract, or fixed per canonical crop.

    Exactly one of ``from``, ``default`` (with its ``justification``) or ``by_crop``. A fixed value is
    a statement of the contract, not of the row: where the field is one of metric, basis, moisture or
    purpose, the justification is mandatory (checked by the section that owns the field).
    """

    from_: RawPath | None = Field(default=None, alias="from")
    default: Scalar | None = None
    justification: str | None = None
    by_crop: dict[Annotated[str, StringConstraints(pattern=_EPPO)], Fixed] | None = None

    @model_validator(mode="after")
    def _exactly_one_source(self) -> Declared:
        given = [name for name, value in (("from", self.from_), ("default", self.default),
                                          ("by_crop", self.by_crop)) if value is not None]
        if len(given) != 1:
            raise ValueError(f"declare exactly one of from, default or by_crop (got {given or 'none'})")
        if self.justification is not None and self.default is None:
            raise ValueError("a justification only goes with a default")
        if self.by_crop is not None and not self.by_crop:
            raise ValueError("by_crop is empty")
        return self

    def fixed_values(self) -> tuple[Fixed, ...]:
        if self.default is not None:
            return (Fixed(default=self.default, justification=self.justification),)
        return tuple(self.by_crop.values()) if self.by_crop else ()


def _require_justified(name: str, declared: Declared | None) -> None:
    if declared is None:
        return
    for fixed in declared.fixed_values():
        if not (fixed.justification or "").strip():
            raise ValueError(f"{name}: a default without a justification is rejected "
                             "(quote the source, or write 'unknown')")


class Factor(_Closed):
    factor: NonBlank
    from_: RawPath = Field(alias="from")
    unit: NonBlank | None = None


class IrrigationField(_Closed):
    from_: RawPath = Field(alias="from")
    vocab: Literal["irrigation"]


class ProductionSystemField(_Closed):
    from_: RawPath = Field(alias="from")
    vocab: Literal["production_system"]


class UnitFields(_Closed):
    crop: Ref
    variety: Ref | None = None
    site: Ref | None = None
    season: Ref | None = None
    irrigation: IrrigationField | None = None
    production_system: ProductionSystemField | None = None
    rootstock: Ref | None = None
    clone: Ref | None = None
    planting_year: Ref | None = None
    row_discriminator: Ref | None = None


class YieldSpec(_Closed):
    from_: RawPath = Field(alias="from")
    unit: NonBlank
    metric: Declared
    basis: Declared
    moisture_pct: Declared | None = None
    derivation_method: NonBlank | None = None

    @model_validator(mode="after")
    def _defaults_are_justified(self) -> YieldSpec:
        _require_justified("yield.metric", self.metric)
        _require_justified("yield.basis", self.basis)
        _require_justified("yield.moisture_pct", self.moisture_pct)
        return self


class UnitSpec(_Closed):
    fields: UnitFields
    factors: tuple[Factor, ...] = ()
    purpose: Declared | None = None
    yield_: YieldSpec | None = Field(default=None, alias="yield")

    @model_validator(mode="after")
    def _yield_has_a_justified_purpose(self) -> UnitSpec:
        _require_justified("purpose", self.purpose)
        if self.yield_ is not None and self.purpose is None:
            raise ValueError("a contract with a yield must declare the unit purpose (grain or forage) "
                             "with its justification: every yield carries a purpose")
        names = [factor.factor for factor in self.factors]
        if len(names) != len(set(names)):
            raise ValueError("a factor is declared twice")
        return self


class ObservationSpec(_Closed):
    from_: RawPath = Field(alias="from")
    variable: VariableId
    unit: NonBlank | None = None  # as the source prints it; required when the variable has a unit
    stage: Declared | None = None
    date: Ref | None = None
    qualifier: Declared | None = None  # tells apart observations of one variable on one unit
    derivation_method: NonBlank | None = None

    @field_validator("variable")
    @classmethod
    def _yield_has_its_own_section(cls, value: str) -> str:
        if value == YIELD_VARIABLE:
            raise ValueError(f"{YIELD_VARIABLE} comes from the unit.yield section, not from observations")
        return value


class IgnoreSpec(_Closed):
    field: RawPath
    reason: NonBlank


class SitesSpec(_Closed):
    # raw site names matching one of these are aggregates (zone, network, average): they must resolve
    # to a registry site that is not a field site, so an aggregate is never disguised as a plot
    aggregate_patterns: tuple[Regex, ...] = ()


class Expected(_Closed):
    units: int = Field(ge=0)
    observations: int = Field(ge=0)
    sites: int = Field(ge=0)


def _covers(prefix: str, path: str) -> bool:
    return path == prefix or path.startswith(prefix + ".")


class Contract(_Closed):
    source_id: SourceId
    raw: RawSpec
    adapter: Annotated[str, StringConstraints(pattern=r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")]
    document: DocumentSpec
    study: StudySpec
    unit: UnitSpec
    observations: tuple[ObservationSpec, ...] = ()
    ignore: tuple[IgnoreSpec, ...] = ()
    sites: SitesSpec = SitesSpec()
    expected: Expected

    def mapped_paths(self) -> frozenset[str]:
        """Every raw path the contract reads."""
        paths: set[str] = set()

        def add(*specs: Any) -> None:
            for spec in specs:
                if spec is not None and spec.from_ is not None:
                    paths.add(spec.from_)

        document = self.document
        add(document.title, document.issue, document.year, document.url, document.file_name,
            document.raw_sha256, *document.locator)
        paths.update(self.study.group_by)
        fields = self.unit.fields
        add(fields.crop, fields.variety, fields.site, fields.season, fields.irrigation,
            fields.production_system, fields.rootstock, fields.clone, fields.planting_year,
            fields.row_discriminator, *self.unit.factors)
        declared: list[Declared | None] = [self.unit.purpose]
        if self.unit.yield_ is not None:
            add(self.unit.yield_)
            declared += [self.unit.yield_.metric, self.unit.yield_.basis, self.unit.yield_.moisture_pct]
        for observation in self.observations:
            add(observation, observation.date)
            declared += [observation.stage, observation.qualifier]
        add(*declared)
        return frozenset(paths)

    @model_validator(mode="after")
    def _paths_are_consistent(self) -> Contract:
        sources = [obs.from_ for obs in self.observations]
        if self.unit.yield_ is not None:
            sources.append(self.unit.yield_.from_)
        repeated = sorted(path for path, count in Counter(sources).items() if count > 1)
        if repeated:
            raise ValueError(f"raw field(s) read as a value more than once: {repeated}")
        ignored = [entry.field for entry in self.ignore]
        if len(ignored) != len(set(ignored)):
            raise ValueError("a field is ignored twice")
        mapped = self.mapped_paths()
        clash = sorted(path for path in mapped for entry in ignored if _covers(entry, path))
        if clash:
            raise ValueError(f"field(s) both mapped and ignored: {clash}")
        return self


def contract_hash(contract: Contract) -> str:
    """sha256 of the canonical JSON of the contract (recorded in the build manifest)."""
    text = json.dumps(contract.model_dump(mode="json", by_alias=True), sort_keys=True,
                      ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_contract(path: str | Path) -> Contract:
    """Read and validate a contract file; every problem is a :class:`ContractError`."""
    file = Path(path)
    try:
        data = yaml.safe_load(file.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ContractError(f"{file.name}: cannot read the contract: {exc}") from exc
    try:
        return Contract.model_validate(data)
    except ValidationError as exc:
        raise ContractError(f"{file.name}: invalid contract:\n{exc}") from exc
