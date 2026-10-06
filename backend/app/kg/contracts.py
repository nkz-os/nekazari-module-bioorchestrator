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

import datetime as dt
import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
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

from . import identity
from .model import (
    STANDARD_MOISTURE_BASIS,
    DocumentRow,
    FactorLevel,
    Gap,
    ObservationRow,
    SiteRow,
    SourceId,
    StudyRow,
    UnitRow,
    VariableId,
    VarietyRow,
)
from .registries import (
    IRRIGATION_DERIVATION_V1,
    Registries,
    Site,
    Unit,
    UnknownEntryError,
    Variable,
)

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


class IrrigationDerivationSpec(_Closed):
    """Derive the irrigation regime from the unit's yield where the source states none.

    The cutoffs are per-crop data (``irrigation_thresholds.yaml``), never part of the contract. The
    source always wins; the derived regime is stored on the canonical field only and the unit records
    the method and the cutoffs it used.
    """

    method: Literal["yield_threshold_v1"]


class UnitSpec(_Closed):
    fields: UnitFields
    factors: tuple[Factor, ...] = ()
    purpose: Declared | None = None
    yield_: YieldSpec | None = Field(default=None, alias="yield")
    irrigation_derivation: IrrigationDerivationSpec | None = None

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


# ═════════════════════════════════════════════════════════════════════════════
# the bundle (what the engine returns)
# ═════════════════════════════════════════════════════════════════════════════

class _Out(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RangeFinding(_Out):
    """An observation outside the plausible range that applied to it (severity is the gate's call)."""

    obs_key: str
    unit_key: str
    crop_eppo: str
    variable_id: str
    value: float
    range_id: str
    range_min: float
    range_max: float
    range_status: Literal["reviewed", "assumption"]


class RangeChecks(_Out):
    """The range checks of a run: performed, and SKIPPED with the reason (a skipped check is no check).

    ``evaluated`` observations had an applicable range; ``skipped`` were numeric ratio or percent
    values for which no range applied: ``no_range_for_crop_variable`` (the registry has none for that
    crop and variable) or ``no_matching_conditions`` (it has, but none matches the unit's irrigation,
    production system or purpose). ``not_applicable`` are observations no range can judge (ordinal,
    text, date scales).
    """

    evaluated: int = 0
    in_range: int = 0
    out_of_range: tuple[RangeFinding, ...] = ()
    skipped: int = 0
    skipped_by_reason: dict[str, int] = {}
    not_applicable: int = 0


class BuildReport(_Out):
    """What the engine saw, for the manifest and the gate: nothing here is a silent decision."""

    source_id: str
    contract_hash: str
    registries_hash: str
    raw_rows: int
    collapsed_duplicate_units: int = 0
    collapsed_duplicate_observations: int = 0
    gaps: dict[str, int] = {}  # "unit.raw_site" -> rows left without a value, by row kind and field
    missing: dict[str, int] = {}  # mapped raw path -> rows where it was absent or blank
    unresolved_sites: dict[str, int] = {}  # raw site as printed -> units
    unresolved_vocab: dict[str, int] = {}  # "irrigation:<literal>" -> units
    irrigation_derived: dict[str, int] = {}  # "<EPPO>:rainfed|irrigated|ambiguous" -> units the yield cutoff judged
    unregistered_varieties: dict[str, int] = {}  # "<EPPO>:<name>" -> units (the review queue)
    range_checks: RangeChecks = RangeChecks()
    warnings: tuple[str, ...] = ()


class Bundle(_Out):
    """The canonical rows of one source, sorted by key (so the raw row order never matters).

    Keys are not stored on the rows; derive them with :mod:`app.kg.identity` (``unit_key(unit)`` and
    so on). ``unit.document_key``, ``unit.study_key``, ``unit.site_key``, ``unit.variety_key`` and
    ``observation.unit_key`` already link the rows.
    """

    source_id: str
    documents: tuple[DocumentRow, ...]
    studies: tuple[StudyRow, ...]
    sites: tuple[SiteRow, ...]
    varieties: tuple[VarietyRow, ...]
    units: tuple[UnitRow, ...]
    observations: tuple[ObservationRow, ...]
    report: BuildReport


# ═════════════════════════════════════════════════════════════════════════════
# validation against the registries
# ═════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class _UnitMap:
    """The unit a source prints, and the registry unit of the variable it is converted to."""

    printed: str
    code: str
    target: str


@dataclass(frozen=True)
class _ObsPlan:
    spec: ObservationSpec
    variable: Variable
    unit: _UnitMap | None


@dataclass(frozen=True)
class _Plan:
    contract: Contract
    registries: Registries
    study_type: str
    yield_variable: Variable | None
    yield_unit: _UnitMap | None
    observations: tuple[_ObsPlan, ...]
    aggregate_patterns: tuple[re.Pattern[str], ...]
    warnings: tuple[str, ...]


def _resolve_unit(
    registries: Registries, source_id: str, printed: str | None, variable: Variable, where: str,
    problems: list[str],
) -> _UnitMap | None:
    if variable.unit is None:
        if printed is not None:
            problems.append(f"{where}: variable {variable.id} has no unit; remove `unit`")
        return None
    if printed is None:
        problems.append(f"{where}: variable {variable.id} is measured in {variable.unit}: "
                        "state the unit the source prints in `unit`")
        return None
    unit: Unit | None = registries.source_unit(source_id, printed)
    if unit is None:
        try:
            unit = registries.unit(printed)
        except UnknownEntryError:
            problems.append(f"{where}: unit {printed!r} is neither a {source_id} source unit nor a registered unit")
            return None
    target = registries.unit(variable.unit)
    if unit.dimension != target.dimension:
        problems.append(f"{where}: unit {printed!r} ({unit.dimension}) cannot be converted to "
                        f"{target.code} ({target.dimension}) of variable {variable.id}: dimension mismatch")
        return None
    return _UnitMap(printed=printed, code=unit.code, target=target.code)


def _check_declared(
    registries: Registries, declared: Declared | None, name: str, problems: list[str], *,
    vocab: str | None = None, percent: bool = False,
) -> None:
    if declared is None:
        return
    for crop in declared.by_crop or {}:
        entry = registries.crop(crop)
        if entry is None or entry.eppo != crop:
            problems.append(f"{name}: by_crop key {crop!r} is not a canonical EPPO code of the crops registry")
    for fixed in declared.fixed_values():
        value = fixed.default
        if percent:
            if isinstance(value, str) or not 0 < value < 100:
                problems.append(f"{name}: {value!r} is not a moisture percentage between 0 and 100")
        elif vocab is not None and registries.vocab_entry(vocab, str(value)) is None:
            problems.append(f"{name}: {value!r} is not in the {vocab} vocabulary")


def _vocab_id(registries: Registries, kind: str, value: Any) -> str | None:
    entry = registries.vocab_entry(kind, value if isinstance(value, str) else str(value))
    return entry.id if entry else None


def _check_yield_coherence(registries: Registries, spec: YieldSpec, purpose: Declared | None,
                           problems: list[str]) -> None:
    """Static contradictions between metric, purpose, basis and moisture, caught before any row."""
    if spec.metric.default is not None and purpose is not None and purpose.default is not None:
        metric = registries.vocab_entry("yield_metric", str(spec.metric.default))
        wanted = _vocab_id(registries, "purpose", purpose.default)
        if metric is not None and wanted is not None and metric.purpose != wanted:
            problems.append(f"yield: metric {metric.id!r} yields purpose {metric.purpose!r}, but the contract "
                            f"declares purpose {wanted!r}: the declarations contradict")
    basis_ids: dict[str | None, str | None] = {}
    if spec.basis.default is not None:
        basis_ids[None] = _vocab_id(registries, "yield_basis", spec.basis.default)
    elif spec.basis.by_crop is not None:
        for crop, fixed in spec.basis.by_crop.items():
            basis_ids[crop] = _vocab_id(registries, "yield_basis", fixed.default)
    moisture = spec.moisture_pct
    for crop, basis in basis_ids.items():
        where = "yield" if crop is None else f"yield (crop {crop})"
        if basis == STANDARD_MOISTURE_BASIS:
            covered = moisture is not None and (moisture.by_crop is None or crop in moisture.by_crop
                                                or crop is None)
            if not covered:
                problems.append(f"{where}: the {STANDARD_MOISTURE_BASIS} basis needs its moisture_pct, "
                                "declared with a justification or read from a raw field")
        elif basis is not None and moisture is not None and crop is None:
            problems.append(f"{where}: moisture_pct only goes with the {STANDARD_MOISTURE_BASIS} basis")


def _compile(contract: Contract, registries: Registries) -> _Plan:
    """Check the contract against the registries and resolve what the row mapping needs."""
    problems: list[str] = []
    warnings: list[str] = []
    source_id = contract.source_id

    try:
        registries.source(source_id)
    except UnknownEntryError as exc:
        problems.append(str(exc))

    study_entry = registries.vocab_entry("study_type", contract.study.type)
    if study_entry is None:
        problems.append(f"study: {contract.study.type!r} is not in the study_type vocabulary")

    unit_spec = contract.unit
    if unit_spec.irrigation_derivation is not None:
        if unit_spec.fields.irrigation is None:
            problems.append("irrigation_derivation: the contract reads no irrigation field, so 'the source states "
                            "none' cannot be told from 'the source was not read'")
        if unit_spec.yield_ is None:
            problems.append("irrigation_derivation: needs a unit yield to derive from")
    purpose = unit_spec.purpose
    _check_declared(registries, purpose, "purpose", problems, vocab="purpose")

    yield_variable: Variable | None = None
    yield_unit: _UnitMap | None = None
    if unit_spec.yield_ is not None:
        spec = unit_spec.yield_
        try:
            yield_variable = registries.variable(YIELD_VARIABLE)
        except UnknownEntryError as exc:
            problems.append(f"yield: {exc}")
        else:
            yield_unit = _resolve_unit(registries, source_id, spec.unit, yield_variable, "yield", problems)
        _check_declared(registries, spec.metric, "yield.metric", problems, vocab="yield_metric")
        _check_declared(registries, spec.basis, "yield.basis", problems, vocab="yield_basis")
        _check_declared(registries, spec.moisture_pct, "yield.moisture_pct", problems, percent=True)
        _check_yield_coherence(registries, spec, purpose, problems)

    observations: list[_ObsPlan] = []
    for index, spec in enumerate(contract.observations):
        where = f"observations[{index}] ({spec.from_})"
        try:
            variable = registries.variable(spec.variable)
        except UnknownEntryError as exc:
            problems.append(f"{where}: {exc}")
            continue
        unit = _resolve_unit(registries, source_id, spec.unit, variable, where, problems)
        observations.append(_ObsPlan(spec=spec, variable=variable, unit=unit))
        evidence = registries.variable_for_raw_key(source_id, spec.from_.rsplit(".", 1)[-1])
        if evidence is not None and evidence.id != variable.id:
            warnings.append(f"{where}: the contract maps it to {variable.id}, but the registry's discovery "
                            f"evidence says the raw key publishes {evidence.id}")
        _check_declared(registries, spec.stage, f"{where}.stage", problems)
        _check_declared(registries, spec.qualifier, f"{where}.qualifier", problems)

    if problems:
        raise ContractError(
            f"contract {source_id} is inconsistent with the registries:\n  - " + "\n  - ".join(problems))
    assert study_entry is not None
    return _Plan(
        contract=contract, registries=registries, study_type=study_entry.id, yield_variable=yield_variable,
        yield_unit=yield_unit, observations=tuple(observations),
        aggregate_patterns=tuple(re.compile(pattern, re.IGNORECASE) for pattern in contract.sites.aggregate_patterns),
        warnings=tuple(warnings),
    )


# ═════════════════════════════════════════════════════════════════════════════
# raw rows
# ═════════════════════════════════════════════════════════════════════════════

class _RowProblem(Exception):
    """A raw row cannot be mapped; the message says why (collected into ContractDataError)."""


def _flatten(row: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    """Leaf values keyed by dotted path. An empty mapping has no leaves; lists are leaves."""
    if not isinstance(row, Mapping):
        raise _RowProblem(f"a raw row must be a mapping, got {type(row).__name__}")
    leaves: dict[str, Any] = {}
    for key, value in row.items():
        if not isinstance(key, str) or not key.strip() or "." in key or key != key.strip():
            raise _RowProblem(f"raw key {key!r} cannot be addressed as a path (blank, padded or containing '.')")
        path = f"{prefix}{key}"
        if isinstance(value, Mapping):
            leaves.update(_flatten(value, f"{path}."))
        else:
            leaves[path] = value
    return leaves


def _get(flat: Mapping[str, Any], path: str) -> Any:
    """The raw value at a path; absent, None and blank text are all 'missing' (None)."""
    value = flat.get(path)
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return value


def _text(value: Any, what: str) -> str:
    """A raw value as text, as printed. Numbers print the way identity canonicalises them."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise _RowProblem(f"{what}: {value!r} is not text or a number")
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    if not math.isfinite(value):
        raise _RowProblem(f"{what}: {value!r} is not finite")
    return str(int(value)) if value.is_integer() else repr(value)


def _number(value: Any, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _RowProblem(f"{what}: {value!r} is not a number")
    number = float(value)
    if not math.isfinite(number):
        raise _RowProblem(f"{what}: {value!r} is not finite")
    return number


def _integer(value: Any, what: str) -> int:
    if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
        return int(value.strip())
    number = _number(value, what)
    if not number.is_integer():
        raise _RowProblem(f"{what}: {value!r} is not a whole number")
    return int(number)


def _date(value: Any, what: str) -> dt.date:
    if isinstance(value, dt.datetime):
        raise _RowProblem(f"{what}: {value!r} is a date and time; the observation date is a calendar date")
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value.strip())
        except ValueError:
            pass
    raise _RowProblem(f"{what}: {value!r} is not an ISO date (YYYY-MM-DD)")


def _single_year(season: str | None) -> int | None:
    if season is not None and re.fullmatch(r"\d{4}", season.strip()):
        return int(season.strip())
    return None


class _Gaps:
    """Why each field of a row is missing: the first reason recorded for a field wins."""

    def __init__(self) -> None:
        self._reasons: dict[str, str] = {}

    def add(self, field_name: str, reason: str) -> None:
        self._reasons.setdefault(field_name, reason)

    def rows(self, values: Mapping[str, Any]) -> tuple[Gap, ...]:
        """Gaps for the recorded fields that really are empty, in a fixed order."""
        return tuple(Gap(field=name, reason=reason) for name, reason in sorted(self._reasons.items())
                     if values.get(name) in (None, ()))


def _absent(path: str) -> str:
    return f"raw field {path!r} is absent or blank in the source row"


def _validation_text(exc: ValidationError) -> str:
    return "; ".join(f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}" for error in exc.errors())


def _declared_raw(declared: Declared, flat: Mapping[str, Any], crop: str, name: str) -> Any:
    """The raw value a declaration gives for this row, or None when its raw field is missing."""
    if declared.from_ is not None:
        return _get(flat, declared.from_)
    if declared.default is not None:
        return declared.default
    assert declared.by_crop is not None
    fixed = declared.by_crop.get(crop)
    if fixed is None:
        raise _RowProblem(f"the contract declares no {name} for crop {crop}")
    return fixed.default


def _differences(first: Mapping[str, Any], second: Mapping[str, Any]) -> list[str]:
    return sorted(name for name in first if first[name] != second[name])


# ═════════════════════════════════════════════════════════════════════════════
# the row mapping
# ═════════════════════════════════════════════════════════════════════════════

class _Builder:
    def __init__(self, plan: _Plan) -> None:
        self.plan = plan
        self.registries = plan.registries
        self.contract = plan.contract
        self.documents: dict[str, tuple[int, DocumentRow]] = {}
        self.studies: dict[str, StudyRow] = {}
        self.sites: dict[str, SiteRow] = {}
        self.varieties: dict[str, list[VarietyRow]] = {}
        self.unit_rows: dict[str, tuple[int, UnitRow]] = {}
        self.obs_rows: dict[str, tuple[int, ObservationRow]] = {}
        self.problems: list[str] = []
        self.missing: Counter[str] = Counter()
        self.unresolved_sites: Counter[str] = Counter()
        self.unresolved_vocab: Counter[str] = Counter()
        self.irrigation_derived: Counter[str] = Counter()
        self.unregistered: Counter[str] = Counter()  # variety_key -> units
        self.collapsed_units = 0
        self.collapsed_observations = 0

    # ── small readers (record what is missing) ──────────────────────────────

    def _take_text(self, flat: Mapping[str, Any], ref: Any, gaps: _Gaps, field_name: str) -> str | None:
        if ref is None:
            return None
        value = _get(flat, ref.from_)
        if value is None:
            self.missing[ref.from_] += 1
            gaps.add(field_name, _absent(ref.from_))
            return None
        return _text(value, ref.from_)

    def _take_int(self, flat: Mapping[str, Any], ref: Any, gaps: _Gaps, field_name: str) -> int | None:
        if ref is None:
            return None
        value = _get(flat, ref.from_)
        if value is None:
            self.missing[ref.from_] += 1
            gaps.add(field_name, _absent(ref.from_))
            return None
        return _integer(value, ref.from_)

    def _vocab(self, kind: str, raw: str | None, field_name: str, gaps: _Gaps) -> str | None:
        """The stored form of a raw vocabulary literal; None plus a reported gap when unrecognised."""
        if raw is None:
            return None
        stored = self.registries.vocab(kind, raw)
        if stored is None:
            self.unresolved_vocab[f"{kind}:{raw}"] += 1
            gaps.add(field_name, f"{raw!r} is not in the {kind} vocabulary")
        return stored

    # ── one raw row ─────────────────────────────────────────────────────────

    def add_row(self, index: int, flat: Mapping[str, Any]) -> None:
        contract, registries = self.contract, self.registries
        fields = contract.unit.fields
        source_id = contract.source_id

        crop_value = _get(flat, fields.crop.from_)
        if crop_value is None:
            raise _RowProblem(f"crop: raw field {fields.crop.from_!r} is absent or blank")
        crop = registries.crop(_text(crop_value, fields.crop.from_))
        if crop is None:
            raise _RowProblem(f"crop {crop_value!r} is not in the crops registry (add the alias there)")
        eppo = crop.eppo

        document_key_, locator = self._document(index, flat)

        gaps = _Gaps()
        raw_variety = self._take_text(flat, fields.variety, gaps, "raw_variety")
        raw_site = self._take_text(flat, fields.site, gaps, "raw_site")
        raw_season = self._take_text(flat, fields.season, gaps, "raw_season")
        raw_irrigation = self._take_text(flat, fields.irrigation, gaps, "raw_irrigation")
        raw_system = self._take_text(flat, fields.production_system, gaps, "raw_production_system")
        rootstock = self._take_text(flat, fields.rootstock, gaps, "rootstock")
        clone = self._take_text(flat, fields.clone, gaps, "clone")
        planting_year = self._take_int(flat, fields.planting_year, gaps, "planting_year")
        discriminator = self._take_text(flat, fields.row_discriminator, gaps, "row_discriminator")
        levels = self._factor_levels(flat)

        year = _single_year(raw_season)
        if raw_season is not None and year is None:
            gaps.add("year", f"season {raw_season!r} is not a single calendar year")

        study_key_ = self._study(flat, eppo, raw_season, year)
        site_id = self._site(raw_site, gaps)
        variety_key_ = self._variety(eppo, raw_variety)
        irrigation = self._vocab("irrigation", raw_irrigation, "irrigation_regime", gaps)
        production_system = self._vocab("production_system", raw_system, "production_system", gaps)

        yield_fields, yield_obs_fields, purpose = self._yield(flat, eppo, gaps)
        irrigation_derivation: dict[str, Any] = {}
        if self.contract.unit.irrigation_derivation is not None and raw_irrigation is None:
            # the source states no regime (a stated one always wins, even an unrecognised one)
            irrigation, irrigation_derivation = self._derive_irrigation(eppo, yield_fields.get("yield_kg_ha"), gaps)

        values: dict[str, Any] = {
            "source_id": source_id, "document_key": document_key_, "crop_eppo": eppo,
            "raw_variety": raw_variety, "raw_site": raw_site, "raw_season": raw_season,
            "raw_irrigation": raw_irrigation, "raw_production_system": raw_system,
            "factor_levels": levels, "rootstock": rootstock, "clone": clone,
            "planting_year": planting_year, "row_discriminator": discriminator,
            "study_key": study_key_, "site_key": site_id, "variety_key": variety_key_, "year": year,
            "irrigation_regime": irrigation, "production_system": production_system, "purpose": purpose,
            "locator": locator, **yield_fields, **irrigation_derivation,
        }
        try:
            unit = UnitRow(**values, gaps=gaps.rows(values))
        except ValidationError as exc:
            raise _RowProblem(f"unit: {_validation_text(exc)}") from exc
        ukey = identity.unit_key(unit)

        observations: list[ObservationRow] = []
        if yield_obs_fields is not None:
            observations.append(self._observation(
                {"unit_key": ukey, "variable_id": YIELD_VARIABLE, "locator": locator, **yield_obs_fields},
                _Gaps()))
        for obs_plan in self.plan.observations:
            built = self._other_observation(flat, obs_plan, eppo, ukey, locator)
            if built is not None:
                observations.append(built)

        self._store_unit(index, ukey, unit)
        for observation in observations:
            self._store_observation(index, observation)

    # ── document, study, site, variety ─────────────────────────────────────

    def _document(self, index: int, flat: Mapping[str, Any]) -> tuple[str, str | None]:
        spec = self.contract.document
        gaps = _Gaps()
        title_value = _get(flat, spec.title.from_)
        if title_value is None:
            raise _RowProblem(f"document: title: raw field {spec.title.from_!r} is absent or blank")
        values: dict[str, Any] = {
            "source_id": self.contract.source_id,
            "title": _text(title_value, spec.title.from_),
            "issue": self._take_text(flat, spec.issue, gaps, "issue"),
            "year": self._take_int(flat, spec.year, gaps, "year"),
            "url": self._take_text(flat, spec.url, gaps, "url"),
            "file_name": self._take_text(flat, spec.file_name, gaps, "file_name"),
            "raw_sha256": self._take_text(flat, spec.raw_sha256, gaps, "raw_sha256"),
        }
        try:
            document = DocumentRow(**values, gaps=gaps.rows(values))
        except ValidationError as exc:
            raise _RowProblem(f"document: {_validation_text(exc)}") from exc
        key = identity.document_key(document)
        seen = self.documents.get(key)
        if seen is None:
            self.documents[key] = (index, document)
        elif seen[1] != document:
            fields = _differences(seen[1].model_dump(), document.model_dump())
            raise _RowProblem(f"document {document.title!r} appears with more than one {'/'.join(fields)} "
                              f"(first seen in row {seen[0]})")

        parts: list[str] = []
        for part in spec.locator:
            value = _get(flat, part.from_)
            if value is None:
                self.missing[part.from_] += 1
            else:
                parts.append(f"{part.label} {_text(value, part.from_)}")
        return key, ("; ".join(parts) or None)

    def _study(self, flat: Mapping[str, Any], eppo: str, raw_season: str | None, year: int | None) -> str:
        spec = self.contract.study
        gaps = _Gaps()
        if raw_season is None and self.contract.unit.fields.season is not None:
            gaps.add("raw_season", _absent(self.contract.unit.fields.season.from_))
        parts: list[str] = []
        for path in spec.group_by:
            value = _get(flat, path)
            if value is None:
                self.missing[path] += 1
            else:
                parts.append(f"{path}={_text(value, path)}")
        raw_scope = "; ".join(parts) or None
        if spec.group_by and raw_scope is None:
            gaps.add("raw_scope", "none of the fields the series is split by is present in the source row")
        if raw_season is not None and year is None:
            gaps.add("year", f"season {raw_season!r} is not a single calendar year")
        values: dict[str, Any] = {
            "source_id": self.contract.source_id, "study_type": self.plan.study_type, "crop_eppo": eppo,
            "raw_season": raw_season, "raw_scope": raw_scope,
            "name": ", ".join(part for part in (self.contract.source_id, eppo, raw_season, raw_scope) if part),
            "year": year, "design": spec.design,
        }
        try:
            study = StudyRow(**values, gaps=gaps.rows(values))
        except ValidationError as exc:
            raise _RowProblem(f"study: {_validation_text(exc)}") from exc
        key = identity.study_key(study)
        self.studies.setdefault(key, study)
        return key

    def _site(self, raw_site: str | None, gaps: _Gaps) -> str | None:
        """Resolve an OBSERVED place (zone, stratum, location) through the sites registry."""
        if raw_site is None:
            return None
        site = self.registries.site(raw_site)
        aggregate = any(pattern.search(raw_site) for pattern in self.plan.aggregate_patterns)
        if site is None:
            self.unresolved_sites[raw_site] += 1
            gaps.add("site_key", f"site {raw_site!r} is not in the sites registry")
            return None
        if aggregate and site.site_kind == "field":
            raise _RowProblem(f"site {raw_site!r} matches an aggregate pattern but the registry holds it as a "
                              f"field site ({site.id}): an aggregate is never disguised as a plot")
        row = self._site_row(site)
        key = identity.site_key(row)
        self.sites.setdefault(key, row)
        return key

    def _site_row(self, site: Site) -> SiteRow:
        gaps = _Gaps()
        if site.site_kind == "field" and site.latitude is None:
            gaps.add("latitude", "the sites registry holds no published coordinates for this site")
            gaps.add("longitude", "the sites registry holds no published coordinates for this site")
        values: dict[str, Any] = {
            "site_id": site.id, "name": site.name, "site_kind": site.site_kind, "country": site.country,
            "latitude": site.latitude, "longitude": site.longitude, "coordinate_source": site.coordinate_source,
            "climate_class": None,
            "source_ids": tuple(sorted({*site.sources, self.contract.source_id})),
        }
        return SiteRow(**values, gaps=gaps.rows(values))

    def _variety(self, eppo: str, raw_variety: str | None) -> str | None:
        if raw_variety is None:
            return None
        registered = self.registries.variety(eppo, raw_variety)
        if registered is not None:
            row = VarietyRow(
                crop_eppo=eppo, name=registered.name, registry_id=registered.id, status=registered.status,
                aliases=registered.aliases, source_ids=(self.contract.source_id,))
            key = identity.variety_key(row)
            self.varieties.setdefault(key, [row])
            return key
        row = VarietyRow(crop_eppo=eppo, name=raw_variety, status="candidate", source_ids=(self.contract.source_id,))
        key = identity.variety_key(row)
        self.varieties.setdefault(key, []).append(row)
        self.unregistered[key] += 1
        return key

    def _factor_levels(self, flat: Mapping[str, Any]) -> tuple[FactorLevel, ...]:
        levels: list[FactorLevel] = []
        for factor in self.contract.unit.factors:
            value = _get(flat, factor.from_)
            if value is None:
                self.missing[factor.from_] += 1
                continue
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                raise _RowProblem(f"factor {factor.factor}: {value!r} is not text or a number")
            if isinstance(value, float) and not math.isfinite(value):
                raise _RowProblem(f"factor {factor.factor}: {value!r} is not finite")
            levels.append(FactorLevel(factor=factor.factor, level=value, unit=factor.unit))
        return tuple(levels)

    # ── irrigation derived from the yield ───────────────────────────────────

    def _derive_irrigation(
        self, eppo: str, yield_kg_ha: float | None, gaps: _Gaps,
    ) -> tuple[str | None, dict[str, Any]]:
        """The regime a crop's yield cutoffs give, and the record of having applied them.

        ``yield <= low`` is rainfed, ``yield >= high`` irrigated, in between there is no regime and a
        gap ``irrigation_ambiguous_yield``. No yield or no calibrated cutoff for the crop: nothing is
        derived and nothing is recorded.
        """
        threshold = self.registries.irrigation_threshold(eppo)
        if threshold is None or yield_kg_ha is None:
            return None, {}
        assert threshold.low_kg_ha is not None and threshold.high_kg_ha is not None
        record = {
            "irrigation_derivation": IRRIGATION_DERIVATION_V1,
            "irrigation_yield_low_kg_ha": threshold.low_kg_ha,
            "irrigation_yield_high_kg_ha": threshold.high_kg_ha,
        }
        if yield_kg_ha <= threshold.low_kg_ha:
            outcome, regime = "rainfed", self.registries.vocab("irrigation", "rainfed")
        elif yield_kg_ha >= threshold.high_kg_ha:
            outcome, regime = "irrigated", self.registries.vocab("irrigation", "irrigated")
        else:
            outcome, regime = "ambiguous", None
            gaps.add("irrigation_regime",
                     f"irrigation_ambiguous_yield: {yield_kg_ha:g} kg/ha lies between the {eppo} cutoffs "
                     f"{threshold.low_kg_ha:g} and {threshold.high_kg_ha:g} kg/ha")
        self.irrigation_derived[f"{eppo}:{outcome}"] += 1
        return regime, record

    # ── yield and the unit purpose ──────────────────────────────────────────

    def _yield(self, flat: Mapping[str, Any], eppo: str, gaps: _Gaps,
               ) -> tuple[dict[str, Any], dict[str, Any] | None, str | None]:
        """The unit's derived yield copy, the yield Observation's fields, and the unit purpose."""
        unit_spec = self.contract.unit
        registries = self.registries
        spec = unit_spec.yield_
        purpose: str | None = None
        if unit_spec.purpose is not None:
            raw_purpose = _declared_raw(unit_spec.purpose, flat, eppo, "purpose")
            if raw_purpose is None:
                gaps.add("purpose", _absent(unit_spec.purpose.from_ or "purpose"))
            else:
                purpose = _vocab_id(registries, "purpose", raw_purpose)
                if purpose is None:
                    gaps.add("purpose", f"{raw_purpose!r} is not in the purpose vocabulary")
        if spec is None:
            return {}, None, purpose

        raw_yield = _get(flat, spec.from_)
        if raw_yield is None:
            self.missing[spec.from_] += 1
            gaps.add("yield_kg_ha", _absent(spec.from_))
            return {}, None, purpose
        plan_unit = self.plan.yield_unit
        assert plan_unit is not None
        original = _number(raw_yield, spec.from_)
        kg_ha = registries.convert(original, plan_unit.code, plan_unit.target)

        metric = self._yield_context("metric", spec.metric, "yield_metric", flat, eppo)
        basis = self._yield_context("basis", spec.basis, "yield_basis", flat, eppo)
        if purpose is None:
            raise _RowProblem(f"{spec.from_}: a yield carries its purpose: the unit purpose is missing or "
                              "not in the purpose vocabulary")
        metric_entry = registries.vocab_entry("yield_metric", metric)
        assert metric_entry is not None
        if metric_entry.purpose != purpose:
            raise _RowProblem(f"yield metric {metric!r} gives purpose {metric_entry.purpose!r} but the unit "
                              f"purpose is {purpose!r}")
        moisture = self._moisture(spec, basis, flat, eppo)

        yield_obs = {
            "value": kg_ha, "unit": plan_unit.target, "basis": basis, "moisture_pct": moisture,
            "metric": metric, "purpose": purpose, "value_original": original,
            "unit_original": spec.unit, "derivation_method": spec.derivation_method, "raw_key": spec.from_,
        }
        unit_copy = {
            "yield_kg_ha": kg_ha, "yield_metric": metric, "yield_basis": basis, "yield_moisture_pct": moisture,
            "yield_value_original": original, "yield_unit_original": spec.unit,
            "derivation_method": spec.derivation_method,
        }
        return unit_copy, yield_obs, purpose

    def _yield_context(self, name: str, declared: Declared, kind: str, flat: Mapping[str, Any], eppo: str) -> str:
        raw = _declared_raw(declared, flat, eppo, f"yield {name}")
        if raw is None:
            raise _RowProblem(f"yield {name}: raw field {declared.from_!r} is absent or blank; a yield "
                              f"without its {name} is not stored")
        value = _vocab_id(self.registries, kind, raw)
        if value is None:
            raise _RowProblem(f"yield {name}: {raw!r} is not in the {kind} vocabulary")
        return value

    def _moisture(self, spec: YieldSpec, basis: str, flat: Mapping[str, Any], eppo: str) -> float | None:
        declared = spec.moisture_pct
        if basis != STANDARD_MOISTURE_BASIS:
            if declared is not None and declared.from_ is not None and _get(flat, declared.from_) is not None:
                raise _RowProblem(f"yield basis {basis!r} but the row prints a moisture "
                                  f"({declared.from_!r}): the declarations contradict")
            return None
        if declared is None:
            raise _RowProblem(f"the {STANDARD_MOISTURE_BASIS} basis needs a yield moisture_pct")
        raw = _declared_raw(declared, flat, eppo, "yield moisture_pct")
        if raw is None:
            raise _RowProblem(f"yield moisture: raw field {declared.from_!r} is absent or blank; the "
                              f"{STANDARD_MOISTURE_BASIS} basis needs it")
        value = _number(raw, "yield moisture_pct")
        if not 0 < value < 100:
            raise _RowProblem(f"yield moisture_pct {value!r} is not between 0 and 100")
        return value

    # ── observations ────────────────────────────────────────────────────────

    def _observation(self, values: dict[str, Any], gaps: _Gaps) -> ObservationRow:
        try:
            return ObservationRow(**values, gaps=gaps.rows(values))
        except ValidationError as exc:
            raise _RowProblem(f"observation {values['variable_id']}: {_validation_text(exc)}") from exc

    def _other_observation(
        self, flat: Mapping[str, Any], obs_plan: _ObsPlan, eppo: str, ukey: str, locator: str | None,
    ) -> ObservationRow | None:
        spec, variable, unit = obs_plan.spec, obs_plan.variable, obs_plan.unit
        raw = _get(flat, spec.from_)
        if raw is None:
            self.missing[spec.from_] += 1
            return None
        gaps = _Gaps()
        values: dict[str, Any] = {
            "unit_key": ukey, "variable_id": variable.id, "raw_key": spec.from_, "locator": locator,
            "derivation_method": spec.derivation_method,
        }
        if variable.scale in ("categorical", "date"):
            values["value_text"] = _text(raw, spec.from_)
        else:
            original = _number(raw, spec.from_)
            if variable.domain is not None and not variable.domain[0] <= original <= variable.domain[1]:
                raise _RowProblem(f"{spec.from_}: {original!r} is outside the domain {list(variable.domain)} "
                                  f"of {variable.id} (a different scale printed under the same name?)")
            values["value_original"] = original
            if unit is not None:
                values["value"] = self.registries.convert(original, unit.code, unit.target)
                values["unit"] = unit.target
                values["unit_original"] = unit.printed
            else:
                values["value"] = original
        if spec.stage is not None:
            raw_stage = _declared_raw(spec.stage, flat, eppo, "stage")
            if raw_stage is None:
                self.missing[spec.stage.from_ or ""] += 1
                gaps.add("stage", _absent(spec.stage.from_ or "stage"))
            else:
                values["stage"] = _text(raw_stage, "stage")
        if spec.qualifier is not None:
            raw_qualifier = _declared_raw(spec.qualifier, flat, eppo, "qualifier")
            if raw_qualifier is None:
                self.missing[spec.qualifier.from_ or ""] += 1
                gaps.add("qualifier", _absent(spec.qualifier.from_ or "qualifier"))
            else:
                values["qualifier"] = _text(raw_qualifier, "qualifier")
        if spec.date is not None:
            raw_date = _get(flat, spec.date.from_)
            if raw_date is None:
                self.missing[spec.date.from_] += 1
                gaps.add("date", _absent(spec.date.from_))
            else:
                values["date"] = _date(raw_date, spec.date.from_)
        return self._observation(values, gaps)

    # ── storing, with duplicate and conflict handling ───────────────────────

    def _store_unit(self, index: int, key: str, unit: UnitRow) -> None:
        seen = self.unit_rows.get(key)
        if seen is None:
            self.unit_rows[key] = (index, unit)
            return
        merged = _merge_duplicate(seen[1], unit)
        if merged is None:
            fields = _differences(seen[1].model_dump(exclude={"locator"}), unit.model_dump(exclude={"locator"}))
            raise _RowProblem(f"same unit key as row {seen[0]} but different content ({', '.join(fields)}): "
                              "two different observations cannot share an identity; a row_discriminator "
                              "or the adapter must tell them apart")
        self.collapsed_units += 1
        self.unit_rows[key] = (seen[0], merged)

    def _store_observation(self, index: int, observation: ObservationRow) -> None:
        key = identity.obs_key(observation)
        seen = self.obs_rows.get(key)
        if seen is None:
            self.obs_rows[key] = (index, observation)
            return
        merged = _merge_duplicate(seen[1], observation)
        if merged is None:
            fields = _differences(seen[1].model_dump(exclude={"locator"}),
                                  observation.model_dump(exclude={"locator"}))
            raise _RowProblem(f"same observation key as row {seen[0]} ({observation.variable_id}) but different "
                              f"content ({', '.join(fields)})")
        self.collapsed_observations += 1
        self.obs_rows[key] = (seen[0], merged)


def _merge_duplicate(first: Any, second: Any) -> Any | None:
    """The row to keep when two raw rows carry the same identity and content; None on any difference.

    Only the locator (where in the document it was printed) may differ; the smaller one is kept so
    the result does not depend on the order of the raw rows.
    """
    if first.model_dump(exclude={"locator"}) != second.model_dump(exclude={"locator"}):
        return None
    return first if (first.locator or "") <= (second.locator or "") else second


# ═════════════════════════════════════════════════════════════════════════════
# range checks, unmapped fields, run_contract
# ═════════════════════════════════════════════════════════════════════════════

def _range_checks(
    registries: Registries, units: Mapping[str, UnitRow], observations: Iterable[ObservationRow],
) -> RangeChecks:
    """Measure the plausible-range checks of a bundle, counting every one that cannot be made.

    Severity is not decided here: a finding says which range applied and whether it is ``reviewed``
    or an ``assumption`` (the gate errors only on reviewed ranges).
    """
    has_range = {(rng.crop, rng.variable) for rng in registries.ranges}
    evaluated = in_range = skipped = not_applicable = 0
    skipped_by_reason: Counter[str] = Counter()
    findings: list[RangeFinding] = []
    for observation in observations:
        variable = registries.variable(observation.variable_id)
        if observation.value is None or variable.scale not in ("ratio", "percent"):
            not_applicable += 1
            continue
        unit = units[observation.unit_key]
        rng = registries.range_for(unit.crop_eppo, observation.variable_id, {
            "irrigation": unit.irrigation_regime,
            "production_system": unit.production_system,
            "purpose": unit.purpose,
        })
        if rng is None:
            skipped += 1
            skipped_by_reason[
                "no_matching_conditions" if (unit.crop_eppo, observation.variable_id) in has_range
                else "no_range_for_crop_variable"] += 1
            continue
        evaluated += 1
        if rng.min <= observation.value <= rng.max:
            in_range += 1
        else:
            findings.append(RangeFinding(
                obs_key=identity.obs_key(observation), unit_key=observation.unit_key, crop_eppo=unit.crop_eppo,
                variable_id=observation.variable_id, value=observation.value, range_id=rng.id,
                range_min=rng.min, range_max=rng.max, range_status=rng.status))
    return RangeChecks(
        evaluated=evaluated, in_range=in_range, out_of_range=tuple(sorted(findings, key=lambda f: f.obs_key)),
        skipped=skipped, skipped_by_reason=dict(sorted(skipped_by_reason.items())), not_applicable=not_applicable)


def _check_unmapped(contract: Contract, flats: Sequence[Mapping[str, Any]]) -> None:
    mapped = contract.mapped_paths()
    ignored = [entry.field for entry in contract.ignore]
    unmapped: Counter[str] = Counter()
    for flat in flats:
        for path in flat:
            if path not in mapped and not any(_covers(prefix, path) for prefix in ignored):
                unmapped[path] += 1
    if unmapped:
        raise UnmappedFieldError(unmapped)


def _count_gaps(kind: str, rows: Iterable[Any], counter: Counter[str]) -> None:
    for row in rows:
        for gap in row.gaps:
            counter[f"{kind}.{gap.field}"] += 1


def _finish(builder: _Builder, plan: _Plan, raw_rows: int) -> Bundle:
    registries, contract = plan.registries, plan.contract
    varieties = {key: min(rows, key=lambda row: row.name) for key, rows in builder.varieties.items()}
    units = {key: row for key, (_, row) in builder.unit_rows.items()}
    observations = {key: row for key, (_, row) in builder.obs_rows.items()}
    documents = {key: row for key, (_, row) in builder.documents.items()}

    gaps: Counter[str] = Counter()
    _count_gaps("document", documents.values(), gaps)
    _count_gaps("study", builder.studies.values(), gaps)
    _count_gaps("site", builder.sites.values(), gaps)
    _count_gaps("unit", units.values(), gaps)
    _count_gaps("observation", observations.values(), gaps)

    ordered_observations = tuple(observations[key] for key in sorted(observations))
    report = BuildReport(
        source_id=contract.source_id, contract_hash=contract_hash(contract),
        registries_hash=registries.registries_hash, raw_rows=raw_rows,
        collapsed_duplicate_units=builder.collapsed_units,
        collapsed_duplicate_observations=builder.collapsed_observations,
        gaps=dict(sorted(gaps.items())), missing=dict(sorted(builder.missing.items())),
        unresolved_sites=dict(sorted(builder.unresolved_sites.items())),
        unresolved_vocab=dict(sorted(builder.unresolved_vocab.items())),
        irrigation_derived=dict(sorted(builder.irrigation_derived.items())),
        unregistered_varieties=dict(sorted(
            (f"{varieties[key].crop_eppo}:{varieties[key].name}", count)
            for key, count in builder.unregistered.items())),
        range_checks=_range_checks(registries, units, ordered_observations),
        warnings=plan.warnings,
    )
    return Bundle(
        source_id=contract.source_id,
        documents=tuple(documents[key] for key in sorted(documents)),
        studies=tuple(builder.studies[key] for key in sorted(builder.studies)),
        sites=tuple(builder.sites[key] for key in sorted(builder.sites)),
        varieties=tuple(varieties[key] for key in sorted(varieties)),
        units=tuple(units[key] for key in sorted(units)),
        observations=ordered_observations,
        report=report,
    )


def _check_expected(contract: Contract, bundle: Bundle) -> None:
    actual = {"units": len(bundle.units), "observations": len(bundle.observations), "sites": len(bundle.sites)}
    wanted = contract.expected.model_dump()
    wrong = [f"{name}: expected {wanted[name]}, got {actual[name]}" for name in wanted if wanted[name] != actual[name]]
    if wrong:
        raise ExpectedCountError(f"contract {contract.source_id} declares other counts than the build "
                                 f"produced ({'; '.join(wrong)}): rows were dropped, added or split")


def run_contract(contract: Contract, registries: Registries, raw_rows: Iterable[Mapping[str, Any]]) -> Bundle:
    """Map raw rows to the canonical bundle under ``contract``, or fail; never drop a field silently.

    Raises :class:`ContractError` (the contract disagrees with the registries),
    :class:`UnmappedFieldError` (a raw field is neither mapped nor ignored),
    :class:`ContractDataError` (rows that cannot be mapped, listed with their position) or
    :class:`ExpectedCountError` (the counts differ from the contract's ``expected``).
    """
    plan = _compile(contract, registries)
    rows = list(raw_rows)
    flats: list[dict[str, Any]] = []
    problems: list[str] = []
    for index, row in enumerate(rows):
        try:
            flats.append(_flatten(row))
        except _RowProblem as exc:
            problems.append(f"row {index}: {exc}")
    if problems:
        raise ContractDataError(problems)
    _check_unmapped(contract, flats)

    builder = _Builder(plan)
    for index, flat in enumerate(flats):
        try:
            builder.add_row(index, flat)
        except _RowProblem as exc:
            builder.problems.append(f"row {index}: {exc}")
    if builder.problems:
        raise ContractDataError(builder.problems)
    bundle = _finish(builder, plan, len(rows))
    _check_expected(contract, bundle)
    return bundle


__all__ = [
    "YIELD_VARIABLE",
    "BuildReport",
    "Bundle",
    "Contract",
    "ContractDataError",
    "ContractError",
    "ExpectedCountError",
    "RangeChecks",
    "RangeFinding",
    "UnmappedFieldError",
    "contract_hash",
    "load_contract",
    "run_contract",
]
