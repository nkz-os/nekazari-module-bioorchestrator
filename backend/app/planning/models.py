"""Response contract of the planning endpoints, version CONTRACT_VERSION.

Mirrored in ``src/planning/types.ts``: changing a field is a contract change (bump the
version). Reason ``code`` values are i18n keys of the UI (``planning.reason.<code>``) and
``params`` are plain values the UI formats.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, Field

CONTRACT_VERSION = "2.1"

Verdict = Literal["fits", "caveats", "no_fit"]
# Owner decision (2026-10-13): the regime vocabulary is rainfed | irrigated. The Spanish words that
# earlier clients sent are still read on input and never written.
_LEGACY_REGIMES = {"secano": "rainfed", "regadío": "irrigated", "regadio": "irrigated"}


def _legacy_regime(value: Any) -> Any:
    return _LEGACY_REGIMES.get(value, value) if isinstance(value, str) else value


Regime = Annotated[Literal["rainfed", "irrigated"], BeforeValidator(_legacy_regime)]
Management = Literal["conventional", "organic"]
SowingMethod = Literal["direct", "conventional"]
Provenance = Literal["actual", "declared", "planned", "unknown"]
Gate = Literal["soil", "frost", "water", "rotation", "data"]
Role = Literal["main_crop", "cover_crop", "catch_crop"]
ParamValue = str | int | float | bool | None


class Reason(BaseModel):
    code: str
    gate: Gate
    params: dict[str, ParamValue] = Field(default_factory=dict)


class Sowing(BaseModel):
    sowing_type: str | None = None
    start_month: int | None = None
    end_month: int | None = None
    typical_sowing_doy: int | None = None
    source: str | None = None


class SpeciesFit(BaseModel):
    species: str
    eppo: str | None
    names: dict[str, str]
    lifecycle: str | None = None
    verdict: Verdict
    reasons: list[Reason]
    sowing: Sowing
    has_variety_trials: bool
    estimate_providers: list[str]


class ParcelFacts(BaseModel):
    climate_class: str | None
    climate_source: str
    soil_texture: str | None
    soil_ph: float | None
    soil_source: str
    regime: Regime | None
    regime_source: str
    history_available: bool


class FitResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    parcel_id: str
    campaign: int
    facts: ParcelFacts
    species: list[SpeciesFit]
    graphs: list[str]


class VarietyStat(BaseModel):
    variety: str
    relative_yield_pct: float
    n_environments: int
    years: list[int]
    mean_kg_ha: float
    fw_slope: float | None
    stability: Literal["stable", "average", "responsive"] | None
    few_environments: bool


class VarietiesResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    parcel_id: str
    crop: str
    regime: Regime
    management: Management
    status: Literal["ok", "no_trials", "needs_climate"]
    tier: Literal["field", "regional"] | None = None
    zone_match: dict[str, Any] | None = None
    country: str | None = None
    n_environments: int = 0
    varieties: list[VarietyStat] = Field(default_factory=list)
    data_gaps: list[str] = Field(default_factory=list)
    attributions: list[dict[str, Any]] = Field(default_factory=list)
    graph_id: str


class Percentiles(BaseModel):
    p10: float
    p50: float
    p90: float


class Validation(BaseModel):
    status: Literal["validated", "experimental", "unvalidated", "not_applicable"]
    metric: str | None = None
    scope: str | None = None


class Range(BaseModel):
    low: float
    high: float


class Moisture(BaseModel):
    """Grain moisture the figure is expressed at; figures are only compared at the same one."""
    basis: Literal["commercial", "dry_matter", "as_reported"]
    pct: float | None = None


class Estimate(BaseModel):
    provider_id: str
    provider_version: str
    basis: Literal["trial_evidence", "water_limited", "gap_corrected", "potential"]
    unit: Literal["t/ha"] = "t/ha"
    percentiles: Percentiles | None = None  # null for range-only figures (1.1)
    range: Range | None = None  # 1.1: low-high without a median
    coverage: float | None = None  # 1.1: measured share of held-out years inside the range
    moisture: Moisture = Field(default_factory=lambda: Moisture(basis="as_reported"))  # 1.1
    n: int
    n_kind: Literal["trials", "years"]
    validation: Validation
    warnings: list[str] = Field(default_factory=list)
    attributions: list[dict[str, Any]] = Field(default_factory=list)
    graph_id: str | None = None


class Unavailable(BaseModel):
    provider_id: str
    reason: str
    code: str | None = None  # 1.1: the provider's own error code, when it has one


class EstimateInputs(BaseModel):
    regime: Regime
    management: Management
    sowing_date: date | None
    sowing_source: Literal["planned", "none"]
    soil_source: str
    climate_source: str


class EstimatesResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    parcel_id: str
    crop: str
    variety: str | None
    status: Literal["ok", "no_trials", "needs_climate"]
    inputs: EstimateInputs
    estimates: list[Estimate]
    unavailable: list[Unavailable]
    disagreement: bool


class AquacropResponse(BaseModel):
    """State of the asynchronous AquaCrop provider (1.1); the UI polls this until it settles."""
    contract_version: str = CONTRACT_VERSION
    provider_id: Literal["aquacrop"] = "aquacrop"
    state: Literal["unavailable", "queued", "running", "done", "failed"]
    done: int = 0
    total: int | None = None
    estimates: list[Estimate] = Field(default_factory=list)
    unavailable: list[Unavailable] = Field(default_factory=list)
    job_id: str | None = None
    # How the crop was sown in this simulation and where that came from (set once done).
    sowing_method: SowingMethod | None = None
    sowing_method_source: Literal["request", "field_operations", "default"] | None = None


class SeasonSowing(BaseModel):
    date: date
    source: Literal["crop_cycle"]
    provenance: Literal["actual", "declared"]


class SeasonInfo(BaseModel):
    """The season behind an in-season forecast (2.1)."""
    status: Literal["in_season", "complete"]
    as_of: date | None  # last day of observed weather
    crop: str  # EPPO
    regime: Regime
    sowing: SeasonSowing
    sowing_method: SowingMethod | None = None
    sowing_method_source: Literal["request", "field_operations", "default"] | None = None
    harvest_date: date | None = None  # median member in season; the simulated one when complete
    n_years: int = 0  # analog years; 0 when complete
    weather_source: str | None = None
    computed_at: datetime | None = None  # when the job finished (UTC)
    provider_version: str


class InSeasonResponse(BaseModel):
    """In-season AquaCrop forecast (2.1): the job states of AquacropResponse, plus the season."""
    contract_version: str = CONTRACT_VERSION
    provider_id: Literal["aquacrop"] = "aquacrop"
    state: Literal["unavailable", "queued", "running", "done", "failed"]
    done: int = 0
    total: int | None = None
    estimates: list[Estimate] = Field(default_factory=list)
    unavailable: list[Unavailable] = Field(default_factory=list)
    job_id: str | None = None
    season: SeasonInfo | None = None


class CycleCell(BaseModel):
    crop_id: str | None
    species: str | None
    species_raw: str | None
    names: dict[str, str]
    variety: str | None
    role: str
    provenance: Provenance
    status: str | None
    start: str | None
    end: str | None
    lifecycle: str = "unknown"
    first_campaign: int | None = None


class PlanWarning(BaseModel):
    code: str
    campaign: int | None = None
    params: dict[str, ParamValue] = Field(default_factory=dict)


class ParcelRow(BaseModel):
    parcel_id: str
    name: str | None
    area_ha: float | None
    farm_id: str | None
    regime: Regime | None
    regime_source: str
    cycles_available: bool
    cells: dict[str, list[CycleCell]]
    warnings: list[PlanWarning]


class FarmGroup(BaseModel):
    farm_id: str | None
    name: str | None


class CropArea(BaseModel):
    species: str | None
    names: dict[str, str]
    area_ha: float


class FarmSummary(BaseModel):
    area_by_crop: list[CropArea]
    undecided: int
    rotation_warnings: int


class FarmResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    campaign: int
    campaigns: list[int]
    farms: list[FarmGroup]
    parcels: list[ParcelRow]
    summary: FarmSummary


class PlanCycleIn(BaseModel):
    role: Role
    crop: str = Field(min_length=2, max_length=64)
    variety: str | None = Field(None, max_length=64)
    sowing_start: date
    sowing_end: date | None = None
    # Owner decision (2026-10-12): the farmer is not asked for a harvest date; the server derives the
    # ends it needs (plan_write._ends). A date sent here keeps the earlier rules.
    expected_termination: date | None = None
    # A perennial main crop (alfalfa) is planned for a number of campaigns instead of an end date.
    perennial_years: int | None = Field(None, ge=1, le=10)
    termination_method: Literal["roller_crimper", "mowing", "harvest", "incorporate", "grazing"] | None = None


class PlanIn(BaseModel):
    campaign: int = Field(ge=2000, le=2100)
    regime: Regime
    management: Management
    cycles: list[PlanCycleIn] = Field(min_length=1, max_length=4)


class PlanWriteResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    parcel_id: str
    campaign: int
    written: list[str]
    removed: list[str]
