"""Response contract of the planning endpoints, version CONTRACT_VERSION.

Mirrored in ``src/planning/types.ts``: changing a field is a contract change (bump the
version). Reason ``code`` values are i18n keys of the UI (``planning.reason.<code>``) and
``params`` are plain values the UI formats.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field

CONTRACT_VERSION = "1.0"

Verdict = Literal["fits", "caveats", "no_fit"]
Regime = Literal["secano", "regadío"]
Management = Literal["conventional", "organic"]
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


class Estimate(BaseModel):
    provider_id: str
    provider_version: str
    basis: Literal["trial_evidence", "water_limited", "gap_corrected", "potential"]
    unit: Literal["t/ha"] = "t/ha"
    percentiles: Percentiles
    n: int
    n_kind: Literal["trials", "years"]
    validation: Validation
    warnings: list[str] = Field(default_factory=list)
    attributions: list[dict[str, Any]] = Field(default_factory=list)
    graph_id: str | None = None


class Unavailable(BaseModel):
    provider_id: str
    reason: str


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
    expected_termination: date
    termination_method: Literal["roller_crimper", "harvest", "incorporate", "grazing"] | None = None


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
