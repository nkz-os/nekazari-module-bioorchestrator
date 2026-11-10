"""Response contract of the planning endpoints, version CONTRACT_VERSION.

Mirrored in ``src/planning/types.ts``: changing a field is a contract change (bump the
version). Reason ``code`` values are i18n keys of the UI (``planning.reason.<code>``) and
``params`` are plain values the UI formats.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, Field

CONTRACT_VERSION = "2.7"

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


class IrrigationPercentiles(BaseModel):
    p10: float
    p50: float
    p90: float


class IrrigationMethod(BaseModel):
    kc_source: str
    pe_method: str


class IrrigationNeed(BaseModel):
    """Seasonal NET irrigation need over the climatology years, in the field (no application efficiency)."""
    percentiles: IrrigationPercentiles
    unit: Literal["mm"] = "mm"
    n_years: int
    first_year: int
    last_year: int
    sowing_doy: int
    method: IrrigationMethod
    source: Literal["openmeteo_era5"] = "openmeteo_era5"
    attributions: list[dict[str, Any]] = Field(default_factory=list)


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
    cover_only: bool = False  # offered as a cover crop only, never as the main crop
    # Regional statistic; orders the crops inside a verdict, never changes it. Not a fit reason.
    regional_yield: RegionalYield | None = None
    regional_yield_unavailable: str | None = None
    # Irrigated parcels only; informs the water gate, never changes the verdict by itself.
    irrigation_need: IrrigationNeed | None = None
    # regime_not_irrigated | kc_unavailable | no_kc | no_stage_lengths | no_sowing_date | pending | weather_unavailable
    irrigation_need_unavailable: str | None = None


class ParcelFacts(BaseModel):
    climate_class: str | None
    climate_source: str
    soil_texture: str | None
    soil_ph: float | None
    # The soil PROFILE's source (soil module dataSource), not the provenance of the texture (entered/measured/estimated).
    soil_source: str
    regime: Regime | None
    regime_source: str
    history_available: bool
    irrigation_allotment_mm: float | None = None
    # Of the 2 campaigns before the planned one, those with no main crop in the history (2.5);
    # [] when complete, null when the history could not be read.
    history_missing_campaigns: list[int] | None = None


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
    # Mean relative yield in the poor trials (lower tercile of environment means); None below the thresholds.
    low_env_relative_yield_pct: float | None = None
    n_low_environments: int = 0
    # Actuality: the variety's last trial year, and whether it falls outside the recent campaigns window.
    last_year: int | None = None
    stale: bool = False


class EvidenceDepth(BaseModel):
    level: Literal["deep", "broad", "moderate", "pilot"]
    n_trials: int
    n_sites: int
    n_years: int
    first_year: int | None = None
    last_year: int | None = None


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
    evidence_depth: EvidenceDepth | None = None


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


class RegionalMoisture(BaseModel):
    basis: Literal["eu_standard"] = "eu_standard"
    pct: float | None = None


class RegionalYield(BaseModel):
    """Regional statistic (Eurostat NUTS2 mean, rainfed and irrigated together); never a forecast for a parcel."""
    provider_id: Literal["eurostat_nuts2"] = "eurostat_nuts2"
    basis: Literal["regional_statistics"] = "regional_statistics"
    nuts: str
    region_name: dict[str, str] | None = None
    first_year: int
    last_year: int
    n_years: int
    percentiles: Percentiles
    unit: Literal["t/ha"] = "t/ha"
    moisture: RegionalMoisture = Field(default_factory=RegionalMoisture)
    regimes: Literal["mixed"] = "mixed"
    relative_index: float | None = None
    attributions: list[dict[str, Any]] = Field(default_factory=list)


class Estimate(BaseModel):
    provider_id: str
    provider_version: str
    basis: Literal["parcel_history", "trial_evidence", "water_limited", "gap_corrected", "potential"]
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
    # 2.7: milestone dates of the engine's cereal calendar (wheat, durum wheat, barley); null otherwise
    phenology: dict[str, Any] | None = None


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
    yield_kg_ha: float | None = None
    yield_source: Literal["measured", "declared"] | None = None


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
    history_campaigns: list[int] = Field(default_factory=list)
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


class FortnightOut(BaseModel):
    start: date
    end: date


class CalendarMain(BaseModel):
    window: FortnightOut | None
    window_source: str | None
    default: FortnightOut | None
    selected: FortnightOut | None
    options: list[FortnightOut]
    perennial_years_default: int | None


class CalendarPrevious(BaseModel):
    crop: str | None
    end: date | None
    end_source: Literal["harvest_recorded", "planned_end", "typical_maturity", "campaign_start"]


class CalendarCover(BaseModel):
    fits: bool
    default: FortnightOut | None
    options: list[FortnightOut]
    ends: date | None
    weeks: int | None
    short: bool


class CalendarResponse(BaseModel):
    contract_version: str = CONTRACT_VERSION
    parcel_id: str
    campaign: int
    crop: str | None
    cover_crop: str | None
    fortnights: list[FortnightOut]
    main: CalendarMain
    previous: CalendarPrevious
    cover: CalendarCover | None
