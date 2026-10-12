/** Mirror of backend/app/planning/models.py — contract version 1.1. Change both together. */
import type { SourceAttributionItem } from '../types/attribution';

export const CONTRACT_VERSION = '1.1';
export type Verdict = 'fits' | 'caveats' | 'no_fit';
export type Regime = 'secano' | 'regadío';
export type Management = 'conventional' | 'organic';
export type Provenance = 'actual' | 'declared' | 'planned' | 'unknown';
export type Role = 'main_crop' | 'cover_crop' | 'catch_crop';
export type TerminationMethod = 'roller_crimper' | 'mowing' | 'harvest' | 'incorporate' | 'grazing';
export type ParamValue = string | number | boolean | null;
export type Names = Record<string, string>;
export type TFn = (key: string, opts?: Record<string, unknown>) => string;

export interface Reason { code: string; gate: 'soil' | 'frost' | 'water' | 'rotation' | 'data'; params: Record<string, ParamValue> }
export interface Sowing { sowing_type: string | null; start_month: number | null; end_month: number | null; typical_sowing_doy: number | null; source: string | null }
export interface SpeciesFit {
  species: string; eppo: string | null; names: Names; lifecycle: string | null; verdict: Verdict; reasons: Reason[]; sowing: Sowing;
  has_variety_trials: boolean; estimate_providers: string[];
}
export interface ParcelFacts {
  climate_class: string | null; climate_source: string; soil_texture: string | null; soil_ph: number | null;
  soil_source: string; regime: Regime | null; regime_source: string; history_available: boolean;
}
export interface FitResponse { contract_version: string; parcel_id: string; campaign: number; facts: ParcelFacts; species: SpeciesFit[]; graphs: string[] }
export interface VarietyStat {
  variety: string; relative_yield_pct: number; n_environments: number; years: number[]; mean_kg_ha: number;
  fw_slope: number | null; stability: 'stable' | 'average' | 'responsive' | null; few_environments: boolean;
}
export interface VarietiesResponse {
  contract_version: string; parcel_id: string; crop: string; regime: Regime; management: Management;
  status: 'ok' | 'no_trials' | 'needs_climate'; tier: 'field' | 'regional' | null;
  zone_match: Record<string, unknown> | null; country: string | null; n_environments: number;
  varieties: VarietyStat[]; data_gaps: string[]; attributions: SourceAttributionItem[]; graph_id: string;
}
export type Basis = 'trial_evidence' | 'water_limited' | 'gap_corrected' | 'potential';
export interface Moisture { basis: 'commercial' | 'dry_matter' | 'as_reported'; pct: number | null }
export interface Estimate {
  provider_id: string; provider_version: string; basis: Basis; unit: 't/ha';
  percentiles: { p10: number; p50: number; p90: number } | null; // null for range-only figures (1.1)
  range: { low: number; high: number } | null; coverage: number | null; moisture: Moisture;
  n: number; n_kind: 'trials' | 'years';
  validation: { status: 'validated' | 'experimental' | 'unvalidated' | 'not_applicable'; metric: string | null; scope: string | null };
  warnings: string[]; attributions: SourceAttributionItem[]; graph_id: string | null;
}
export interface EstimatesResponse {
  contract_version: string; parcel_id: string; crop: string; variety: string | null;
  status: 'ok' | 'no_trials' | 'needs_climate';
  inputs: { regime: Regime; management: Management; sowing_date: string | null; sowing_source: 'planned' | 'none'; soil_source: string; climate_source: string };
  estimates: Estimate[]; unavailable: Unavailable[]; disagreement: boolean;
}
export interface Unavailable { provider_id: string; reason: string; code?: string | null }
export type AquacropState = 'unavailable' | 'queued' | 'running' | 'done' | 'failed';
export interface AquacropResponse {
  contract_version: string; provider_id: 'aquacrop'; state: AquacropState; done: number; total: number | null;
  estimates: Estimate[]; unavailable: Unavailable[]; job_id: string | null;
}
export interface CycleCell {
  crop_id: string | null; species: string | null; species_raw: string | null; names: Names; variety: string | null;
  role: string; provenance: Provenance; status: string | null; start: string | null; end: string | null;
  lifecycle: string; first_campaign: number | null;
}
export interface PlanWarning { code: string; campaign: number | null; params: Record<string, ParamValue> }
export interface ParcelRow {
  parcel_id: string; name: string | null; area_ha: number | null; farm_id: string | null; regime: Regime | null;
  regime_source: string; cycles_available: boolean; cells: Record<string, CycleCell[]>; warnings: PlanWarning[];
}
export interface FarmResponse {
  contract_version: string; campaign: number; campaigns: number[];
  farms: { farm_id: string | null; name: string | null }[]; parcels: ParcelRow[];
  summary: { area_by_crop: { species: string | null; names: Names; area_ha: number }[]; undecided: number; rotation_warnings: number };
}
export interface PlanCycleIn {
  role: Role; crop: string; variety: string | null; sowing_start: string; sowing_end: string | null;
  expected_termination: string; termination_method: TerminationMethod | null;
}
export interface PlanIn { campaign: number; regime: Regime; management: Management; cycles: PlanCycleIn[] }
export interface PlanWriteResponse { contract_version: string; parcel_id: string; campaign: number; written: string[]; removed: string[] }
