/**
 * Types of the crop recommendation API. Mirror of
 * backend/app/graph/recommend.py::build_recommendation and
 * backend/app/graph/dao.py::recommend_for_conditions / get_parcel_environment.
 */

export type Level = 'suitable' | 'marginal' | 'unsuitable' | 'unknown';
export type WaterLevel = 'low' | 'medium' | 'high' | 'unknown';
export type FrostLevel = 'none' | 'risk' | 'unknown';
export type SowingType = 'autumn' | 'spring' | 'summer' | 'perennial';
export type TrustLevel = 'high' | 'medium' | 'low';
export type Similarity = 'koppen' | 'vector_v2_fallback';
export type Interval = [number | null, number | null];
/** `main` = each crop's main harvested product (grain, fruit, kernel, tuber); `forage` = forage records only. */
export type Purpose = 'main' | 'forage';
/** `field` = numbers from field trials; `regional` = aggregate national/regional sites only (never mixed). */
export type EvidenceTier = 'field' | 'regional';
export type YieldBasis = 'dry_matter' | 'fresh_matter';

export interface VarietyRec {
  variety: string;
  variety_uri: string | null;
  expected_kg_ha: number | null;
  interval: Interval;
  n_trials: number;
  disease_summary: { resistant: number; total: number };
}

export interface Recommendation {
  recommendation_id: string;
  crop: { eppo: string; scientific_name: string; sowing_type: SowingType | null };
  fit: {
    relative_yield_pct: number | null;
    stability_cv: number | null;
    reference: { median_kg_ha: number | null; n_trials: number; scope: string };
  };
  yield: {
    expected_kg_ha: number | null;
    interval: Interval;
    interval_method: string;
    /** Forage mode with a number: `dry_matter` (kg dry matter/ha). null otherwise. */
    basis: YieldBasis | null;
    sd: number | null;
    n_trials: number;
    n_sites: number;
  };
  suitability: {
    soil: { level: Level; warnings: string[] };
    water: { level: WaterLevel; etc_mm: number | null };
    frost: { level: FrostLevel };
  };
  season: {
    sowing_window: { start_month: number; end_month: number } | null;
    cycle_days: number | null;
    source: string | null;
    /** Typical sowing day of year (GGCMI crop calendar); set only when sowing_window is null. */
    typical_sowing_doy?: number | null;
    /** Typical maturity day of year (GGCMI crop calendar). */
    typical_maturity_doy?: number | null;
    /** True when an irrigated parcel shows the rainfed GGCMI calendar (no irrigated value there). */
    typical_rainfed_fallback?: boolean | null;
  };
  trust: { level: TrustLevel; data_gaps: string[]; similarity: Similarity };
  varieties: VarietyRec[];
  evidence: {
    trial_count: number;
    sources: string[];
    sites: string[];
    years: [number, number] | null;
    tier: EvidenceTier;
    purpose: Purpose;
    /** Field recs: distinct numeric regional trials at the climate's aggregate sites (in no number). */
    regional_trial_count: number | null;
    /** Main mode: distinct forage trials of the crop at the same analog sites, counted not averaged. */
    other_purpose_trials: { forage?: number };
    /** Forage mode: best variety's forage trials with a kg value but no known basis (no number from them). */
    unknown_basis_trials: number | null;
  };
  assumptions: { id: string; value: unknown; citation: string }[];
}

export interface ParcelEnvironment {
  parcel_id: string;
  area_ha: number | null;
  centroid: { lat: number | null; lon: number | null };
  country?: string | null;
  climate_class: string | null;
  climate_detail: Record<string, unknown> | null;
  soil: {
    texture?: string | null;
    wrb_type?: string | null;
    ph?: number | null;
    data_available: boolean;
    source?: string;
  };
  irrigation: { inferred: string | null; source: string; overridable: boolean };
  campaign: { assigned: boolean };
  inputs_used: Record<string, unknown>;
}

export interface RecommendOk {
  status: 'ok';
  /** Evidence-policy version the answer was computed under. */
  evidence_policy: string;
  recommendations: Recommendation[];
  data_quality: Record<string, number | null>;
  conditions: Record<string, unknown>;
  parcel_environment?: ParcelEnvironment;
}

export interface RecommendNeedsClimate {
  status: 'needs_climate';
  parcel_environment: ParcelEnvironment;
}

export type RecommendResponse = RecommendOk | RecommendNeedsClimate;

export interface EvidenceItem {
  trial_id: string;
  variety: string;
  site: string;
  year: number | null;
  yield_kg_ha: number | null;
  irrigation_regime: string | null;
  production_system: string | null;
  source_id: string | null;
  confidence: string | null;
  tier: EvidenceTier;
  /** Forage mode: `yield_kg_ha` is kg dry matter/ha. */
  basis: YieldBasis | null;
}

export interface EvidencePage {
  items: EvidenceItem[];
  total: number;
  page: number;
  page_size: number;
  purpose: Purpose;
  tier: EvidenceTier;
}
