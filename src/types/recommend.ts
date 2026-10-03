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
  };
  trust: { level: TrustLevel; data_gaps: string[]; similarity: Similarity };
  varieties: VarietyRec[];
  evidence: { trial_count: number; sources: string[]; sites: string[]; years: [number, number] | null };
  assumptions: { id: string; value: unknown; citation: string }[];
}

export interface ParcelEnvironment {
  parcel_id: string;
  area_ha: number | null;
  centroid: { lat: number | null; lon: number | null };
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
}

export interface EvidencePage {
  items: EvidenceItem[];
  total: number;
  page: number;
  page_size: number;
}
