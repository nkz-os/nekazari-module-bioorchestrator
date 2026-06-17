/**
 * BioOrchestrator API Service
 *
 * Centralized NKZClient-based API client. Replaces the broken dadisApi.ts
 * and scattered raw fetch() calls across components.
 *
 * DAD-IS note: FAO prohibits commercial use of their API.
 * Each user must provide their own DAD-IS API credentials.
 * When unconfigured, the UI shows a setup prompt + fallback to
 * IkerKeta's existing livestock/GBIF connectors.
 */

const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";
const BASE = `${API_BASE}/api/bioorchestrator`;
// Direct graph path bypasses api-gateway auth for public reference data endpoints
const GRAPH = `${API_BASE}/api`;

// ── DAD-IS per-user credentials (localStorage) ──────────────────────────────

interface DadisCredentials { apiUrl: string; apiToken: string; }
const DADIS_STORAGE_KEY = 'bioorchestrator.dadis.credentials';

export function getDadisCredentials(): DadisCredentials | null {
  try { const raw = localStorage.getItem(DADIS_STORAGE_KEY); if (!raw) return null; const creds = JSON.parse(raw); if (creds.apiUrl && creds.apiToken) return creds; return null; } catch { return null; }
}
export function setDadisCredentials(apiUrl: string, apiToken: string): void { localStorage.setItem(DADIS_STORAGE_KEY, JSON.stringify({ apiUrl, apiToken })); }
export function clearDadisCredentials(): void { localStorage.removeItem(DADIS_STORAGE_KEY); }

function dadisHeaders(): Record<string, string> {
  const creds = getDadisCredentials();
  if (!creds) return {};
  return { 'X-Dadis-Api-Url': creds.apiUrl, 'X-Dadis-Api-Token': creds.apiToken };
}

// ── Auth helper — reads JWT from Keycloak (injected by host) ──────────────

function getAuthToken(): string | null {
  if (typeof window === 'undefined') return null;
  try {
    const keycloak = (window as any).keycloak;
    if (keycloak?.token) return keycloak.token;
  } catch { /* keycloak not available */ }
  return null;
}

export function authHeaders(): Record<string, string> {
  const token = getAuthToken();
  return token ? { 'Authorization': `Bearer ${token}` } : {};
}

// ── Simple fetch wrapper ────────────────────────────────────────────────────
// Uses JWT Bearer token for auth (the backend/bioorchestrator expects JWT,
// not session cookies). Gateway routes (/api/bioorchestrator/*) also pick it up.

async function get(path: string, extraHeaders?: Record<string, string>): Promise<any> {
  const headers: Record<string, string> = { ...authHeaders(), ...extraHeaders };
  const url = path.startsWith("http") ? path : `${BASE}${path}`;
  const resp = await fetch(url, { headers, credentials: 'include' });
  // 401 = not authenticated, expected for public pages — return null silently
  if (resp.status === 401) return null;
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const ct = resp.headers.get('content-type') || '';
  return ct.includes('application/json') ? resp.json() : resp.text();
}

async function post(path: string, body?: any, extraHeaders?: Record<string, string>): Promise<any> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json', ...authHeaders(), ...extraHeaders };
  const url = path.startsWith("http") ? path : `${BASE}${path}`;
  const resp = await fetch(url, { method: 'POST', headers, body: body ? JSON.stringify(body) : undefined, credentials: 'include' });
  // 401 = not authenticated, expected for public pages — return null silently
  if (resp.status === 401) return null;
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const ct = resp.headers.get('content-type') || '';
  return ct.includes('application/json') ? resp.json() : resp.text();
}

// ── Crop Catalog Types ────────────────────────────────────────────────────

export interface CropItem {
  uri: string;
  name: string;
  scientificName: string;
  dataProvider: string;
  variety_count: number;
  has_kc: boolean;
  has_thermal: boolean;
  has_npk?: boolean;
  has_rotation?: boolean;
  registered_varieties?: number;
}

export interface CropDetail {
  uri: string;
  name: string;
  scientificName: string;
  dataProvider: string;
  data_available: {
    kc: boolean;
    d1_d2: boolean;
    mds: boolean;
    thermal: boolean;
    soil_suitability: boolean;
    npk: boolean;
  };
  varieties: { uri: string; name: string }[];
  phenology: Record<string, any>[];
  heat_tolerance: Record<string, any>[];
  soil_suitability: Record<string, any>[];
  nutrient_profile: Record<string, any>[];
}

// ── Crop Catalog API functions ──────────────────────────────────────────────

export function useCropApi() {
  async function getCatalog(params?: {
    source?: string;
    q?: string;
    parent?: string;
  }): Promise<{ crops: CropItem[]; total: number }> {
    const searchParams = new URLSearchParams();
    if (params?.source) searchParams.set('source', params.source);
    if (params?.q) searchParams.set('q', params.q);
    if (params?.parent) searchParams.set('parent', params.parent);
    const qs = searchParams.toString();
    return get(`/api/crop/catalog${qs ? `?${qs}` : ''}`);
  }

  async function getCropDetail(cropId: string): Promise<CropDetail> {
    return get(`/api/crop/catalog/${encodeURIComponent(cropId)}`);
  }

  async function triggerIngest(source: string, speciesFilter?: string): Promise<any> {
    const params = new URLSearchParams({ source });
    if (speciesFilter) params.set('species_filter', speciesFilter);
    return post(`/api/crop/catalog/ingest?${params}`);
  }

  async function contributeParameter(body: {
    crop_id: string;
    params: Record<string, number>;
    provenance: { doi: string; author?: string; year?: number; institution?: string; method?: string; conditions?: string };
  }): Promise<any> {
    return post('/api/crop/catalog/contribute', body);
  }

  async function getThermalSummary(): Promise<{
    total_species: number;
    with_thermal: number;
    without_thermal: number;
  }> {
    return get('/api/crop/catalog/thermal-summary');
  }

  async function getNpkSummary(): Promise<{
    total_species: number;
    with_npk: number;
    without_npk: number;
  }> {
    return get('/api/crop/catalog/npk-summary');
  }

  async function triggerDeriveThermal(): Promise<{ status: string; message: string }> {
    return post('/api/crop/catalog/derive-thermal');
  }

  return { getCatalog, getCropDetail, triggerIngest, contributeParameter, getThermalSummary, getNpkSummary, triggerDeriveThermal };
}

// ── Hook (no useAuth — relies on httpOnly cookie) ───────────────────────────

export function useBioApi() {
  return {
    getSources: () => get(`${GRAPH}/graph/agriculture/sources`),
    runPipeline: (body: any) => post('/api/pipeline/run', body),
    getPipelineHistory: (limit = 5) => get(`/api/pipeline/history?limit=${limit}`),
    getSpecies: () => get(`${GRAPH}/graph/species`),
    getPhenologyParams: (params: URLSearchParams) => get(`${GRAPH}/graph/phenology-params?${params.toString()}`),
    contributePhenology: (params: URLSearchParams) => post(`/api/graph/phenology-params/contribute?${params.toString()}`),
    getHeatTolerance: (species: string) => get(`${GRAPH}/graph/heat-tolerance?species=${encodeURIComponent(species)}`),
    getNutrientProfile: (species: string, stage?: string) => get(`${GRAPH}/graph/nutrient-profile?species=${encodeURIComponent(species)}${stage ? `&stage=${encodeURIComponent(stage)}` : ''}`),
    getNextCrop: (crop: string) => get(`${GRAPH}/graph/recommendations/next-crop?previous_crop=${encodeURIComponent(crop)}`),
    getSoilSuitability: (species: string) => get(`${GRAPH}/graph/soil-suitability?species=${encodeURIComponent(species)}`),
    getSoilData: (lat: number, lon: number) => get(`${GRAPH}/graph/soil-data?lat=${lat}&lon=${lon}`),
    getProtectedArea: (lat: number, lon: number) => get(`${GRAPH}/graph/protected-area-check?lat=${lat}&lon=${lon}`),
    getVarieties: (species: string) => get(`${GRAPH}/graph/varieties?species=${encodeURIComponent(species)}`),
    getPesticides: (crop: string) => get(`${GRAPH}/graph/pesticides?crop=${encodeURIComponent(crop)}`),
    getPollinators: (lat: number, lon: number) => get(`${GRAPH}/graph/pollinators?lat=${lat}&lon=${lon}`),
    getTerrain: (lat: number, lon: number) => get(`${GRAPH}/graph/terrain?lat=${lat}&lon=${lon}`),
    getClimateReference: (lat: number, lon: number) => get(`${GRAPH}/graph/climate-reference?lat=${lat}&lon=${lon}`),
    getRotationConstraints: (crop: string) => get(`${GRAPH}/graph/rotation-constraints?crop=${encodeURIComponent(crop)}`),
    getGraphSpecies: () => get(`${GRAPH}/graph/species`),
    simulateCrop: (baseline: string, scenario: string) => get(`${GRAPH}/graph/recommendations/simulate?baseline_crop=${encodeURIComponent(baseline)}&scenario_crop=${encodeURIComponent(scenario)}`),
    getDadisCountries: () => get('/api/dadis/countries', dadisHeaders()),
    getDadisSpecies: () => get('/api/dadis/species', dadisHeaders()),
    getDadisBreeds: (classification = 'all', countryIds?: string[], speciesIds?: number[]) => post('/api/dadis/breeds', { classification, countryIds: countryIds || [], speciesIds: speciesIds || [] }, dadisHeaders()),
    getDadisBreedById: (breedId: string, lang = 'en') => get(`/api/dadis/breeds/${encodeURIComponent(breedId)}?lang=${lang}`, dadisHeaders()),
    getParcelVegetation: (parcelId: string, index = 'ndvi', period = '3m'): Promise<VegetationData> =>
      get(`/api/parcel/${encodeURIComponent(parcelId)}/vegetation?index=${index}&period=${period}`),
    getParcelSoil: (parcelId: string): Promise<SoilData> =>
      get(`/api/parcel/${encodeURIComponent(parcelId)}/soil`),
    getAgricultureCrops: () => get(`${GRAPH}/graph/agriculture/crops`),
    getTrialSites: () => get(`${GRAPH}/graph/agriculture/trial-sites`),
    extrapolateVarieties: (params: Record<string, string>) => {
      const qs = new URLSearchParams(params).toString();
      return get(`${GRAPH}/graph/agriculture/extrapolate?${qs}`);
    },
    getRegenerativeSequence: (params: Record<string, string>) => {
      const qs = new URLSearchParams(params).toString();
      return get(`${GRAPH}/graph/agriculture/regenerative-sequence?${qs}`);
    },
    getClimateClasses: () => get(`${GRAPH}/graph/reference/climate-classes`),
    getSoilTypes: () => get(`${GRAPH}/graph/reference/soil-types`),
  };
}

// ── Parcel Data Types ───────────────────────────────────────────────────────

export interface VegetationObservation {
  date: string;
  value: number;
}

export interface VegetationTrend {
  direction: 'up' | 'down' | 'stable';
  delta: number;
  label: string;
}

export interface VegetationData {
  available: boolean;
  index: string;
  period: string;
  observations: VegetationObservation[];
  current: number | null;
  trend: VegetationTrend | null;
  count: number;
  source?: string;
  processor?: string;
  message?: string;
}

export interface SoilHorizon {
  depthFrom: number;
  depthTo: number;
  sand?: number;
  silt?: number;
  clay?: number;
  organicCarbon?: number;
  ph?: number;
  cec?: number;
  availableWaterCapacity?: number;
  fieldCapacity?: number;
  wiltingPoint?: number;
  usdaTextureClass?: string;
}

export interface SoilData {
  available: boolean;
  entityId?: string;
  horizons: SoilHorizon[];
  hydrologicGroup?: string;
  source?: string;
  message?: string;
}

export interface ParcelItem {
  id: string;
  name: string;
}

export async function fetchParcels(): Promise<ParcelItem[]> {
  try {
    const resp = await fetch(
      `${API_BASE}/ngsi-ld/v1/entities?type=AgriParcel&options=keyValues&limit=500`,
      { headers: { "Accept": "application/ld+json", ...authHeaders() }, credentials: "include" }
    );
    if (!resp.ok) return [];
    const entities = await resp.json();
    if (!Array.isArray(entities)) return [];
    return entities.map((e: any) => ({
      id: e.id,
      name: e.name?.value || e.name || e.id?.split(":")?.pop() || e.id,
    }));
  } catch (err) {
    console.warn('[fetchParcels] Failed to fetch parcels:', err instanceof Error ? err.message : String(err));
    return [];
  }
}

// ── F4: Crop-Health Integration ──────────────────────────────────────────────

export interface AssignCropRequest {
  parcel_id: string;
  variety_uri: string;
  crop_uri: string;
  management: "organic" | "conventional";
  season_start: string;
  season_end: string;
}

export interface AssignCropResponse {
  status: "assigned" | "cleared";
  parcel_id: string;
  variety: string;
  crop: string;
  management: string;
}

export interface CropContextResponse {
  parcel_id: string;
  crop: { eppo: string; name: string; scientific_name: string | null };
  variety: { name: string; uri: string } | null;
  management: string | null;
  season: {
    start: string | null;
    end: string | null;
    gdd_accumulated: number | null;
    current_stage: string | null;
  };
  phenology: Record<string, unknown> | null;
  thermal_limits: Record<string, unknown> | null;
  soil: {
    requirements: Record<string, unknown> | null;
    actual: Record<string, unknown> | null;
    suitability: Record<string, unknown> | null;
  };
  soil_sensors: Record<string, unknown> | null;
  phenology_source: string;
  match_level: string;
  provenance: Record<string, unknown> | null;
}

export interface YieldPotentialResponse {
  variety: string;
  crop: string;
  target_environment: Record<string, unknown>;
  expected_yield_kg_ha: number;
  confidence_interval: [number, number];
  trials_analyzed: number;
  similar_sites: string[];
  current_estimated_yield_kg_ha?: number;
  yield_gap_kg_ha?: number;
  yield_gap_pct?: number;
  limiting_factor?: string;
  stage_ky: Record<string, number>;
}

export async function assignCrop(
  data: AssignCropRequest
): Promise<AssignCropResponse> {
  const res = await fetch(`${API_BASE}/api/graph/agriculture/assign-crop`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(data),
    credentials: "include",
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function getCropContext(
  parcelId: string,
  gdd?: number
): Promise<CropContextResponse> {
  const params = new URLSearchParams({ parcel_id: parcelId });
  if (gdd !== undefined) params.append("gdd", String(gdd));
  const res = await fetch(
    `${API_BASE}/api/graph/agriculture/crop-context?${params}`,
    { headers: authHeaders(), credentials: "include" }
  );
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function getYieldPotential(
  variety: string,
  crop: string,
  climateClass?: string,
  soilType?: string,
  parcelId?: string
): Promise<YieldPotentialResponse> {
  const params = new URLSearchParams({ variety, crop });
  if (climateClass) params.append("climate_class", climateClass);
  if (soilType) params.append("soil_type", soilType);
  if (parcelId) params.append("parcel_id", parcelId);
  const res = await fetch(
    `${API_BASE}/api/graph/agriculture/yield-potential?${params}`,
    { headers: authHeaders(), credentials: "include" }
  );
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

// ── F5: ParcelHealth History & Alerts ─────────────────────────────────────

export interface HistoryPoint {
  date: string;
  cwsi: number | null;
  mds: number | null;
  balance: number | null;
}

export interface AlertItem {
  type: string;
  severity: string;
  recommended_action: string;
  timestamp: string;
  stage?: string;
  eco_impact?: {
    pollinator_species: string[];
    risk_level: string;
    recommended_window: string;
    safer_alternatives: string[];
  };
}

export async function fetchAssessmentHistory(parcelId: string, days: number = 30): Promise<HistoryPoint[]> {
  const resp = await fetch(
    `${API_BASE}/api/crop-health/assessments/history?parcelId=${encodeURIComponent(parcelId)}&days=${days}`,
    { headers: authHeaders(), credentials: "include" }
  );
  if (!resp.ok) return [];
  const data = await resp.json();
  return data.points || [];
}

export async function fetchAlerts(parcelId: string): Promise<AlertItem[]> {
  const resp = await fetch(
    `${API_BASE}/api/graph/agriculture/alerts?parcel_id=${encodeURIComponent(parcelId)}`,
    { headers: authHeaders(), credentials: "include" }
  );
  if (!resp.ok) return [];
  const data = await resp.json();
  return data.alerts || [];
}

export async function fetchOrganicInputs(crop: string): Promise<OrganicInputsResult> {
  const resp = await fetch(
    `${API_BASE}/api/graph/agriculture/organic-inputs?crop=${encodeURIComponent(crop)}`,
    { headers: authHeaders(), credentials: "include" }
  );
  if (!resp.ok) return {inputs: [], source_unavailable: false};
  return resp.json();
}

// ── F6: Regenerative Sequence Types ──────────────────────────────────────

export interface CropListCrop {
  eppo_code: string;
  scientific_name: string;
}

export interface CoverCropAlternative {
  cover_crop: string;
  cover_crop_common: string;
  cover_crop_scientific: string;
  biomass_t_ha: number;
  c_n_ratio: number;
  n_available_kg_ha: number;
  type: string;
}

export interface WaterBalanceDetail {
  risk: string;
  crop_etc_mm: number;
  growing_season_et0_mm: number;
  growing_season_rainfall_mm: number;
  effective_rainfall_mm: number;
  soil_awc_mm: number;
  water_supply_mm: number;
  deficit_mm: number;
  avg_annual_rainfall_mm: number;
  avg_annual_et0_mm: number | null;
  soil_type: string | null;
  cover_kc: number;
  method: string;
}

export interface RegenerativeSequenceResult {
  cover_crop: string;
  cover_crop_common: string;
  cover_crop_scientific: string;
  cover_crop_type: string;
  cover_biomass_t_ha: number;
  c_n_ratio: number;
  n_cover_total_kg_ha: number;
  n_cover_available_kg_ha: number;
  n_protein_fixed_kg_ha: number;
  protein_crop: string;
  protein_crop_scientific: string;
  protein_crop_common: string;
  protein_variety: string | null;
  expected_protein_yield_kg_ha: number | null;
  protein_kg_ha: number;
  management_mode: string;
  organic_data_warning: string | null;
  termination_gdd: number;
  termination_method: string;
  cover_crop_sowing_date: string;
  termination_date_estimate: string;
  protein_crop_sowing_date: string;
  protein_crop_harvest_date: string;
  water_balance_risk: string;
  water_balance_detail: WaterBalanceDetail;
  alternatives: CoverCropAlternative[];
  variety_trials: Record<string, unknown>[];
  management_distribution: { cover_crop_params: string; variety_trials: string };
  provenance: { cover_crop_source: string; n_fixation_source: string; yield_source: string; climate_source: string };
  carbon_projection?: {
    current_soc_pct: number | null;
    target_soc_pct: number;
    projected_soc_pct: number | null;
    soc_delta_pct: number;
    co2e_sequestered_ton_ha: number;
    fertilizer_n_saved_kg_ha: number;
    fertilizer_savings_eur_ha: number;
    years_to_target: number | null;
    soil_texture: string;
    methodology: string;
  };
  error?: string;
}

export interface OrganicInputItem {
  product: string;
  active_substance: string;
  category: string;
}

export interface OrganicInputsResult {
  inputs: OrganicInputItem[];
  source_unavailable?: boolean;
}
