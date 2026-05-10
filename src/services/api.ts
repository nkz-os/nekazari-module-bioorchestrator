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

import { NKZClient, useAuth } from '@nekazari/sdk';

const BASE = '/api/bioorchestrator';

// ── DAD-IS per-user credentials (localStorage) ──────────────────────────────

interface DadisCredentials {
  apiUrl: string;
  apiToken: string;
}

const DADIS_STORAGE_KEY = 'bioorchestrator.dadis.credentials';

export function getDadisCredentials(): DadisCredentials | null {
  try {
    const raw = localStorage.getItem(DADIS_STORAGE_KEY);
    if (!raw) return null;
    const creds = JSON.parse(raw);
    if (creds.apiUrl && creds.apiToken) return creds;
    return null;
  } catch {
    return null;
  }
}

export function setDadisCredentials(apiUrl: string, apiToken: string): void {
  localStorage.setItem(DADIS_STORAGE_KEY, JSON.stringify({ apiUrl, apiToken }));
}

export function clearDadisCredentials(): void {
  localStorage.removeItem(DADIS_STORAGE_KEY);
}

function dadisHeaders(): Record<string, string> | undefined {
  const creds = getDadisCredentials();
  if (!creds) return undefined;
  return {
    'X-Dadis-Api-Url': creds.apiUrl,
    'X-Dadis-Api-Token': creds.apiToken,
  };
}

// ── Hook ───────────────────────────────────────────────────────────────────

export function useBioApi() {
  const { getToken, tenantId } = useAuth();
  const client = new NKZClient({
    baseUrl: BASE,
    getToken,
    getTenantId: () => tenantId,
  });

  return {
    // ── Sources ──────────────────────────────────────────────────────────
    getSources: () => client.get('/api/v1/sources'),

    // ── Pipeline ─────────────────────────────────────────────────────────
    runPipeline: (body: { sources?: string[]; limit?: number }) =>
      client.post('/api/pipeline/run', body),

    getPipelineHistory: (limit = 5) =>
      client.get(`/api/pipeline/history?limit=${limit}`),

    // ── Graph / Phenology ────────────────────────────────────────────────
    getSpecies: () => client.get('/api/graph/species'),

    getPhenologyParams: (params: URLSearchParams) =>
      client.get(`/api/graph/phenology-params?${params.toString()}`),

    contributePhenology: (params: URLSearchParams) =>
      client.post(`/api/graph/phenology-params/contribute?${params.toString()}`),

    getHeatTolerance: (species: string) =>
      client.get(`/api/graph/heat-tolerance?species=${encodeURIComponent(species)}`),

    getNutrientProfile: (species: string, stage?: string) => {
      const url = stage
        ? `/api/graph/nutrient-profile?species=${encodeURIComponent(species)}&stage=${encodeURIComponent(stage)}`
        : `/api/graph/nutrient-profile?species=${encodeURIComponent(species)}`;
      return client.get(url);
    },

    // ── Recommendations ──────────────────────────────────────────────────
    getNextCrop: (cropType: string) =>
      client.get(`/api/graph/recommendations/next-crop?previous_crop=${encodeURIComponent(cropType)}`),

    getSoilSuitability: (species: string) =>
      client.get(`/api/graph/soil-suitability?species=${encodeURIComponent(species)}`),

    getSoilData: (lat: number, lon: number) =>
      client.get(`/api/graph/soil-data?lat=${lat}&lon=${lon}`),

    getProtectedArea: (lat: number, lon: number) =>
      client.get(`/api/graph/protected-area-check?lat=${lat}&lon=${lon}`),

    getVarieties: (species: string) =>
      client.get(`/api/graph/varieties?species=${encodeURIComponent(species)}`),

    getPesticides: (crop: string) =>
      client.get(`/api/graph/pesticides?crop=${encodeURIComponent(crop)}`),

    getPollinators: (lat: number, lon: number) =>
      client.get(`/api/graph/pollinators?lat=${lat}&lon=${lon}`),

    getTerrain: (lat: number, lon: number) =>
      client.get(`/api/graph/terrain?lat=${lat}&lon=${lon}`),

    getClimateReference: (lat: number, lon: number) =>
      client.get(`/api/graph/climate-reference?lat=${lat}&lon=${lon}`),

    simulateCrop: (baseline: string, scenario: string) =>
      client.get(
        `/api/graph/recommendations/simulate?baseline_crop=${encodeURIComponent(baseline)}&scenario_crop=${encodeURIComponent(scenario)}`,
      ),

    // ── DAD-IS (per-user credentials) ────────────────────────────────────
    getDadisCountries: () =>
      client.get('/api/dadis/countries', { headers: dadisHeaders() } as any),

    getDadisSpecies: () =>
      client.get('/api/dadis/species', { headers: dadisHeaders() } as any),

    getDadisBreeds: (
      classification: string = 'all',
      countryIds?: string[],
      speciesIds?: number[],
    ) =>
      client.post(
        '/api/dadis/breeds',
        {
          classification,
          countryIds: countryIds || [],
          speciesIds: speciesIds || [],
        },
        { headers: dadisHeaders() } as any,
      ),

    getDadisBreedById: (breedId: string, lang: string = 'en') =>
      client.get(
        `/api/dadis/breeds/${encodeURIComponent(breedId)}?lang=${lang}`,
        { headers: dadisHeaders() } as any,
      ),
  };
}
