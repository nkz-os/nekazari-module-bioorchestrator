/**
 * BioOrchestrator API service: sources dashboard, crop simulation, parcel list.
 */

import { useMemo } from 'react';

const API_BASE = (import.meta as any).env?.VITE_API_URL || "https://nkz.robotika.cloud";
const BASE = `${API_BASE}/api/bioorchestrator`;
// Direct graph path bypasses api-gateway auth for public reference data endpoints
const GRAPH = `${API_BASE}/api`;
export { API_BASE };

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

/** Non-2xx answer carrying the backend `{detail:{code,message}}` body when present. */
export class ApiError extends Error {
  constructor(readonly status: number, readonly code: string | null, message: string) {
    super(message);
    this.name = 'ApiError';
  }
}

/** Reads `{detail:{code,message}}` (or a string `detail`) from a failed response; falls back to `HTTP <status>`. */
export async function apiErrorFrom(resp: Response): Promise<ApiError> {
  let code: string | null = null;
  let message = `HTTP ${resp.status}`;
  try {
    const body = await resp.json();
    const d = body?.detail;
    if (d && typeof d === 'object') {
      if (typeof d.code === 'string') code = d.code;
      if (typeof d.message === 'string' && d.message) message = d.message;
    } else if (typeof d === 'string' && d) {
      message = d;
    }
  } catch { /* body is not JSON */ }
  return new ApiError(resp.status, code, message);
}

/** POST that throws ApiError (status, code, message) on failure; 401 returns null like post(). */
/** GET with the detailed error body; 401 is an error here too (a job poll has no anonymous answer). */
async function getDetailed(path: string): Promise<any> {
  const url = path.startsWith('http') ? path : `${BASE}${path}`;
  const resp = await fetch(url, { headers: authHeaders(), credentials: 'include' });
  if (!resp.ok) throw await apiErrorFrom(resp);
  return resp.json();
}

async function postDetailed(path: string): Promise<any> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json', ...authHeaders() };
  const url = path.startsWith('http') ? path : `${BASE}${path}`;
  const resp = await fetch(url, { method: 'POST', headers, credentials: 'include' });
  if (resp.status === 401) return null;
  if (!resp.ok) throw await apiErrorFrom(resp);
  return resp.json();
}

// ── Hook ────────────────────────────────────────────────────────────────────

export function useBioApi() {
  // eslint-disable-next-line react-hooks/exhaustive-deps -- functions close over module scope only
  return useMemo(() => ({
    getSources: () => get(`${GRAPH}/graph/agriculture/sources`),
    /** Starts (or joins: same inputs, same day) a simulation job. Same inputs as runCropSimulation. */
    startCropSimulationJob: async (p: {
      parcelId: string;
      cropSlug?: string;
      sowingDate?: string;
      irrigation: 'rainfed' | 'full';
      engine: string;
    }) => {
      const params = new URLSearchParams({ parcel_id: p.parcelId, irrigation: p.irrigation, engine: p.engine });
      if (p.cropSlug) params.set('crop_slug', p.cropSlug);
      if (p.sowingDate) params.set('sowing_date', p.sowingDate);
      const view = await postDetailed(`${GRAPH}/graph/agriculture/crop-simulation/jobs?${params.toString()}`);
      if (view == null) throw new ApiError(401, null, 'HTTP 401');
      return view;
    },
    getCropSimulationJob: (jobId: string) =>
      getDetailed(`${GRAPH}/graph/agriculture/crop-simulation/jobs/${encodeURIComponent(jobId)}`),
  }), []);
}

export interface ParcelItem {
  id: string;
  name: string;
}

export async function fetchParcels(): Promise<ParcelItem[]> {
  try {
    const resp = await fetch(
      `${API_BASE}/ngsi-ld/v1/entities?type=AgriParcel&options=keyValues&limit=500`,
      {
        headers: {
          "Accept": "application/json",
          "Link": '<https://nekazari.robotika.cloud/ngsi-ld-context.json>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"',
          ...authHeaders(),
        },
        credentials: "include",
      }
    );
    if (!resp.ok) return [];
    const entities = await resp.json();
    if (!Array.isArray(entities)) return [];
    return entities.map((e: any) => ({
      id: e.id,
      name: e.name?.value || e.name || e.id?.split(":")?.pop() || e.id,
    }));
  } catch (err) {
    console.warn('[fetchParcels] Failed:', err instanceof Error ? err.message : String(err));
    return [];
  }
}
