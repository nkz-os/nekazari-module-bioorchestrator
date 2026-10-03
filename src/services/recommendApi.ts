/**
 * Client of the crop recommendation endpoints (conditions, evidence, parcel).
 * Empty values are never sent: the backend rejects e.g. `climate_class=` with 422.
 */

import { API_BASE, authHeaders } from './api';
import type { EvidencePage, RecommendResponse, Similarity } from '../types/recommend';

export type QueryValue = string | number | boolean | null | undefined | string[];
export type QueryParams = Record<string, QueryValue>;

const GRAPH = `${API_BASE}/api/graph`;

export function buildRecommendQuery(params: QueryParams): string {
  const qs = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue;
    if (Array.isArray(value)) {
      const parts = value.filter((v) => v !== '');
      if (parts.length === 0) continue;
      qs.set(key, parts.join(','));
      continue;
    }
    qs.set(key, String(value));
  }
  return qs.toString();
}

async function getJson<T>(path: string, params: QueryParams, signal?: AbortSignal): Promise<T> {
  const query = buildRecommendQuery(params);
  const url = query ? `${GRAPH}${path}?${query}` : `${GRAPH}${path}`;
  const resp = await fetch(url, { headers: authHeaders(), credentials: 'include', signal });
  if (!resp.ok) throw new Error(String(resp.status));
  return (await resp.json()) as T;
}

export function fetchRecommendParcel(
  parcelId: string,
  opts: QueryParams = {},
  signal?: AbortSignal,
): Promise<RecommendResponse> {
  return getJson<RecommendResponse>(`/recommend/parcel/${encodeURIComponent(parcelId)}`, opts, signal);
}

export function fetchRecommendConditions(
  cond: QueryParams,
  signal?: AbortSignal,
): Promise<RecommendResponse> {
  return getJson<RecommendResponse>('/agriculture/recommend', cond, signal);
}

export function fetchEvidence(
  cond: QueryParams,
  crop: string,
  page = 1,
  similarity: Similarity = 'koppen',
  signal?: AbortSignal,
): Promise<EvidencePage> {
  return getJson<EvidencePage>(
    '/agriculture/recommend/evidence',
    { ...cond, crop, page, similarity },
    signal,
  );
}
