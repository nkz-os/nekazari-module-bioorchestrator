/** Client of /api/graph/planning (contract 1.0). Empty values are never sent. */
import { API_BASE, authHeaders } from '../services/api';
import type { SourceAttributionItem } from '../types/attribution';
import type {
  EstimatesResponse, FarmResponse, FitResponse, Management, PlanIn, PlanWriteResponse, Regime, VarietiesResponse,
} from './types';

const PLANNING = `${API_BASE}/api/graph/planning`;
const enc = encodeURIComponent;

export class PlanningApiError extends Error {
  constructor(public readonly status: number, public readonly code: string | null) {
    super(`planning api ${status}${code ? ` ${code}` : ''}`);
  }
}

export function buildQuery(params: Record<string, string | number | null | undefined>): string {
  const qs = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue;
    qs.set(key, String(value));
  }
  const text = qs.toString();
  return text ? `?${text}` : '';
}

async function request<T>(method: 'GET' | 'PUT', url: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  const headers: Record<string, string> = { ...authHeaders() };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const resp = await fetch(url, {
    method, headers, credentials: 'include', signal, body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!resp.ok) {
    let code: string | null = null;
    try {
      const payload = await resp.json();
      code = typeof payload?.detail?.code === 'string' ? payload.detail.code : null;
    } catch {
      code = null;
    }
    throw new PlanningApiError(resp.status, code);
  }
  return (await resp.json()) as T;
}

export const fetchFarm = (campaign?: number, parcelId?: string, signal?: AbortSignal) =>
  request<FarmResponse>('GET', `${PLANNING}/farm${buildQuery({ campaign, parcel_id: parcelId })}`, undefined, signal);

export const fetchFit = (parcelId: string, campaign: number, regime: Regime | null, signal?: AbortSignal) =>
  request<FitResponse>('GET', `${PLANNING}/parcels/${enc(parcelId)}/fit${buildQuery({ campaign, regime })}`, undefined, signal);

export const fetchVarieties = (parcelId: string, crop: string, regime: Regime, management: Management, signal?: AbortSignal) =>
  request<VarietiesResponse>('GET', `${PLANNING}/parcels/${enc(parcelId)}/varieties${buildQuery({ crop, regime, management })}`, undefined, signal);

export interface EstimatesQuery { crop: string; variety: string | null; regime: Regime; management: Management; sowingDate: string | null }
export const fetchEstimates = (parcelId: string, q: EstimatesQuery, signal?: AbortSignal) =>
  request<EstimatesResponse>('GET', `${PLANNING}/parcels/${enc(parcelId)}/estimates${buildQuery({
    crop: q.crop, variety: q.variety, regime: q.regime, management: q.management, sowing_date: q.sowingDate,
  })}`, undefined, signal);

export const putPlan = (parcelId: string, body: PlanIn) =>
  request<PlanWriteResponse>('PUT', `${PLANNING}/parcels/${enc(parcelId)}/plan`, body);

export const fetchAttributions = (signal?: AbortSignal) =>
  request<{ attributions: SourceAttributionItem[]; total: number }>(
    'GET', `${API_BASE}/api/graph/agriculture/sources/attributions`, undefined, signal);
