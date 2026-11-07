/** Client of /api/graph/planning (contract 1.1). Empty values are never sent. */
import { API_BASE, authHeaders } from '../services/api';
import type { SourceAttributionItem } from '../types/attribution';
import { API_ORIGIN } from './cropHealthClient';
import { CONTRACT_VERSION } from './types';
import type {
  AquacropResponse, CalendarResponse, CoverReferenceResponse, EstimatesResponse, FarmResponse, FitResponse, Management, PlanIn, PlanWriteResponse, Regime, VarietiesResponse,
} from './types';

const PLANNING = `${API_BASE}/api/graph/planning`;
const enc = encodeURIComponent;

export class PlanningApiError extends Error {
  constructor(public readonly status: number, public readonly code: string | null) {
    super(`planning api ${status}${code ? ` ${code}` : ''}`);
    this.name = 'PlanningApiError';
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

const major = (version: string): string => version.split('.')[0];

/** A minor bump is additive and accepted; a major bump is a different contract and refused. */
function assertContract(payload: unknown, status: number): void {
  const version = (payload as { contract_version?: unknown } | null | undefined)?.contract_version;
  if (typeof version === 'string' && major(version) !== major(CONTRACT_VERSION)) {
    throw new PlanningApiError(status, 'contract_mismatch');
  }
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
  let payload: unknown;
  try {
    payload = await resp.json();
  } catch {
    throw new PlanningApiError(resp.status, 'invalid_response');
  }
  assertContract(payload, resp.status);
  return payload as T;
}

export const fetchFarm = (campaign?: number, parcelId?: string, signal?: AbortSignal) =>
  request<FarmResponse>('GET', `${PLANNING}/farm${buildQuery({ campaign, parcel_id: parcelId })}`, undefined, signal);

/** `refreshSoil` is sent only for the read right after a soil write: the server then skips its soil cache. */
export const fetchFit = (parcelId: string, campaign: number, regime: Regime | null, signal?: AbortSignal, refreshSoil = false) =>
  request<FitResponse>('GET', `${PLANNING}/parcels/${enc(parcelId)}/fit${buildQuery({ campaign, regime, refresh_soil: refreshSoil ? 1 : null })}`, undefined, signal);

export const fetchVarieties = (parcelId: string, crop: string, regime: Regime, management: Management, signal?: AbortSignal) =>
  request<VarietiesResponse>('GET', `${PLANNING}/parcels/${enc(parcelId)}/varieties${buildQuery({ crop, regime, management })}`, undefined, signal);

export interface EstimatesQuery { crop: string; variety: string | null; regime: Regime; management: Management; sowingDate: string | null }
export const fetchEstimates = (parcelId: string, q: EstimatesQuery, signal?: AbortSignal) =>
  request<EstimatesResponse>('GET', `${PLANNING}/parcels/${enc(parcelId)}/estimates${buildQuery({
    crop: q.crop, variety: q.variety, regime: q.regime, management: q.management, sowing_date: q.sowingDate,
  })}`, undefined, signal);

/** The engine takes neither variety nor management: changing them must not restart a simulation. */
export type AquacropQuery = Pick<EstimatesQuery, 'crop' | 'regime' | 'sowingDate'> & { sowingMethod?: 'direct' | null };
export const fetchAquacrop = (parcelId: string, q: AquacropQuery, signal?: AbortSignal, opts: { retry?: boolean; jobId?: string | null } = {}) =>
  request<AquacropResponse>('GET', `${PLANNING}/parcels/${enc(parcelId)}/estimates/aquacrop${buildQuery({
    crop: q.crop, regime: q.regime, sowing_date: q.sowingDate, sowing_method: q.sowingMethod ?? null,
    retry: opts.retry ? 'true' : null, job_id: opts.jobId,
  })}`, undefined, signal);

export const fetchCoverReference = (species: string, season: 'autumn' | 'spring', signal?: AbortSignal) =>
  request<CoverReferenceResponse>('GET', `${PLANNING}/cover-crops/${enc(species)}/reference${buildQuery({ season })}`, undefined, signal);

export const putPlan = (parcelId: string, body: PlanIn) =>
  request<PlanWriteResponse>('PUT', `${PLANNING}/parcels/${enc(parcelId)}/plan`, body);

export const fetchAttributions = (signal?: AbortSignal) =>
  request<{ attributions: SourceAttributionItem[]; total: number }>(
    'GET', `${API_BASE}/api/graph/agriculture/sources/attributions`, undefined, signal);

export interface CalendarQuery { campaign: number; crop: string; cover: string | null; mainSowing: string | null }
export const fetchCalendar = (parcelId: string, q: CalendarQuery, signal?: AbortSignal) =>
  request<CalendarResponse>('GET', `${PLANNING}/parcels/${enc(parcelId)}/calendar${buildQuery({
    campaign: q.campaign, crop: q.crop, cover: q.cover, main_sowing: q.mainSowing,
  })}`, undefined, signal);

/**
 * W10: the regime the farmer chose is remembered on the parcel (entity-manager is the parcel's writer),
 * so it is not asked again and in-season forecasts can run. Only what the farmer chose; never a guess.
 */
export async function saveParcelRegime(parcelId: string, regime: Regime): Promise<void> {
  const res = await fetch(`${API_BASE}/api/entities/parcels/${enc(parcelId)}`, {
    method: 'PATCH', credentials: 'include',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify({ attributes: { irrigationRegime: { type: 'Property', value: regime } } }),
  });
  if (!res.ok) throw new PlanningApiError(res.status, null);
}

export const ALLOTMENT_ATTRIBUTE = 'nkz:irrigationAllotment';

/**
 * The water the farmer has for the campaign, in mm, kept on the parcel like the regime. `null` clears it.
 * The `nkz:` prefix is in the platform context; a bare name would land in the default vocabulary.
 */
export async function saveParcelAllotment(parcelId: string, mm: number | null, now: Date = new Date()): Promise<void> {
  const body = mm === null
    ? { remove: [ALLOTMENT_ATTRIBUTE] }
    : { attributes: { [ALLOTMENT_ATTRIBUTE]: { type: 'Property', value: mm, unitCode: 'MMT', observedAt: now.toISOString() } } };
  const res = await fetch(`${API_BASE}/api/entities/parcels/${enc(parcelId)}`, {
    method: 'PATCH', credentials: 'include',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new PlanningApiError(res.status, null);
}

/** The 12 USDA texture classes exactly as the soil module emits and accepts them. */
export const USDA_TEXTURE_CLASSES = [
  'sand', 'loamy-sand', 'sandy-loam', 'loam', 'silt-loam', 'silt',
  'sandy-clay-loam', 'clay-loam', 'silty-clay-loam', 'sandy-clay', 'silty-clay', 'clay',
] as const;
export type UsdaTextureClass = (typeof USDA_TEXTURE_CLASSES)[number];

/** A write to a sibling module (soil, field operations) that was refused; `code` is its error code when it sent one. */
async function refused(res: Response): Promise<PlanningApiError> {
  let code: string | null = null;
  try {
    const body = await res.json();
    code = typeof body?.detail?.code === 'string' ? body.detail.code : null;
  } catch {
    code = null;
  }
  return new PlanningApiError(res.status, code);
}

async function writeToModule(method: 'POST' | 'PUT' | 'DELETE', url: string, body?: unknown): Promise<Response> {
  const headers: Record<string, string> = { ...authHeaders() };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const res = await fetch(url, { method, headers, credentials: 'include', body: body === undefined ? undefined : JSON.stringify(body) });
  if (!res.ok) throw await refused(res);
  return res;
}

/** The soil module's own parcel path segment: the id without its URN prefix (or anything up to "AgriParcel:"), URL-encoded. */
export function soilParcelSegment(parcelId: string): string {
  const canonical = 'urn:ngsi-ld:AgriParcel:';
  if (parcelId.startsWith(canonical)) return enc(parcelId.slice(canonical.length));
  const expanded = parcelId.match(/AgriParcel:(.+)$/);
  return enc(expanded ? expanded[1] : parcelId);
}

/** The farmer's own topsoil texture, kept by the soil module (provenance "entered"; a lab analysis still wins). 404: no soil profile yet. */
export const saveDeclaredTexture = async (parcelId: string, textureClass: UsdaTextureClass): Promise<void> => {
  await writeToModule('PUT', `${API_ORIGIN}/api/soil/parcel/${soilParcelSegment(parcelId)}/declared-texture`, { usda_texture_class: textureClass });
};
export const clearDeclaredTexture = async (parcelId: string): Promise<void> => {
  await writeToModule('DELETE', `${API_ORIGIN}/api/soil/parcel/${soilParcelSegment(parcelId)}/declared-texture`);
};

export const EPPO_PATTERN = /^[A-Z0-9]{5,6}$/;

export interface DeclaredCropOptions {
  variety?: string | null;
  yieldKgHa?: number | null;
}

/** A past main crop the farmer declares for a campaign; field operations keeps it with no dates. Returns the crop id. */
export async function saveDeclaredCrop(
  parcelId: string,
  campaign: number,
  eppo: string,
  opts?: DeclaredCropOptions,
): Promise<string> {
  const payload: Record<string, unknown> = { campaign, eppo };
  const trimmedVariety = opts?.variety?.trim();
  if (trimmedVariety) payload.variety = trimmedVariety;
  if (typeof opts?.yieldKgHa === 'number' && Number.isFinite(opts.yieldKgHa) && opts.yieldKgHa > 0) {
    payload.yield_kg_ha = opts.yieldKgHa;
  }
  const res = await writeToModule('POST', `${API_ORIGIN}/api/field-operations/parcels/${enc(parcelId)}/declared-crops`, payload);
  const body = await res.json().catch(() => null);
  return typeof body?.crop_id === 'string' ? body.crop_id : '';
}

/** Deletes a declared-only past crop for a campaign (never deletes a dated cycle). */
export async function deleteDeclaredCrop(parcelId: string, campaign: number): Promise<void> {
  await writeToModule('DELETE', `${API_ORIGIN}/api/field-operations/parcels/${enc(parcelId)}/declared-crops/${enc(String(campaign))}`);
}
