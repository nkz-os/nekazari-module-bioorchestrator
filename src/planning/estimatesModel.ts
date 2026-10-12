import type { SourceAttributionItem } from '../types/attribution';
import type { AquacropResponse, AquacropState, Basis, Estimate, EstimatesResponse, Moisture } from './types';

export const BASIS_ORDER: Basis[] = ['trial_evidence', 'gap_corrected', 'water_limited', 'potential'];

/** Rank of a basis; an unknown basis ranks after every known one. */
const rank = (basis: Basis): number => {
  const index = BASIS_ORDER.indexOf(basis);
  return index === -1 ? BASIS_ORDER.length : index;
};

export function sortEstimates(estimates: Estimate[]): Estimate[] {
  return [...estimates].sort((a, b) => rank(a.basis) - rank(b.basis));
}

/** Only a validated estimate is positive and only not_applicable is neutral; anything else warns. */
export function validationTone(status: Estimate['validation']['status']): 'positive' | 'warning' | 'default' {
  if (status === 'validated') return 'positive';
  return status === 'not_applicable' ? 'default' : 'warning';
}

export function uniqueAttributions(estimates: Estimate[]): SourceAttributionItem[] {
  const seen = new Map<string, SourceAttributionItem>();
  for (const e of estimates) for (const a of e.attributions) if (!seen.has(a.source_id)) seen.set(a.source_id, a);
  return [...seen.values()];
}

const DISAGREEMENT_RATIO = 1.5; // same value and rule as backend app/planning/estimates.py

/** Same-basis AND same-moisture model medians that differ by more than the ratio. Range-only figures and trials never count. */
export function disagrees(estimates: Estimate[]): boolean {
  const groups = new Map<string, number[]>();
  for (const e of estimates) {
    if (e.basis === 'trial_evidence' || !e.percentiles) continue;
    const key = `${e.basis}|${e.moisture?.basis}|${e.moisture?.pct}`;
    groups.set(key, [...(groups.get(key) ?? []), e.percentiles.p50]);
  }
  return [...groups.values()].some((v) => v.length >= 2 && Math.max(...v) > DISAGREEMENT_RATIO * Math.min(...v));
}

export const AQUACROP_POLL_MS = 3000;
export const AQUACROP_POLL_CAP_MS = 4 * 60 * 1000;

const PROVIDER = 'aquacrop';
export const aquacropPending = (data: EstimatesResponse): boolean =>
  data.unavailable.some((u) => u.provider_id === PROVIDER && u.reason === 'async');

/** What to do after an answer (null = none yet): keep polling, stop, or give up at the cap. */
export function pollDecision(state: AquacropState | null, elapsedMs: number): 'poll' | 'stop' | 'timeout' {
  if (state !== null && state !== 'queued' && state !== 'running') return 'stop';
  return elapsedMs >= AQUACROP_POLL_CAP_MS ? 'timeout' : 'poll';
}

export interface MergedEstimates { data: EstimatesResponse; progress: { done: number; total: number | null } | null }

/**
 * The /estimates answer plus the AquaCrop provider's, once it has settled. While it runs the "async"
 * row stays and `progress` says how far; a failure, timeout or lost request replaces it with its
 * reason, so a missing result is never shown as an empty one.
 */
export function mergeAquacrop(data: EstimatesResponse, aq: AquacropResponse | null, failure?: 'timeout' | 'request_failed'): MergedEstimates {
  if (!aquacropPending(data)) return { data, progress: null };
  const others = data.unavailable.filter((u) => !(u.provider_id === PROVIDER && u.reason === 'async'));
  if (failure) return { data: { ...data, unavailable: [...others, { provider_id: PROVIDER, reason: failure }] }, progress: null };
  if (!aq || aq.state === 'queued' || aq.state === 'running') {
    return { data, progress: { done: aq?.done ?? 0, total: aq?.total ?? null } };
  }
  const estimates = [...data.estimates, ...aq.estimates];
  return { data: { ...data, estimates, unavailable: [...others, ...aq.unavailable], disagreement: disagrees(estimates) }, progress: null };
}

export function moistureLabel(m: Moisture): { key: string; params: Record<string, unknown> } {
  if (m.basis === 'commercial') {
    return m.pct == null ? { key: 'planning.estimate.moisture.commercialNoPct', params: {} }
      : { key: 'planning.estimate.moisture.commercial', params: { pct: m.pct } };
  }
  return { key: `planning.estimate.moisture.${m.basis}`, params: {} };
}
