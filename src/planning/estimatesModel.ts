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

export type AquacropFailure = 'still_queued' | 'request_failed' | 'engine_not_deployed';

export interface MergedEstimates { data: EstimatesResponse; progress: { done: number; total: number | null } | null }

/**
 * The /estimates answer plus the AquaCrop provider's, once it has settled. While it runs the "async"
 * row stays and `progress` says how far; a failure, timeout or lost request replaces it with its
 * reason, so a missing result is never shown as an empty one.
 */
export function mergeAquacrop(data: EstimatesResponse, aq: AquacropResponse | null, failure?: AquacropFailure): MergedEstimates {
  if (!aquacropPending(data)) return { data, progress: null };
  const others = data.unavailable.filter((u) => !(u.provider_id === PROVIDER && u.reason === 'async'));
  if (failure) return { data: { ...data, unavailable: [...others, { provider_id: PROVIDER, reason: failure }] }, progress: null };
  if (!aq || aq.state === 'queued' || aq.state === 'running') {
    return { data, progress: { done: aq?.done ?? 0, total: aq?.total ?? null } };
  }
  const estimates = [...data.estimates, ...aq.estimates];
  return { data: { ...data, estimates, unavailable: [...others, ...aq.unavailable], disagreement: disagrees(estimates) }, progress: null };
}

/** The moisture caption, or null when the payload has none (a 1.0 backend during a staggered deploy). */
export function moistureLabel(m: Moisture | null | undefined): { key: string; params: Record<string, unknown> } | null {
  if (!m) return null;
  if (m.basis === 'commercial') {
    return m.pct == null ? { key: 'planning.estimate.moisture.commercialNoPct', params: {} }
      : { key: 'planning.estimate.moisture.commercial', params: { pct: m.pct } };
  }
  return { key: `planning.estimate.moisture.${m.basis}`, params: {} };
}

/** Progress text: "done/total" only once the total is a positive number (the job API sends null first). */
export function progressLabel(progress: { done: number; total: number | null }):
  { key: string; params: Record<string, unknown> } {
  return progress.total !== null && progress.total > 0
    ? { key: 'planning.estimate.simulating', params: { done: progress.done, total: progress.total } }
    : { key: 'planning.estimate.simulatingStart', params: {} };
}

/** Only the request that follows a user's retry carries retry=true; polling never does. */
export const retryFlag = (retryCount: number, attempt: number): boolean => retryCount > 0 && attempt === 0;

/** The button belongs to a failed engine, a busy queue and the two resumable states, never to a gate reason. */
export const canRetry = (u: { provider_id: string; reason: string }): boolean =>
  u.provider_id === PROVIDER && ['engine_error', 'busy', 'request_failed', 'still_queued'].includes(u.reason);

/** A 404/405 means the backend with this endpoint is not deployed yet; any other error is a failed request. */
export function failureFor(error: unknown): AquacropFailure {
  const status = (error as { status?: unknown } | null)?.status;
  return status === 404 || status === 405 ? 'engine_not_deployed' : 'request_failed';
}

export interface RetryState { inputs: string; retries: number; resumes: number }

/** A click: `retry` relaunches a failed job, `resume` only restarts polling. Other inputs start from zero. */
export function nextRetry(prev: RetryState, inputs: string, kind: 'retry' | 'resume'): RetryState {
  const base = prev.inputs === inputs ? prev : { inputs, retries: 0, resumes: 0 };
  return kind === 'retry' ? { ...base, retries: base.retries + 1 } : { ...base, resumes: base.resumes + 1 };
}

/** Retries that apply to these inputs: any input change resets them. */
export const retryCountFor = (state: RetryState, inputs: string): number => (state.inputs === inputs ? state.retries : 0);

/** Light polls: every request after the first carries the job id of the previous answer. */
export const jobIdForPoll = (lastJobId: string | null, attempt: number): string | null => (attempt === 0 ? null : lastJobId);

const KNOWN_EXPECTED = ['crop_not_calibrated', 'irrigated', 'outside_calibrated_regions', 'region_not_calibrated', 'missing', 'unknown']
  .map((r) => `expected_${r}`);

/** i18n key of an unavailable reason; a calibration reason this UI does not know reads as the generic one. */
export function unavailableKey(reason: string): string {
  const known = reason.startsWith('expected_') && !KNOWN_EXPECTED.includes(reason) ? 'expected_unknown' : reason;
  return `planning.estimate.unavailable.${known}`;
}

/**
 * The one-line expected yield (W3): the gap-corrected range when its model is validated, then trial
 * evidence P10–P90 (observed, not a model); never a ceiling, never an unvalidated model's figure.
 */
export function yieldSummary(estimates: Estimate[]): { low: number; high: number } | null {
  const gap = estimates.find((e) => e.basis === 'gap_corrected' && e.range && e.validation.status === 'validated');
  if (gap?.range) return { low: gap.range.low, high: gap.range.high };
  const trial = estimates.find((e) => e.basis === 'trial_evidence' && e.percentiles);
  return trial?.percentiles ? { low: trial.percentiles.p10, high: trial.percentiles.p90 } : null;
}
