import type { SourceAttributionItem } from '../types/attribution';
import type { Basis, Estimate } from './types';

export const BASIS_ORDER: Basis[] = ['trial_evidence', 'water_limited', 'gap_corrected', 'potential'];

export function sortEstimates(estimates: Estimate[]): Estimate[] {
  return [...estimates].sort((a, b) => BASIS_ORDER.indexOf(a.basis) - BASIS_ORDER.indexOf(b.basis));
}

export function validationTone(status: Estimate['validation']['status']): 'positive' | 'warning' | 'default' {
  if (status === 'validated') return 'positive';
  return status === 'experimental' ? 'warning' : 'default';
}

export function uniqueAttributions(estimates: Estimate[]): SourceAttributionItem[] {
  const seen = new Map<string, SourceAttributionItem>();
  for (const e of estimates) for (const a of e.attributions) if (!seen.has(a.source_id)) seen.set(a.source_id, a);
  return [...seen.values()];
}
