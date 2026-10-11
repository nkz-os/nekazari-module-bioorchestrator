import type { SourceAttributionItem } from '../types/attribution';
import type { Basis, Estimate } from './types';

export const BASIS_ORDER: Basis[] = ['trial_evidence', 'water_limited', 'gap_corrected', 'potential'];

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
