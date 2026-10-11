import { describe, expect, it } from 'vitest';
import { sortEstimates, uniqueAttributions, validationTone } from './estimatesModel';
import type { Estimate } from './types';

const est = (basis: Estimate['basis'], source = 'GENVCE'): Estimate => ({
  provider_id: basis, provider_version: 'v', basis, unit: 't/ha', percentiles: { p10: 1, p50: 2, p90: 3 }, n: 1,
  n_kind: 'trials', validation: { status: 'not_applicable', metric: null, scope: null }, warnings: [], graph_id: 'global',
  attributions: [{ source_id: source, text: source, url: '', licence_id: '', licence_url: '' }],
});

describe('estimates model', () => {
  it('orders trials first, then water-limited, then gap-corrected', () => {
    expect(sortEstimates([est('gap_corrected'), est('trial_evidence'), est('water_limited')]).map((e) => e.basis))
      .toEqual(['trial_evidence', 'water_limited', 'gap_corrected']);
  });
  it('attributions once per source; experimental is a warning', () => {
    expect(uniqueAttributions([est('trial_evidence'), est('water_limited')]).length).toBe(1);
    expect(validationTone('experimental')).toBe('warning');
  });
});
