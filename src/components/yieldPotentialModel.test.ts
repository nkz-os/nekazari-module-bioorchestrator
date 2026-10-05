import { describe, expect, it } from 'vitest';
import type { YieldPotentialResponse } from '../services/api';
import { yieldPotentialView } from './yieldPotentialModel';

const base: YieldPotentialResponse = {
  variety: 'V', crop: 'TRZAX', target_environment: {}, expected_yield_kg_ha: 7000,
  confidence_interval: [6000, 8000], trials_analyzed: 2, similar_sites: [], stage_ky: {},
};

describe('yieldPotentialView', () => {
  it('no answer: nothing', () => {
    expect(yieldPotentialView(null)).toBe('none');
    expect(yieldPotentialView(undefined)).toBe('none');
  });
  it('a null expected yield is "no data", never a zero gap', () => {
    const noData = { ...base, expected_yield_kg_ha: null, confidence_interval: null, trials_analyzed: 0,
      data_gaps: ['no_measured_yield'] };
    expect(yieldPotentialView(noData)).toBe('no_data');
    expect(yieldPotentialView({ ...noData, yield_gap_pct: 0 })).toBe('no_data');
  });
  it('a number with a current estimate shows the gap; without one, nothing', () => {
    expect(yieldPotentialView({ ...base, yield_gap_pct: 12.5, yield_gap_kg_ha: 875 })).toBe('gap');
    expect(yieldPotentialView({ ...base, yield_gap_pct: 0, yield_gap_kg_ha: 0 })).toBe('gap');
    expect(yieldPotentialView(base)).toBe('none');
    expect(yieldPotentialView({ ...base, yield_gap_pct: null })).toBe('none');
  });
});
