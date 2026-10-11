import { describe, expect, it } from 'vitest';
import { groupFit, reasonKey, reasonTone } from './fitModel';
import type { SpeciesFit } from './types';

const sp = (species: string, verdict: SpeciesFit['verdict'], es: string): SpeciesFit => ({
  species, eppo: null, names: { es }, lifecycle: 'annual', verdict, reasons: [], has_variety_trials: false, estimate_providers: [],
  sowing: { sowing_type: null, start_month: null, end_month: null, typical_sowing_doy: null, source: null },
});

describe('fit model', () => {
  it('groups by verdict and sorts by localized name, never by yield', () => {
    const g = groupFit([sp('wheat', 'fits', 'trigo'), sp('barley', 'fits', 'cebada'), sp('rapeseed', 'no_fit', 'colza')], 'es');
    expect(g.fits.map((s) => s.species)).toEqual(['barley', 'wheat']);
    expect(g.no_fit.map((s) => s.species)).toEqual(['rapeseed']);
  });
  it('keys carry the why of unknowns and tones mark blockers', () => {
    expect(reasonKey({ code: 'soil_unknown', gate: 'soil', params: { why: 'no_parcel_soil' } })).toBe('planning.reason.soil_unknown_no_parcel_soil');
    expect(reasonTone({ code: 'frost_risk', gate: 'frost', params: {} })).toBe('negative');
    expect(reasonTone({ code: 'rotation_benefit', gate: 'rotation', params: {} })).toBe('positive');
    expect(reasonTone({ code: 'regime_missing', gate: 'data', params: {} })).toBe('warning');
    expect(reasonTone({ code: 'multi_year', gate: 'data', params: {} })).toBe('info');
  });
});
