import { describe, expect, it } from 'vitest';
import { groupFit, reasonKey, reasonText, reasonTone } from './fitModel';
import type { SpeciesFit, TFn } from './types';

// Minimal i18n stand-in: interpolates {{x}} and honours defaultValue for missing keys, like i18next.
const TEXTS: Record<string, string> = {
  'planning.texture.sandy-loam': 'franco-arenosa',
  'planning.reason.soil_texture_unsuitable': 'Textura {{value}}: no apta',
  'planning.reason.soil_drainage_unsuitable': 'Drenaje {{value}} no apto',
};
const fakeT: TFn = (key, opts = {}) => {
  const raw = TEXTS[key] ?? (typeof opts.defaultValue === 'string' ? opts.defaultValue : key);
  return raw.replace(/\{\{(\w+)\}\}/g, (_m, name: string) => String(opts[name]));
};

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
  it('an unknown verdict lands in caveats, never in fits, and does not throw', () => {
    const odd = { ...sp('odd', 'fits', 'raro'), verdict: 'bogus' as unknown as SpeciesFit['verdict'] };
    const g = groupFit([sp('wheat', 'fits', 'trigo'), odd], 'es');
    expect(g.fits.map((s) => s.species)).toEqual(['wheat']);
    expect(g.caveats.map((s) => s.species)).toEqual(['odd']);
  });
  it('sorting does not throw on a malformed language tag', () => {
    expect(groupFit([sp('wheat', 'fits', 'trigo'), sp('barley', 'fits', 'cebada')], 'en_US').fits.map((s) => s.species))
      .toEqual(['barley', 'wheat']);
  });
  it('keys carry the why of unknowns and tones mark blockers', () => {
    expect(reasonKey({ code: 'soil_unknown', gate: 'soil', params: { why: 'no_parcel_soil' } })).toBe('planning.reason.soil_unknown_no_parcel_soil');
    expect(reasonTone({ code: 'frost_risk', gate: 'frost', params: {} })).toBe('negative');
    expect(reasonTone({ code: 'rotation_benefit', gate: 'rotation', params: {} })).toBe('positive');
    expect(reasonTone({ code: 'regime_missing', gate: 'data', params: {} })).toBe('warning');
    expect(reasonTone({ code: 'multi_year', gate: 'data', params: {} })).toBe('info');
  });
  it('translates soil texture values, falls back to the raw value, and leaves drainage values alone', () => {
    const texture = (value: string) => ({ code: 'soil_texture_unsuitable', gate: 'soil' as const, params: { value } });
    expect(reasonText(texture('sandy-loam'), {}, 'es', fakeT)).toBe('Textura franco-arenosa: no apta');
    expect(reasonText(texture('mystery-soil'), {}, 'es', fakeT)).toBe('Textura mystery-soil: no apta');
    const drainage = { code: 'soil_drainage_unsuitable', gate: 'soil' as const, params: { value: 'sandy-loam' } };
    expect(reasonText(drainage, {}, 'es', fakeT)).toBe('Drenaje sandy-loam no apto');
  });
});
