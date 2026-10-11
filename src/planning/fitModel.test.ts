import { describe, expect, it } from 'vitest';
import { groupFit, reasonKey, reasonText, reasonTone, splitSharedReasons } from './fitModel';
import type { Reason, SpeciesFit, TFn } from './types';

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

describe('splitSharedReasons', () => {
  const regime: Reason = { code: 'regime_missing', gate: 'data', params: {} };
  const noSoil: Reason = { code: 'soil_unknown', gate: 'soil', params: { why: 'no_parcel_soil' } };
  const frost: Reason = { code: 'frost_risk', gate: 'frost', params: { coldest_min_c: -12, threshold_c: -10, margin_c: 5 } };
  const withReasons = (species: string, reasons: Reason[]): SpeciesFit => ({ ...sp(species, 'fits', species), reasons });

  it('a reason carried by every crop is shared and leaves each crop\'s own list', () => {
    const { shared, perSpecies } = splitSharedReasons([
      withReasons('wheat', [regime, noSoil]), withReasons('barley', [regime, frost, noSoil]), withReasons('oats', [noSoil, regime]),
    ]);
    expect(shared).toEqual([regime, noSoil]);
    expect(perSpecies.get('wheat')).toEqual([]);
    expect(perSpecies.get('barley')).toEqual([frost]);
    expect(perSpecies.get('oats')).toEqual([]);
  });
  it('the same code with other params is a different reason, so it stays per crop', () => {
    const benefit = (previous: string): Reason => ({ code: 'rotation_benefit', gate: 'rotation', params: { previous } });
    const { shared, perSpecies } = splitSharedReasons([withReasons('wheat', [benefit('a')]), withReasons('barley', [benefit('b')])]);
    expect(shared).toEqual([]);
    expect(perSpecies.get('barley')).toEqual([benefit('b')]);
  });
  it('params are compared by value, whatever their key order', () => {
    const a: Reason = { code: 'rotation_restriction', gate: 'rotation', params: { previous: 'x', allowed_from: 2030 } };
    const b: Reason = { code: 'rotation_restriction', gate: 'rotation', params: { allowed_from: 2030, previous: 'x' } };
    expect(splitSharedReasons([withReasons('wheat', [a]), withReasons('barley', [b])]).shared).toHaveLength(1);
  });
  it('a reason missing from one crop is not shared', () => {
    const { shared, perSpecies } = splitSharedReasons([withReasons('wheat', [regime]), withReasons('barley', [])]);
    expect(shared).toEqual([]);
    expect(perSpecies.get('wheat')).toEqual([regime]);
  });
  it('needs at least two crops: a lone crop keeps all its reasons', () => {
    const { shared, perSpecies } = splitSharedReasons([withReasons('wheat', [regime])]);
    expect(shared).toEqual([]);
    expect(perSpecies.get('wheat')).toEqual([regime]);
    expect(splitSharedReasons([])).toEqual({ shared: [], perSpecies: new Map() });
  });
  it('a shared reason listed twice in a crop is removed from it and reported once', () => {
    const { shared, perSpecies } = splitSharedReasons([withReasons('wheat', [regime, regime, frost]), withReasons('barley', [regime])]);
    expect(shared).toEqual([regime]);
    expect(perSpecies.get('wheat')).toEqual([frost]);
  });
});
