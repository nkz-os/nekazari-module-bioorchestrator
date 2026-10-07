import { describe, expect, it } from 'vitest';
import es from '../../locales/es.json';
import en from '../../locales/en.json';
import type { ParcelEnvironment, Recommendation } from '../../types/recommend';
import {
  DEFAULT_FILTERS, KOPPEN_CODES, MAX_COMPARE, conditionsQuery, evidenceConditions,
  formatAssumptionValue, knownText, parcelQuery, parseFrostMargin, rangeScaleMax,
  seasonKey, toggleCompare, isFewTrials, frostMarginStatus, irrigationOptions, pickIrrigation, PURPOSES,
  describeReferenceScope, type ScopeTranslate,
  DEFAULT_PURPOSE, carriesEvidencePolicy, effectiveFilters, nextPolicyAware,
} from './pageModel';
import type { RecommendResponse } from '../../types/recommend';

const env = (detail: Record<string, unknown> | null): ParcelEnvironment => ({
  parcel_id: 'p1', area_ha: null, centroid: { lat: null, lon: null }, climate_class: 'Csb',
  climate_detail: detail, soil: { data_available: false },
  irrigation: { inferred: null, source: 'none', overridable: true },
  campaign: { assigned: false }, inputs_used: {},
});

describe('KOPPEN_CODES', () => {
  it('lists exactly the brief codes in order', () => {
    expect(KOPPEN_CODES.join(' ')).toBe(
      'Af Am Aw BWh BWk BSh BSk Csa Csb Csc Cwa Cwb Cfa Cfb Cfc Dfa Dfb Dfc Dsa Dsb Dwa Dwb ET',
    );
  });
});

describe('parseFrostMargin', () => {
  it('empty or invalid means server default (undefined)', () => {
    expect(parseFrostMargin('')).toBeUndefined();
    expect(parseFrostMargin('  ')).toBeUndefined();
    expect(parseFrostMargin('abc')).toBeUndefined();
    expect(parseFrostMargin('-1')).toBeUndefined();
    expect(parseFrostMargin('15.5')).toBeUndefined();
  });
  it('accepts 0..15 including decimals and comma', () => {
    expect(parseFrostMargin('0')).toBe(0);
    expect(parseFrostMargin('15')).toBe(15);
    expect(parseFrostMargin('2,5')).toBe(2.5);
  });
});

describe('frostMarginStatus', () => {
  it('empty, valid or invalid', () => {
    expect(frostMarginStatus('')).toBe('empty');
    expect(frostMarginStatus('  ')).toBe('empty');
    expect(frostMarginStatus('3.5')).toBe('valid');
    expect(frostMarginStatus('16')).toBe('invalid');
    expect(frostMarginStatus('x')).toBe('invalid');
  });
});

describe('parcelQuery', () => {
  it('defaults: top_n 15, season/management, no irrigation, no climate, no frost', () => {
    expect(parcelQuery(DEFAULT_FILTERS, false)).toEqual({
      top_n: 15, season: 'all', management: 'any',
    });
  });
  it('maps irrigation override and climate override', () => {
    const q = parcelQuery({ ...DEFAULT_FILTERS, irrigation: 'regadío', climateClass: 'Csa' }, false);
    expect(q.irrigation_regime).toBe('regadío');
    expect(q.climate_class).toBe('Csa');
  });
  it('never sends an empty climate_class', () => {
    expect('climate_class' in parcelQuery({ ...DEFAULT_FILTERS, climateClass: '' }, false)).toBe(false);
  });
  it('frost margin only in expert mode and only when valid', () => {
    const f = { ...DEFAULT_FILTERS, frostMargin: '3' };
    expect(parcelQuery(f, false).frost_margin_c).toBeUndefined();
    expect(parcelQuery(f, true).frost_margin_c).toBe(3);
    expect('frost_margin_c' in parcelQuery({ ...f, frostMargin: '' }, true)).toBe(false);
  });
});

describe('purpose filter', () => {
  it('offers harvest (main) first and forage; harvest is the default', () => {
    expect([...PURPOSES]).toEqual(['main', 'forage']);
    expect(DEFAULT_FILTERS.purpose).toBe('main');
  });
  it('forage is sent to the API on both endpoints; the default is sent as nothing', () => {
    expect(parcelQuery({ ...DEFAULT_FILTERS, purpose: 'forage' }, false).purpose).toBe('forage');
    expect(conditionsQuery({ ...DEFAULT_FILTERS, climateClass: 'Csa', purpose: 'forage' }, false)?.purpose)
      .toBe('forage');
    // an older backend is asked exactly what it always was
    expect(DEFAULT_PURPOSE).toBe('main');
    expect('purpose' in parcelQuery(DEFAULT_FILTERS, true)).toBe(false);
    expect('purpose' in (conditionsQuery({ ...DEFAULT_FILTERS, climateClass: 'Csa' }, false) ?? {})).toBe(false);
  });
  it('is never sent empty', () => {
    const bad = { ...DEFAULT_FILTERS, purpose: '' as never };
    expect('purpose' in parcelQuery(bad, false)).toBe(false);
  });
});

describe('conditionsQuery', () => {
  it('is null without a climate class (endpoint requires one)', () => {
    expect(conditionsQuery(DEFAULT_FILTERS, false)).toBeNull();
  });
  it('carries the picked class plus filters', () => {
    expect(conditionsQuery({ ...DEFAULT_FILTERS, climateClass: 'BSk', season: 'spring' }, false))
      .toEqual({ top_n: 15, season: 'spring', management: 'any', climate_class: 'BSk' });
  });
});

describe('evidenceConditions', () => {
  const echo = {
    climate_class: 'Csa', soil_type: 'Calcisol', irrigation_regime: 'secano', management: 'any',
    season: 'all', crops: null, top_n: 15, annual_rainfall_mm: 400, annual_et0_mm: 1100,
    coldest_month_min_c: 1, annual_temp_c: 14,
  };
  it('koppen: class, soil type and irrigation only', () => {
    expect(evidenceConditions(echo, env(null), 'koppen')).toEqual({
      climate_class: 'Csa', soil_type: 'Calcisol', irrigation_regime: 'secano',
    });
  });
  it('v2: adds the four numeric inputs from parcel climate_detail', () => {
    const detail = { annual_rainfall_mm: 500, annual_et0_mm: 1200, coldest_month_min_c: -2,
                     annual_temp_c: 12, source: 'x' };
    expect(evidenceConditions(echo, env(detail), 'vector_v2_fallback')).toEqual({
      climate_class: 'Csa', soil_type: 'Calcisol', irrigation_regime: 'secano',
      annual_rainfall_mm: 500, annual_et0_mm: 1200, coldest_month_min_c: -2, annual_temp_c: 12,
    });
  });
  it('v2 without parcel detail falls back to the echoed numbers; non-numbers dropped', () => {
    const out = evidenceConditions({ ...echo, annual_temp_c: 'n/a' }, undefined, 'vector_v2_fallback');
    expect(out.annual_rainfall_mm).toBe(400);
    expect('annual_temp_c' in out).toBe(false);
  });
  it('carries the echoed purpose so the trials listed are the ones of the answer', () => {
    expect(evidenceConditions({ climate_class: 'Csa', purpose: 'forage' }, undefined, 'koppen'))
      .toEqual({ climate_class: 'Csa', purpose: 'forage' });
    expect(evidenceConditions({ climate_class: 'Csa', purpose: 'main' }, undefined, 'koppen').purpose).toBe('main');
  });
  it('drops an unknown or empty purpose', () => {
    expect('purpose' in evidenceConditions({ climate_class: 'Csa', purpose: '' }, undefined, 'koppen')).toBe(false);
    expect('purpose' in evidenceConditions({ climate_class: 'Csa', purpose: 'silage' }, undefined, 'koppen')).toBe(false);
  });
  it('sends the tier only for regional recommendations', () => {
    expect(evidenceConditions(echo, env(null), 'koppen', 'regional').tier).toBe('regional');
    expect('tier' in evidenceConditions(echo, env(null), 'koppen', 'field')).toBe(false);
    expect('tier' in evidenceConditions(echo, env(null), 'koppen')).toBe(false);
  });
  it('sends the echoed country only for regional recommendations, and only a valid ISO code', () => {
    const es = { ...echo, country: 'ES' };
    expect(evidenceConditions(es, env(null), 'koppen', 'regional').country).toBe('ES');
    expect('country' in evidenceConditions(es, env(null), 'koppen', 'field')).toBe(false);
    expect('country' in evidenceConditions({ ...echo, country: 'es' }, env(null), 'koppen', 'regional')).toBe(false);
    expect('country' in evidenceConditions({ ...echo, country: null }, env(null), 'koppen', 'regional')).toBe(false);
    const pt = evidenceConditions({ ...es, lat: 41, lon: -3 }, env(null), 'koppen', 'regional');
    expect([pt.lat, pt.lon]).toEqual([41, -3]);
    expect('lat' in evidenceConditions({ ...es, lat: 41 }, env(null), 'koppen', 'regional')).toBe(false);
    expect('lat' in evidenceConditions({ ...es, lat: 41, lon: -3 }, env(null), 'koppen', 'field')).toBe(false);
  });
  it('drops unknown irrigation values and empty strings', () => {
    const out = evidenceConditions({ climate_class: 'Csa', soil_type: '', irrigation_regime: 'drip' },
      undefined, 'koppen');
    expect(out).toEqual({ climate_class: 'Csa' });
  });
});

describe('toggleCompare', () => {
  it('adds, removes, and caps at MAX_COMPARE', () => {
    expect(MAX_COMPARE).toBe(4);
    expect(toggleCompare([], 'a')).toEqual(['a']);
    expect(toggleCompare(['a', 'b'], 'a')).toEqual(['b']);
    expect(toggleCompare(['a', 'b', 'c', 'd'], 'e')).toEqual(['a', 'b', 'c', 'd']);
  });
});

describe('small formatters', () => {
  it('seasonKey maps sowing_type, null to unknown', () => {
    expect(seasonKey('autumn')).toBe('whatToSow.season.autumn');
    expect(seasonKey(null)).toBe('whatToSow.season.unknown');
  });
  it('knownText treats null, empty and "unknown" as missing', () => {
    expect(knownText(null)).toBeNull();
    expect(knownText('')).toBeNull();
    expect(knownText('unknown')).toBeNull();
    expect(knownText('loam')).toBe('loam');
  });
  it('isFewTrials below 5', () => {
    expect(isFewTrials(4)).toBe(true);
    expect(isFewTrials(5)).toBe(false);
  });
  it('formatAssumptionValue renders scalars and objects', () => {
    expect(formatAssumptionValue(0.8)).toBe('0.8');
    expect(formatAssumptionValue({ a: 1 })).toBe('{"a":1}');
    expect(formatAssumptionValue(null)).toBeNull();
  });
  it('rangeScaleMax is the largest known interval high or expected; null when none', () => {
    const r = (hi: number | null, exp: number | null) =>
      ({ yield: { interval: [null, hi], expected_kg_ha: exp } }) as unknown as Recommendation;
    expect(rangeScaleMax([r(1200, 1000), r(null, 3000), r(2500, 2000)])).toBe(3000);
    expect(rangeScaleMax([r(null, null)])).toBeNull();
  });
});

describe('irrigation options', () => {
  it('offers the detected value only with a parcel', () => {
    expect(irrigationOptions(true)).toEqual(['inferred', 'secano', 'regadío']);
    expect(irrigationOptions(false)).toEqual(['secano', 'regadío']);
  });
  it('with a parcel, picking always sets the value', () => {
    expect(pickIrrigation('secano', 'secano', true)).toBe('secano');
    expect(pickIrrigation('secano', 'inferred', true)).toBe('inferred');
  });
  it('in explore mode, picking the active value clears it (no irrigation filter sent)', () => {
    expect(pickIrrigation('secano', 'secano', false)).toBe('inferred');
    expect(pickIrrigation('inferred', 'regadío', false)).toBe('regadío');
    expect(conditionsQuery({ ...DEFAULT_FILTERS, climateClass: 'Csa',
      irrigation: pickIrrigation('secano', 'secano', false) }, false)?.irrigation_regime).toBeUndefined();
  });
});

/** Translator over a real locale file: checks the sentence the farmer reads, not just the keys. */
function translator(dict: unknown): ScopeTranslate {
  return (key, opts) => {
    const node = key.split('.').reduce<unknown>(
      (n, part) => (n && typeof n === 'object' ? (n as Record<string, unknown>)[part] : undefined), dict);
    if (typeof node !== 'string') throw new Error(`missing key ${key}`);
    return node.replace(/\{\{(\w+)\}\}/g, (_m, k: string) => String(opts?.[k] ?? ''));
  };
}

describe('describeReferenceScope', () => {
  const tes = translator({ whatToSow: (es as { whatToSow: unknown }).whatToSow });
  const ten = translator({ whatToSow: (en as { whatToSow: unknown }).whatToSow });

  it('analog sites of a climate class and an irrigation regime', () => {
    expect(describeReferenceScope('analog_sites:Csa:secano', tes)).toBe('sitios análogos Csa · secano');
    expect(describeReferenceScope('analog_sites:Csa:regadio', tes)).toBe('sitios análogos Csa · regadío');
    expect(describeReferenceScope('analog_sites:Csa:regadío', tes)).toBe('sitios análogos Csa · regadío');
    expect(describeReferenceScope('analog_sites:Csa:secano', ten)).toBe('analog sites Csa · rainfed');
  });
  it('no regime split is said, not hidden', () => {
    expect(describeReferenceScope('analog_sites:Dfb:any', tes)).toBe('sitios análogos Dfb · cualquier riego');
    expect(describeReferenceScope('analog_sites:Dfb:any', ten)).toBe('analog sites Dfb · any irrigation');
  });
  it('sites picked by climate similarity, and without a climate class', () => {
    expect(describeReferenceScope('analog_sites:vector_v2:any', tes))
      .toBe('sitios análogos por parecido climático · cualquier riego');
    expect(describeReferenceScope('analog_sites:any:secano', tes)).toBe('sitios análogos · secano');
  });
  it('forage mode adds the purpose', () => {
    expect(describeReferenceScope('analog_sites:Cfb:secano:forage', tes)).toBe('sitios análogos Cfb · secano · forraje');
    expect(describeReferenceScope('analog_sites:Cfb:secano:forage', ten)).toBe('analog sites Cfb · rainfed · forage');
  });
  it('regional recommendations have a regional scope', () => {
    expect(describeReferenceScope('regional', tes)).toBe('registros regionales/nacionales');
    expect(describeReferenceScope('regional', ten)).toBe('regional/national records');
  });
  it('returns an unrecognised scope as is (never invented)', () => {
    for (const raw of ['crop', '', 'analog_sites:Csa', 'analog_sites:Csa:wet', 'analog_sites:Csa:secano:silage',
      'analog_sites::secano', 'analog_sites:Csa:secano:forage:x']) {
      expect(describeReferenceScope(raw, tes)).toBe(raw);
    }
  });
});

// Deploy order: the frontend can be published before the backend that applies the evidence policy.
describe('evidence-policy awareness (old backend renders as before)', () => {
  const ok = (extra: Record<string, unknown> = {}) => ({
    status: 'ok', evidence_policy: '2026-10-05.1', recommendations: [], data_quality: {}, conditions: {}, ...extra,
  }) as unknown as RecommendResponse;
  const legacyOk = () => {
    const { evidence_policy: _omit, ...rest } = ok() as unknown as Record<string, unknown>;
    return rest as unknown as RecommendResponse;
  };
  const needsClimate = { status: 'needs_climate', parcel_environment: env(null) } as RecommendResponse;

  it('an answer carries the policy only when the backend states its version', () => {
    expect(carriesEvidencePolicy(ok())).toBe(true);
    expect(carriesEvidencePolicy(legacyOk())).toBe(false);
    expect(carriesEvidencePolicy(ok({ evidence_policy: '' }))).toBe(false);
    expect(carriesEvidencePolicy(ok({ evidence_policy: null }))).toBe(false);
    expect(carriesEvidencePolicy(needsClimate)).toBe(false);
    expect(carriesEvidencePolicy(null)).toBe(false);
    expect(carriesEvidencePolicy(undefined)).toBe(false);
  });
  it('only an ok answer updates the observed awareness', () => {
    expect(nextPolicyAware(false, ok())).toBe(true);
    expect(nextPolicyAware(true, legacyOk())).toBe(false);   // backend rolled back
    expect(nextPolicyAware(true, needsClimate)).toBe(true);  // cannot tell: keep
    expect(nextPolicyAware(false, needsClimate)).toBe(false);
  });
  it('against a backend not known to apply the policy there is only the default Destino', () => {
    const forage = { ...DEFAULT_FILTERS, purpose: 'forage' as const, climateClass: 'Csa' };
    expect(effectiveFilters(forage, false).purpose).toBe('main');
    expect('purpose' in (conditionsQuery(effectiveFilters(forage, false), false) ?? {})).toBe(false);
    expect(effectiveFilters(forage, true)).toBe(forage);
    expect(conditionsQuery(effectiveFilters(forage, true), false)?.purpose).toBe('forage');
    expect(effectiveFilters(DEFAULT_FILTERS, false)).toBe(DEFAULT_FILTERS);
  });
});
