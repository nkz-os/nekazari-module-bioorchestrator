import { describe, expect, it } from 'vitest';
import type { Recommendation, RecommendResponse } from '../../types/recommend';
import {
  compareRows, forageNoticeCount, formatYield, grossIncome, kgUnitKey, lensAvailability, levelKey,
  partitionRecommendations, rangeBar, recYieldUnit, recsYieldUnit, resolvePageState, soilWarning, unitKey,
  yieldStatus, yieldUnit, yieldValue,
} from './viewModel';

function rec(eppo: string, over: {
  n?: number; exp?: number | null; rel?: number | null; cv?: number | null;
  water?: Recommendation['suitability']['water']['level'];
  soil?: Recommendation['suitability']['soil']['level'];
  frost?: Recommendation['suitability']['frost']['level'];
  trust?: Recommendation['trust']['level']; window?: boolean;
} = {}): Recommendation {
  return {
    recommendation_id: eppo,
    crop: { eppo, scientific_name: eppo, sowing_type: 'autumn' },
    fit: { relative_yield_pct: over.rel ?? null, stability_cv: over.cv ?? null,
           reference: { median_kg_ha: null, n_trials: 0, scope: 'crop' } },
    yield: { expected_kg_ha: over.exp === undefined ? 1000 : over.exp, interval: [800, 1200],
             interval_method: 'observed_range', basis: null, sd: null, n_trials: over.n ?? 5, n_sites: 2 },
    suitability: { soil: { level: over.soil ?? 'suitable', warnings: [] },
                   water: { level: over.water ?? 'low', etc_mm: null },
                   frost: { level: over.frost ?? 'none' } },
    season: { sowing_window: over.window ? { start_month: 10, end_month: 11 } : null,
              cycle_days: null, source: null },
    trust: { level: over.trust ?? 'medium', data_gaps: [], similarity: 'koppen' },
    varieties: [], evidence: { trial_count: 0, sources: [], sites: [], years: null, tier: 'field', purpose: 'main', regional_trial_count: null, other_purpose_trials: {}, unknown_basis_trials: null },
    assumptions: [],
  };
}

describe('partitionRecommendations', () => {
  it('takes the first 3 well-backed recs in API order, rest to more', () => {
    const recs = ['A', 'B', 'C', 'D', 'E'].map((e) => rec(e));
    const { top, more } = partitionRecommendations(recs);
    expect(top.map((r) => r.crop.eppo)).toEqual(['A', 'B', 'C']);
    expect(more.map((r) => r.crop.eppo)).toEqual(['D', 'E']);
  });
  it('shows what exists when fewer than 3 (1-2 crops), more empty', () => {
    const { top, more } = partitionRecommendations([rec('A'), rec('B')]);
    expect(top).toHaveLength(2);
    expect(more).toEqual([]);
  });
  it('moves low-trial recs to more while keeping API order', () => {
    const { top, more } = partitionRecommendations([rec('A', { n: 2 }), rec('B'), rec('C', { n: 1 }), rec('D')]);
    expect(top.map((r) => r.crop.eppo)).toEqual(['B', 'D']);
    expect(more.map((r) => r.crop.eppo)).toEqual(['A', 'C']);
  });
  it('puts everything in more when all have n_trials < 3', () => {
    const { top, more } = partitionRecommendations([rec('A', { n: 2 }), rec('B', { n: 0 })]);
    expect(top).toEqual([]);
    expect(more).toHaveLength(2);
  });
  it('handles an empty list', () => {
    expect(partitionRecommendations([])).toEqual({ top: [], more: [] });
  });
});

describe('rangeBar', () => {
  it('computes percentages', () => {
    expect(rangeBar([200, 600], 400, 1000)).toEqual({ leftPct: 20, widthPct: 40, markerPct: 40 });
  });
  it('clamps to 0..100', () => {
    expect(rangeBar([-100, 2000], 5000, 1000)).toEqual({ leftPct: 0, widthPct: 100, markerPct: 100 });
  });
  it('is null-safe', () => {
    expect(rangeBar([null, 600], 400, 1000)).toBeNull();
    expect(rangeBar([200, null], 400, 1000)).toBeNull();
    expect(rangeBar([200, 600], null, 1000)).toBeNull();
    expect(rangeBar(null, 400, 1000)).toBeNull();
    expect(rangeBar([200, 600], 400, null)).toBeNull();
    expect(rangeBar([200, 600], 400, 0)).toBeNull();
  });
  it('keeps a zero value (not treated as missing)', () => {
    expect(rangeBar([0, 500], 0, 1000)).toEqual({ leftPct: 0, widthPct: 50, markerPct: 0 });
  });
});

describe('levelKey', () => {
  it('maps intents', () => {
    expect(levelKey('water', 'low')).toEqual({ key: 'whatToSow.level.water.low', intent: 'positive' });
    expect(levelKey('water', 'medium').intent).toBe('warning');
    expect(levelKey('water', 'high').intent).toBe('negative');
    expect(levelKey('soil', 'suitable').intent).toBe('positive');
    expect(levelKey('soil', 'marginal').intent).toBe('warning');
    expect(levelKey('soil', 'unsuitable').intent).toBe('negative');
    expect(levelKey('frost', 'none').intent).toBe('positive');
    expect(levelKey('frost', 'risk').intent).toBe('negative');
  });
  it('falls back to unknown/default', () => {
    expect(levelKey('soil', 'unknown')).toEqual({ key: 'whatToSow.level.soil.unknown', intent: 'default' });
    expect(levelKey('frost', null).key).toBe('whatToSow.level.frost.unknown');
    expect(levelKey('water', 'bogus' as never).intent).toBe('default');
  });
});

describe('compareRows all-tie', () => {
  it('has no best column when every column ties, but keeps a lone winner', () => {
    const rows = compareRows([rec('A'), rec('B')]);
    expect(rows.find((r) => r.id === 'expectedYield')!.best).toEqual([]);
    const lone = compareRows([rec('A', { exp: 900 }), rec('B', { exp: 1200 })]);
    expect(lone.find((r) => r.id === 'expectedYield')!.best).toEqual([1]);
  });
});

describe('compareRows', () => {
  const row = (rows: ReturnType<typeof compareRows>, id: string) => rows.find((r) => r.id === id)!;
  it('emits the 7 rows in order', () => {
    expect(compareRows([rec('A')]).map((r) => r.id)).toEqual(
      ['expectedYield', 'relativeYield', 'stability', 'water', 'soil', 'frost', 'trust']);
  });
  it('picks best per row; lower CV wins stability', () => {
    const rows = compareRows([
      rec('A', { exp: 900, rel: 5, cv: 0.3, water: 'high', soil: 'marginal', frost: 'risk', trust: 'low' }),
      rec('B', { exp: 1500, rel: -2, cv: 0.1, water: 'low', soil: 'suitable', frost: 'none', trust: 'high' }),
    ]);
    expect(row(rows, 'expectedYield').best).toEqual([1]);
    expect(row(rows, 'relativeYield').best).toEqual([0]);
    expect(row(rows, 'stability').best).toEqual([1]);
    expect(row(rows, 'water').best).toEqual([1]);
    expect(row(rows, 'soil').best).toEqual([1]);
    expect(row(rows, 'frost').best).toEqual([1]);
    expect(row(rows, 'trust').best).toEqual([1]);
    expect(row(rows, 'relativeYield').cells).toEqual(['+5', '-2']);
  });
  it('marks tied winners, but no star when all columns tie', () => {
    const rows = compareRows([rec('A', { exp: 1000 }), rec('B', { exp: 1000 }), rec('C', { exp: 500 })]);
    expect(row(rows, 'expectedYield').best).toEqual([0, 1]);
    expect(row(rows, 'water').best).toEqual([]);
  });
  it('ignores nulls and unknown; leaves best empty when all null', () => {
    const rows = compareRows([
      rec('A', { exp: null, water: 'unknown', cv: null }),
      rec('B', { exp: 700, water: 'medium', cv: 0.2 }),
    ]);
    expect(row(rows, 'expectedYield').best).toEqual([1]);
    expect(row(rows, 'expectedYield').cells).toEqual([null, '700']);
    expect(row(rows, 'water').cells).toEqual([null, 'medium']);
    expect(row(rows, 'water').best).toEqual([1]);
    expect(row(rows, 'stability').best).toEqual([1]);
    const allNull = compareRows([rec('A', { exp: null }), rec('B', { exp: null })]);
    expect(row(allNull, 'expectedYield').best).toEqual([]);
  });
  it('keeps a 0 value as a real value', () => {
    const rows = compareRows([rec('A', { rel: 0 }), rec('B', { rel: null })]);
    expect(row(rows, 'relativeYield').cells).toEqual(['0', null]);
    expect(row(rows, 'relativeYield').best).toEqual([0]);
  });
});

describe('grossIncome', () => {
  it('converts kg/ha at EUR/t', () => {
    expect(grossIncome(4000, [3000, 5000], 200)).toEqual({ value: 800, low: 600, high: 1000 });
  });
  it('is null without a price', () => {
    expect(grossIncome(4000, [3000, 5000], undefined)).toBeNull();
    expect(grossIncome(4000, [3000, 5000], null)).toBeNull();
    expect(grossIncome(4000, [3000, 5000], 0)).toBeNull();
  });
  it('keeps null parts null', () => {
    expect(grossIncome(null, [null, 5000], 100)).toEqual({ value: null, low: null, high: 500 });
  });
});

describe('lensAvailability', () => {
  it('reflects windows and prices', () => {
    expect(lensAvailability([rec('A')], {})).toEqual({ fit: true, calendar: false, rotation: true, euros: false });
    expect(lensAvailability([rec('A', { window: true })], { A: 150 }))
      .toEqual({ fit: true, calendar: true, rotation: true, euros: true });
    expect(lensAvailability([rec('A')], { B: 150 }).euros).toBe(false);
    const typical = rec('A');
    typical.season = { ...typical.season, typical_sowing_doy: 304, typical_maturity_doy: 190 };
    expect(lensAvailability([typical], {}).calendar).toBe(true);
    expect(lensAvailability([], {})).toEqual({ fit: true, calendar: false, rotation: true, euros: false });
  });
});

describe('resolvePageState', () => {
  const env = { parcel_id: 'p', area_ha: null, centroid: { lat: null, lon: null }, climate_class: null,
    climate_detail: null, soil: { data_available: false }, irrigation: { inferred: null, source: 'unknown', overridable: true },
    campaign: { assigned: false }, inputs_used: {} };
  const ok = (n: number): RecommendResponse => ({ status: 'ok', evidence_policy: 'test',
    recommendations: Array.from({ length: n }, (_, i) => rec(`C${i}`)), data_quality: {}, conditions: {} });
  it('covers every state', () => {
    expect(resolvePageState(true, null, null)).toBe('loading');
    expect(resolvePageState(false, new Error('500'), null)).toBe('error');
    expect(resolvePageState(false, null, { status: 'needs_climate', parcel_environment: env })).toBe('needs_climate');
    expect(resolvePageState(false, null, ok(0))).toBe('empty');
    expect(resolvePageState(false, null, ok(2))).toBe('ok');
  });
  it('never reports loading forever on error with a stale response', () => {
    expect(resolvePageState(false, new Error('x'), ok(2))).toBe('error');
  });
});

describe('soilWarning', () => {
  const withSoil = (level: Recommendation['suitability']['soil']['level'], warnings: string[]) => {
    const r = rec('A', { soil: level });
    r.suitability.soil.warnings = warnings;
    return r;
  };
  it('shows the first warning when soil is marginal or unsuitable', () => {
    expect(soilWarning(withSoil('marginal', ['pH low', 'x']))).toBe('pH low');
    expect(soilWarning(withSoil('unsuitable', ['too sandy']))).toBe('too sandy');
  });
  it('hides warnings for unknown soil (the badge already says no data)', () => {
    expect(soilWarning(withSoil('unknown', ['Parcel soil unavailable']))).toBeNull();
  });
  it('hides warnings for suitable soil and when there are none', () => {
    expect(soilWarning(withSoil('suitable', ['note']))).toBeNull();
    expect(soilWarning(withSoil('marginal', []))).toBeNull();
  });
});

describe('forageNoticeCount', () => {
  const withEvidence = (over: Partial<Recommendation['evidence']>) => {
    const r = rec('A');
    r.evidence = { ...r.evidence, ...over };
    return r;
  };
  it('is the forage trial count of a field recommendation in harvest mode', () => {
    expect(forageNoticeCount(withEvidence({ other_purpose_trials: { forage: 115 } }))).toBe(115);
    expect(forageNoticeCount(withEvidence({ other_purpose_trials: { forage: 1 } }))).toBe(1);
  });
  it('is null when there are none (missing, empty, zero)', () => {
    expect(forageNoticeCount(rec('A'))).toBeNull();
    expect(forageNoticeCount(withEvidence({ other_purpose_trials: { forage: 0 } }))).toBeNull();
    expect(forageNoticeCount(withEvidence({ other_purpose_trials: {} }))).toBeNull();
    expect(forageNoticeCount(withEvidence({ other_purpose_trials: undefined as never }))).toBeNull();
  });
  it('is null in forage mode and on regional recommendations', () => {
    expect(forageNoticeCount(withEvidence({ purpose: 'forage', other_purpose_trials: { forage: 4 } }))).toBeNull();
    expect(forageNoticeCount(withEvidence({ tier: 'regional', other_purpose_trials: { forage: 4 } }))).toBeNull();
  });
});

/** A forage-mode recommendation as the API answers it. */
function forageRec(eppo: string, over: {
  exp?: number | null; basis?: Recommendation['yield']['basis']; gaps?: string[]; n?: number;
} = {}): Recommendation {
  const r = rec(eppo, { exp: over.exp === undefined ? 30106 : over.exp, n: over.n });
  r.yield.basis = over.basis === undefined ? 'dry_matter' : over.basis;
  r.trust.data_gaps = over.gaps ?? [];
  r.evidence = { ...r.evidence, purpose: 'forage', unknown_basis_trials: 0 };
  return r;
}

describe('yield unit and basis label', () => {
  it('plain kg/ha without a basis; forage per its basis', () => {
    expect(yieldUnit(null)).toBe('kg_ha');
    expect(yieldUnit(undefined)).toBe('kg_ha');
    expect(yieldUnit('dry_matter')).toBe('dry_matter');
    expect(yieldUnit('fresh_matter')).toBe('fresh_matter');
  });
  it('label keys: tonnes for the displayed unit, kg for tables and dialogs', () => {
    expect(unitKey('kg_ha')).toBe('whatToSow.unit.kg_ha');
    expect(unitKey('dry_matter')).toBe('whatToSow.unit.dry_matter');
    expect(kgUnitKey('kg_ha')).toBe('whatToSow.unit.kg_ha');
    expect(kgUnitKey('dry_matter')).toBe('whatToSow.unit.kg_dry_matter');
    expect(kgUnitKey('fresh_matter')).toBe('whatToSow.unit.kg_fresh_matter');
  });
  it('harvest-mode recommendations stay in kg/ha', () => {
    expect(recYieldUnit(rec('A'))).toBe('kg_ha');
  });
  it('forage recommendations use their basis', () => {
    expect(recYieldUnit(forageRec('A'))).toBe('dry_matter');
    expect(recYieldUnit(forageRec('A', { basis: 'fresh_matter' }))).toBe('fresh_matter');
  });
  it('a forage answer without a basis holds dry-matter-normalised values only', () => {
    expect(recYieldUnit(forageRec('A', { exp: null, basis: null }))).toBe('dry_matter');
  });
  it('a set takes the unit of the first recommendation that has a basis', () => {
    expect(recsYieldUnit([])).toBe('kg_ha');
    expect(recsYieldUnit([rec('A'), rec('B')])).toBe('kg_ha');
    expect(recsYieldUnit([forageRec('A', { exp: null, basis: null }), forageRec('B')])).toBe('dry_matter');
  });
  it('shows forage in tonnes, kg/ha as whole kg, null stays null', () => {
    expect(yieldValue(30106, 'dry_matter')).toBeCloseTo(30.106);
    expect(yieldValue(3973, 'kg_ha')).toBe(3973);
    expect(yieldValue(null, 'dry_matter')).toBeNull();
    expect(yieldValue(Number.NaN, 'kg_ha')).toBeNull();
    expect(formatYield(30106, 'dry_matter', 'en')).toBe('30.1');
    expect(formatYield(3973.4, 'kg_ha', 'en')).toBe('3,973');
    expect(formatYield(350, 'dry_matter', 'en', 2)).toBe('0.35');
    expect(formatYield(null, 'dry_matter', 'en')).toBeNull();
  });
  it('0 is a value, not missing', () => {
    expect(formatYield(0, 'kg_ha', 'en')).toBe('0');
  });
});

describe('yieldStatus', () => {
  it('measured when there is a number', () => {
    expect(yieldStatus(rec('A'))).toBe('measured');
    expect(yieldStatus(forageRec('A'))).toBe('measured');
  });
  it('forage with trials of unknown basis: not comparable (never a number)', () => {
    expect(yieldStatus(forageRec('A', { exp: null, basis: null, gaps: ['forage_basis_unknown'], n: 4 })))
      .toBe('not_comparable');
  });
  it('presence-only recommendation: no measured yield', () => {
    expect(yieldStatus(rec('A', { exp: null }))).toBe('none');
    const r = rec('A', { exp: null });
    r.trust.data_gaps = ['no_measured_yield', 'regional_evidence_only', 'no_expected_yield'];
    expect(yieldStatus(r)).toBe('no_measured');
  });
  it('a number wins over a stray gap', () => {
    const r = rec('A', { exp: 1200 });
    r.trust.data_gaps = ['no_measured_yield'];
    expect(yieldStatus(r)).toBe('measured');
  });
  it('null without any gap is plain missing data', () => {
    expect(yieldStatus(rec('A', { exp: null }))).toBe('none');
  });
});

describe('compareRows in forage units', () => {
  it('shows tonnes with one decimal and still ranks by the raw value', () => {
    const rows = compareRows([forageRec('A', { exp: 30106 }), forageRec('B', { exp: 17006 })], 'dry_matter');
    const exp = rows.find((r) => r.id === 'expectedYield');
    expect(exp?.cells).toEqual(['30.1', '17']);
    expect(exp?.best).toEqual([0]);
  });
  it('defaults to whole kg/ha', () => {
    const exp = compareRows([rec('A', { exp: 4321.6 })]).find((r) => r.id === 'expectedYield');
    expect(exp?.cells).toEqual(['4322']);
  });
});
