import { describe, expect, it } from 'vitest';
import { back, canAdvance, initialState, next, pickCrop, prefillFromCells, setSowing, stepsOf, toPlanIn } from './wizardModel';
import type { CycleCell } from './types';

const OCT2 = { start: '2026-10-16', end: '2026-10-31' };
const AUG1 = { start: '2026-08-01', end: '2026-08-15' };

describe('wizard model', () => {
  it('skips the variety step when the crop has no trials', () => {
    const s = pickCrop(initialState(), 'alfalfa', false, true, 4);
    expect(stepsOf(s)).toEqual(['crop', 'sowing', 'cover', 'summary']);
    expect(stepsOf(pickCrop(initialState(), 'wheat', true, false, null))).toEqual(['crop', 'variety', 'sowing', 'cover', 'summary']);
  });
  it('a crop change resets variety, sowing and cover', () => {
    let s = pickCrop(initialState(), 'wheat', true, false, null);
    s = { ...s, variety: 'RGT TOCAYO', sowing: OCT2, coverOn: true, coverSpecies: 'vetch', coverSowing: AUG1 };
    s = pickCrop(s, 'barley', true, false, null);
    expect([s.variety, s.sowing, s.coverOn, s.coverSpecies, s.coverSowing]).toEqual([null, null, false, '', null]);
  });
  it('Siguiente needs a crop, then a sowing, then a valid cover when on', () => {
    let s = initialState();
    expect(canAdvance(s, [])).toBe(false);
    s = pickCrop(s, 'wheat', false, false, null);
    expect(canAdvance(s, [])).toBe(true);
    s = next(s); // sowing
    expect(canAdvance(s, [])).toBe(false);
    s = { ...s, sowing: OCT2 };
    expect(canAdvance(s, [])).toBe(true);
    s = next({ ...s, coverOn: true }); // cover
    expect(canAdvance(s, ['vetch'])).toBe(false);
    expect(canAdvance({ ...s, coverSpecies: 'vetch', coverSowing: AUG1 }, ['vetch'])).toBe(true);
    expect(canAdvance({ ...s, coverSpecies: 'x', coverSowing: AUG1 }, ['vetch'])).toBe(false); // invalid free name
  });
  it('a perennial needs its years between 1 and 10', () => {
    const s = { ...next(pickCrop(initialState(), 'alfalfa', false, true, 4)), sowing: OCT2 };
    expect(canAdvance(s, [])).toBe(true);
    expect(canAdvance({ ...s, years: 0 }, [])).toBe(false);
    expect(canAdvance({ ...s, years: 11 }, [])).toBe(false);
  });
  it('moving the main sowing before the chosen cover sowing resets the cover sowing to the new default', () => {
    const s = { ...pickCrop(initialState(), 'maize', false, false, null), coverOn: true, coverSpecies: 'vetch',
      coverSowing: { start: '2027-03-01', end: '2027-03-15' } };
    const moved = setSowing(s, { start: '2027-02-16', end: '2027-02-28' }, [AUG1]);
    expect(moved.coverSowing).toEqual(AUG1);
  });
  it('back keeps choices; next never runs past the summary', () => {
    let s = pickCrop(initialState(), 'wheat', false, false, null);
    s = next(next(next(next(s))));
    expect(s.step).toBe('summary');
    expect(next(s).step).toBe('summary');
    expect(back(s).step).toBe('cover');
    expect(back(s).crop).toBe('wheat');
  });
  it('an existing plan pre-fills crop, variety, fortnights and cover; a lone cover or no plan pre-fills nothing', () => {
    const cell = (over: Partial<CycleCell>): CycleCell => ({
      crop_id: 'c', species: 'maize', species_raw: 'ZEAMX', names: { es: 'maíz' }, variety: 'Var X', role: 'main_crop',
      provenance: 'planned', status: 'planned', start: '2027-04-20', end: '2027-10-01', lifecycle: 'annual', first_campaign: 2027, ...over,
    });
    const cover = cell({ role: 'cover_crop', species: null, species_raw: 'phacelia', variety: null, start: '2026-10-01', end: '2027-04-20' });
    expect(prefillFromCells([cover, cell({})], 2027)).toEqual({
      crop: 'maize', perennial: false, variety: 'Var X', sowing: { start: '2027-04-16', end: '2027-04-30' }, years: null,
      coverOn: true, coverSpecies: 'phacelia', coverSowing: { start: '2026-10-01', end: '2026-10-15' },
    });
    expect(prefillFromCells([cell({ lifecycle: 'perennial', species: 'alfalfa', end: '2030-07-31' })], 2027).years).toBe(4);
    expect(prefillFromCells([cell({ role: 'cover_crop' })], 2027)).toEqual({});
    expect(prefillFromCells([cell({ provenance: 'actual' })], 2027)).toEqual({});
  });
  it('builds the PUT body with fortnight windows; the cover end is derived by the server', () => {
    const s = { ...pickCrop(initialState(), 'wheat', false, false, null), variety: 'RGT TOCAYO', sowing: OCT2,
      coverOn: true, coverSpecies: 'vetch', coverSowing: AUG1, coverMethod: 'roller_crimper' as const };
    expect(toPlanIn(s, 2027, 'rainfed', 'conventional').cycles).toEqual([
      { role: 'cover_crop', crop: 'vetch', variety: null, sowing_start: '2026-08-01', sowing_end: '2026-08-15',
        expected_termination: null, termination_method: 'roller_crimper' },
      { role: 'main_crop', crop: 'wheat', variety: 'RGT TOCAYO', sowing_start: '2026-10-16', sowing_end: '2026-10-31',
        expected_termination: null, termination_method: null },
    ]);
  });
});
