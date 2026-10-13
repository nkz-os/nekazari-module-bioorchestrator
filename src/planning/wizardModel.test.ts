import { describe, expect, it } from 'vitest';
import {
  applyCalendar, back, calendarMainParam, canAdvance, initialState, next, pickCrop, prefillFromCells, regimeToSave, savePlan, setSowing,
  stepsOf, toPlanIn,
} from './wizardModel';
import { vi } from 'vitest';
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
  it('the fallback cover sowing is the first option still before the new main sowing, or none', () => {
    const MAR1 = { start: '2027-03-01', end: '2027-03-15' };
    const s = { ...pickCrop(initialState(), 'maize', false, false, null), coverOn: true, coverSpecies: 'vetch', coverSowing: MAR1 };
    const FEB16 = { start: '2027-02-16', end: '2027-02-28' };
    expect(setSowing(s, FEB16, [MAR1, AUG1]).coverSowing).toEqual(AUG1);
    expect(setSowing(s, FEB16, [MAR1]).coverSowing).toBeNull();
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
  it('a free-text cover name is checked and sent trimmed, as the server resolves it', () => {
    const s = { ...pickCrop(initialState(), 'maize', false, false, null), step: 'cover' as const, sowing: OCT2, coverOn: true,
      coverSpecies: '  phacelia ', coverSowing: AUG1 };
    expect(canAdvance(s, ['vetch'])).toBe(true);
    expect(toPlanIn(s, 2027, 'rainfed', 'conventional').cycles[0].crop).toBe('phacelia');
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

describe('savePlan (W10)', () => {
  it('saves the plan, then the regime the farmer chose for a parcel that had none', async () => {
    const order: string[] = [];
    const out = await savePlan(async () => { order.push('plan'); }, async () => { order.push('regime'); });
    expect([out, order]).toEqual(['saved', ['plan', 'regime']]);
  });
  it('a failed regime save keeps the plan: it reports, never rolls back', async () => {
    const put = vi.fn().mockResolvedValue(undefined);
    expect(await savePlan(put, () => Promise.reject(new Error('503')))).toBe('regime_not_saved');
    expect(put).toHaveBeenCalledTimes(1);
  });
  it('a failed plan save throws and never writes the regime', async () => {
    const regime = vi.fn();
    await expect(savePlan(() => Promise.reject(new Error('409')), regime)).rejects.toThrow('409');
    expect(regime).not.toHaveBeenCalled();
  });
  it('a parcel that already has a regime writes only the plan', async () => {
    expect(await savePlan(async () => {}, null)).toBe('saved');
  });
});

describe('calendar wiring', () => {
  const NOV1 = { start: '2026-11-01', end: '2026-11-15' };
  const SEP1 = { start: '2026-09-01', end: '2026-09-15' };
  const cal = (selected: typeof OCT2, cover: { fits: boolean; default: typeof AUG1 | null; options: typeof AUG1[] }) =>
    ({ main: { selected, default: OCT2 }, cover: { ...cover, ends: selected.start, weeks: 8, short: false } }) as never;
  it('a moved main sowing is only sent from the cover step on: moving it on the strip never reloads the strip', () => {
    const s = { ...pickCrop(initialState(), 'wheat', false, false, null), sowing: NOV1 };
    expect(calendarMainParam({ ...s, step: 'sowing' }, OCT2)).toBeNull();
    expect(calendarMainParam({ ...s, step: 'cover' }, OCT2)).toBe('2026-11-01');
    expect(calendarMainParam({ ...s, step: 'cover', sowing: OCT2 }, OCT2)).toBeNull();
  });
  it('the first answer proposes the main sowing when none is chosen', () => {
    const s = pickCrop(initialState(), 'wheat', false, false, null);
    expect(applyCalendar(s, cal(OCT2, { fits: true, default: AUG1, options: [AUG1] })).sowing).toEqual(OCT2);
  });
  it('an answer for another main sowing never touches the cover (a pre-filled cover is not dropped)', () => {
    const s = { ...pickCrop(initialState(), 'wheat', false, false, null), sowing: NOV1, coverOn: true, coverSpecies: 'vetch', coverSowing: SEP1 };
    expect(applyCalendar(s, cal(OCT2, { fits: false, default: null, options: [] }))).toBe(s);
  });
  it('for the chosen main sowing: no room turns the cover off; a cover sowing that no longer fits falls back to the default', () => {
    const s = { ...pickCrop(initialState(), 'wheat', false, false, null), sowing: OCT2, coverOn: true, coverSpecies: 'vetch', coverSowing: SEP1 };
    expect(applyCalendar(s, cal(OCT2, { fits: false, default: null, options: [] }))).toMatchObject({ coverOn: false, coverSowing: null });
    expect(applyCalendar(s, cal(OCT2, { fits: true, default: AUG1, options: [AUG1] })).coverSowing).toEqual(AUG1);
    expect(applyCalendar(s, cal(OCT2, { fits: true, default: AUG1, options: [AUG1, SEP1] })).coverSowing).toEqual(SEP1);
  });
});

describe('regimeToSave (W10)', () => {
  it('writes the regime on the parcel unless the parcel already has its own', () => {
    expect(regimeToSave({ regime: null, regime_source: 'unknown' }, 'rainfed')).toBe('rainfed');
    // A regime read from the campaign's crop is not the parcel's: the parcel still lacks it.
    expect(regimeToSave({ regime: 'irrigated', regime_source: 'crop' }, 'irrigated')).toBe('irrigated');
    expect(regimeToSave({ regime: 'rainfed', regime_source: 'parcel:irrigationRegime' }, 'rainfed')).toBeNull();
    expect(regimeToSave({ regime: 'irrigated', regime_source: 'parcel:irrigationSystemType' }, 'irrigated')).toBeNull();
  });
});
