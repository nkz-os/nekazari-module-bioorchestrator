import { describe, expect, it } from 'vitest';
import { COVER_METHODS, DEFAULT_PERENNIAL_YEARS, dateOfDoy, defaultSowing, draftFor, draftFromCells, isOtherCover, toPlanIn, validateDraft } from './planModel';
import type { CycleCell, SpeciesFit } from './types';

const sowing = (start_month: number | null) => ({ sowing_type: null, start_month, end_month: null, typical_sowing_doy: null, source: null });

describe('plan model', () => {
  it('autumn sowing belongs to the year before the campaign', () => {
    expect(defaultSowing(sowing(10), 2027)).toBe('2026-10-01');
    expect(defaultSowing(sowing(4), 2027)).toBe('2027-04-01');
    expect(defaultSowing(sowing(null), 2027)).toBe('');
  });
  it('a day of year is clamped to the length of its year (day 366 exists only in leap years)', () => {
    expect(dateOfDoy(366, 2026)).toBe('2026-12-31');
    expect(dateOfDoy(366, 2028)).toBe('2028-12-31');
    expect(dateOfDoy(300, 2026)).toBe('2026-10-27');
  });
  it('a typical sowing day falls in the campaign rule, with day 366 clamped to Dec 31', () => {
    const doy = (typical_sowing_doy: number) => ({ sowing_type: null, start_month: null, end_month: null, typical_sowing_doy, source: null });
    expect(defaultSowing(doy(300), 2027)).toBe('2026-10-01');
    expect(defaultSowing(doy(366), 2027)).toBe('2026-12-01');
    expect(defaultSowing(doy(366), 2028)).toBe('2027-12-01');
    expect(defaultSowing(doy(100), 2027)).toBe('2027-04-01');
  });
  it('mirrors the backend rules: only sowings are dated, a cover sown on the main crop day is allowed', () => {
    const d = { main: { species: 'maize', variety: null, sowing: '2027-04-20', years: null, method: null },
      cover: { species: 'vetch', variety: null, sowing: '2026-10-01', years: null, method: 'roller_crimper' as const } };
    expect(validateDraft(d, 2027)).toBeNull();
    expect(validateDraft({ ...d, cover: { ...d.cover, sowing: '2027-04-20' } }, 2027)).toBeNull();
    expect(validateDraft({ ...d, cover: null, main: { ...d.main, sowing: '' } }, 2027)).toBe('missing_dates');
    expect(validateDraft({ ...d, cover: { ...d.cover, sowing: '2027-05-01' } }, 2027)).toBe('dates_inconsistent');
    expect(validateDraft({ ...d, cover: { ...d.cover, sowing: '2025-06-01' } }, 2027)).toBe('dates_inconsistent');
  });
  it('an annual main crop is sown in its campaign: the autumn before counts, a year earlier or later does not', () => {
    const wheat = (sowing: string) => ({ main: { species: 'wheat', variety: null, sowing, years: null, method: null }, cover: null });
    expect(validateDraft(wheat('2026-10-01'), 2027)).toBeNull();
    expect(validateDraft(wheat('2027-03-01'), 2027)).toBeNull();
    expect(validateDraft(wheat('2026-03-01'), 2027)).toBe('sowing_outside_campaign');
    expect(validateDraft(wheat('2027-09-01'), 2027)).toBe('sowing_outside_campaign');
  });
  it('alfalfa is sown in the campaign and stands 1 to 10 years', () => {
    const alfalfa = (years: number | null, sowing = '2026-09-15') => ({ main: { species: 'alfalfa', variety: null, sowing, years, method: null }, cover: null });
    expect(validateDraft(alfalfa(4), 2027, true)).toBeNull();
    expect(validateDraft(alfalfa(4, '2027-09-15'), 2027, true)).toBe('perennial_start_outside_campaign');
    expect(validateDraft(alfalfa(null), 2027, true)).toBe('missing_duration');
    expect(validateDraft(alfalfa(0), 2027, true)).toBe('missing_duration');
    expect(validateDraft(alfalfa(2.5), 2027, true)).toBe('missing_duration');
    expect(validateDraft(alfalfa(11), 2027, true)).toBe('perennial_too_long');
  });
  it('builds the PUT body with no end dates: the server derives them', () => {
    const fit = { species: 'wheat', sowing: sowing(11), lifecycle: 'annual' } as SpeciesFit;
    const body = toPlanIn(draftFor(fit, 2027), 2027, 'rainfed', 'conventional');
    expect(body.cycles).toEqual([{ role: 'main_crop', crop: 'wheat', variety: null, sowing_start: '2026-11-01',
      sowing_end: null, expected_termination: null, termination_method: null }]);
  });
  it('a perennial main crop starts with the default years and sends them', () => {
    const fit = { species: 'alfalfa', sowing: sowing(9), lifecycle: 'perennial' } as SpeciesFit;
    const draft = draftFor(fit, 2027);
    expect(draft.main.years).toBe(DEFAULT_PERENNIAL_YEARS);
    expect(toPlanIn(draft, 2027, 'rainfed', 'conventional', true).cycles[0]).toMatchObject({ perennial_years: 4, expected_termination: null });
  });
});

describe('isOtherCover', () => {
  const listed = ['vicia', 'avena'];
  it('a listed species is not "other"', () => {
    expect(isOtherCover('vicia', listed, false)).toBe(false);
  });
  it('a name typed in "other" mode stays in that mode even when it equals a listed species (the box must not vanish while typing)', () => {
    expect(isOtherCover('vicia', listed, true)).toBe(true);
  });
  it('a typed species that is not listed is "other", even after a remount lost the choice', () => {
    expect(isOtherCover('phacelia', listed, false)).toBe(true);
    expect(isOtherCover('phacelia', [], false)).toBe(true);
  });
  it('an empty species is "other" only when the user chose it', () => {
    expect(isOtherCover('', listed, true)).toBe(true);
    expect(isOtherCover('', listed, false)).toBe(false);
  });
});

describe('draftFromCells', () => {
  const draftFromCells2027 = (cells: CycleCell[]) => draftFromCells(cells, 2027);
  const cell = (over: Partial<CycleCell>): CycleCell => ({
    crop_id: 'c', species: 'maize', species_raw: 'ZEAMX', names: { es: 'maíz' }, variety: 'Var X', role: 'main_crop',
    provenance: 'planned', status: 'planned', start: '2027-04-20', end: '2027-10-01', lifecycle: 'annual', first_campaign: 2027, ...over,
  });
  it('starts from the planned main crop: species, variety and dates, no cover, termination method unset', () => {
    expect(draftFromCells2027([cell({})])).toEqual({
      main: { species: 'maize', variety: 'Var X', sowing: '2027-04-20', years: null, method: null }, cover: null,
    });
  });
  it('adds the planned cover, whose free-text name comes from the raw species; its termination method is not in the contract', () => {
    const cover = cell({ role: 'cover_crop', species: null, species_raw: 'phacelia', variety: null, start: '2026-10-01', end: '2027-04-20' });
    expect(draftFromCells2027([cover, cell({})])).toEqual({
      main: { species: 'maize', variety: 'Var X', sowing: '2027-04-20', years: null, method: null },
      cover: { species: 'phacelia', variety: null, sowing: '2026-10-01', years: null, method: 'roller_crimper' },
    });
  });
  it('a registry cover uses its slug, a catch crop counts as a cover, and the first one wins', () => {
    const vetch = cell({ role: 'catch_crop', species: 'vetch', species_raw: 'VICSA', variety: null, start: '2026-10-01', end: '2027-03-01' });
    const oats = cell({ role: 'cover_crop', species: 'oats', species_raw: 'AVESA', variety: null, start: '2026-11-01', end: '2027-03-01' });
    expect(draftFromCells2027([vetch, oats, cell({})])?.cover?.species).toBe('vetch');
  });
  it('missing dates become empty strings', () => {
    expect(draftFromCells2027([cell({ start: null, end: null })])?.main).toMatchObject({ sowing: '', years: null });
  });
  it('is null without a planned main crop (a lone cover does not make a plan)', () => {
    expect(draftFromCells2027([])).toBeNull();
    expect(draftFromCells2027([cell({ role: 'cover_crop' })])).toBeNull();
    expect(draftFromCells2027([cell({ provenance: 'actual', status: 'active' })])).toBeNull();
  });
  it('a planned perennial reads its years back from its end, or starts from the default', () => {
    const alfalfa = (end: string | null) => cell({ species: 'alfalfa', lifecycle: 'perennial', start: '2026-09-15', end });
    expect(draftFromCells2027([alfalfa('2030-07-31')])?.main.years).toBe(4);
    expect(draftFromCells2027([alfalfa(null)])?.main.years).toBe(4);
    expect(draftFromCells2027([cell({})])?.main.years).toBeNull();
  });
  it('ignores cells that are not planned', () => {
    const sownCover = cell({ role: 'cover_crop', provenance: 'actual', status: 'active', species: 'vetch' });
    expect(draftFromCells2027([sownCover, cell({})])?.cover).toBeNull();
    expect(draftFromCells2027([cell({ provenance: 'declared' }), cell({ variety: 'Planned' })])?.main.variety).toBe('Planned');
    expect(draftFromCells2027([cell({ provenance: 'unknown' })])).toBeNull();
  });
});

describe('free-text cover names', () => {
  const draft = (cover: string) => ({
    main: { species: 'maize', variety: null, sowing: '2027-04-20', years: null, method: null },
    cover: { species: cover, variety: null, sowing: '2026-10-01', years: null, method: 'roller_crimper' as const },
  });
  it('accepts a name made of letters, digits, spaces, dots, dashes and brackets (what the server accepts)', () => {
    for (const ok of ['Phacelia', 'ab', 'Avena sativa (L.)', 'veza-avena', 'Haba ñandú', ' phacelia ', 'bitter_vetch']) {
      expect(validateDraft(draft(ok), 2027)).toBeNull();
    }
  });
  it('refuses a name the server would not resolve: too short, wrong characters, digit first, too long', () => {
    for (const bad of ['x', '<b>', 'veza/avena', '1abc', 'a'.repeat(65), '   ', 'a;b']) {
      expect(validateDraft(draft(bad), 2027)).toBe('species_name_invalid');
    }
    expect(validateDraft(draft('a'.repeat(64)), 2027)).toBeNull();
  });
  it('a listed species is not checked as a name, and an empty one is still missing_species', () => {
    expect(validateDraft(draft('x'), 2027, false, ['x'])).toBeNull();
    expect(validateDraft(draft('veza/avena'), 2027, false, ['vetch'])).toBe('species_name_invalid');
    expect(validateDraft(draft(''), 2027)).toBe('missing_species');
  });
  it('the main crop is never checked as free text', () => {
    expect(validateDraft({ main: { species: 'x', variety: null, sowing: '2027-04-20', years: null, method: null }, cover: null }, 2027)).toBeNull();
  });
  it('the PUT body sends the cover name trimmed, as it was validated', () => {
    expect(toPlanIn(draft('  phacelia '), 2027, 'rainfed', 'conventional').cycles[0].crop).toBe('phacelia');
  });
});

describe('cover termination', () => {
  it('offers the roller crimper first, then mowing, and nothing else', () => {
    expect(COVER_METHODS).toEqual(['roller_crimper', 'mowing']);
  });
});
