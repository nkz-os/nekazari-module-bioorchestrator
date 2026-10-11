import { describe, expect, it } from 'vitest';
import { defaultSowing, draftFor, toPlanIn, validateDraft } from './planModel';
import type { SpeciesFit } from './types';

const sowing = (start_month: number | null) => ({ sowing_type: null, start_month, end_month: null, typical_sowing_doy: null, source: null });

describe('plan model', () => {
  it('autumn sowing belongs to the year before the campaign', () => {
    expect(defaultSowing(sowing(10), 2027)).toBe('2026-10-01');
    expect(defaultSowing(sowing(4), 2027)).toBe('2027-04-01');
    expect(defaultSowing(sowing(null), 2027)).toBe('');
  });
  it('mirrors the backend rules, roller-crimp day = sowing day allowed', () => {
    const d = { main: { species: 'maize', variety: null, sowing: '2027-04-20', end: '2027-10-01', method: null },
      cover: { species: 'vetch', variety: null, sowing: '2026-10-01', end: '2027-04-20', method: 'roller_crimper' as const } };
    expect(validateDraft(d, 2027)).toBeNull();
    expect(validateDraft({ ...d, cover: null, main: { ...d.main, end: '' } }, 2027)).toBe('missing_dates');
    expect(validateDraft({ ...d, cover: null, main: { ...d.main, end: '2028-01-10' } }, 2027)).toBe('termination_outside_campaign');
    expect(validateDraft({ ...d, cover: { ...d.cover, end: '2027-05-01' } }, 2027)).toBe('cycles_overlap');
  });
  it('alfalfa starts in the campaign and lasts several years', () => {
    const alfalfa = { main: { species: 'alfalfa', variety: null, sowing: '2026-09-15', end: '2030-06-30', method: null }, cover: null };
    expect(validateDraft(alfalfa, 2027, true)).toBeNull();
    expect(validateDraft(alfalfa, 2027, false)).toBe('termination_outside_campaign');
    expect(validateDraft({ main: { ...alfalfa.main, sowing: '2027-09-15' }, cover: null }, 2027, true)).toBe('perennial_start_outside_campaign');
    expect(validateDraft({ main: { ...alfalfa.main, end: '2040-06-30' }, cover: null }, 2027, true)).toBe('perennial_too_long');
  });
  it('builds the PUT body, cover first, variety optional', () => {
    const fit = { species: 'wheat', sowing: sowing(11) } as SpeciesFit;
    const draft = { ...draftFor(fit, 2027) };
    draft.main.end = '2027-07-01';
    const body = toPlanIn(draft, 2027, 'secano', 'conventional');
    expect(body.cycles).toEqual([{ role: 'main_crop', crop: 'wheat', variety: null, sowing_start: '2026-11-01',
      sowing_end: null, expected_termination: '2027-07-01', termination_method: null }]);
  });
});
