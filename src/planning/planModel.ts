import type { Management, PlanCycleIn, PlanIn, Regime, Role, Sowing, SpeciesFit, TerminationMethod } from './types';

/** Select sentinel for "no value" (Radix Select forbids an empty option value). */
export const NONE = '__none__';
/**
 * A crop sown from this month on is harvested the next calendar year, so its sowing falls in
 * the year before the campaign. ASSUMPTION: right for winter crops, wrong for a summer catch
 * crop sown in August; it is only a default and the user edits it.
 */
export const AUTUMN_FROM_MONTH = 8;
export const COVER_METHODS: TerminationMethod[] = ['roller_crimper', 'incorporate', 'grazing', 'harvest'];

export interface CycleDraft { species: string; variety: string | null; sowing: string; end: string; method: TerminationMethod | null }
export interface PlanDraft { main: CycleDraft; cover: CycleDraft | null }
export type DraftError = 'missing_species' | 'missing_dates' | 'termination_outside_campaign' | 'dates_inconsistent'
  | 'cycles_overlap' | 'perennial_start_outside_campaign' | 'perennial_too_long';
/** Same cap as the backend (plan_write.MAX_PERENNIAL_YEARS). */
export const MAX_PERENNIAL_YEARS = 10;

/** Campaign a sowing opens (same rule as the backend history.campaign_of_start). */
export function campaignOfStart(iso: string): number {
  const year = Number(iso.slice(0, 4));
  return Number(iso.slice(5, 7)) >= AUTUMN_FROM_MONTH ? year + 1 : year;
}

export function emptyCover(): CycleDraft {
  return { species: '', variety: null, sowing: '', end: '', method: 'roller_crimper' };
}

const isLeap = (year: number): boolean => (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;

/** Calendar date (YYYY-MM-DD) of a day of year in `year`; a day past the end of that year is clamped to Dec 31. */
export function dateOfDoy(doy: number, year: number): string {
  const length = isLeap(year) ? 366 : 365;
  const day = Math.min(Math.max(doy, 1), length);
  return new Date(Date.UTC(year, 0, day)).toISOString().slice(0, 10);
}

/** Month of a typical sowing day: read in the autumn year when it falls there, else in the campaign year. */
function monthOfTypicalDoy(doy: number, campaign: number): number {
  const autumn = Number(dateOfDoy(doy, campaign - 1).slice(5, 7));
  return autumn >= AUTUMN_FROM_MONTH ? autumn : Number(dateOfDoy(doy, campaign).slice(5, 7));
}

export function defaultSowing(s: Sowing, campaign: number): string {
  let month = s.start_month;
  if (!month && s.typical_sowing_doy) month = monthOfTypicalDoy(s.typical_sowing_doy, campaign);
  if (!month) return '';
  const year = month >= AUTUMN_FROM_MONTH ? campaign - 1 : campaign;
  return `${year}-${String(month).padStart(2, '0')}-01`;
}

export function draftFor(sp: Pick<SpeciesFit, 'species' | 'sowing'>, campaign: number): PlanDraft {
  return { main: { species: sp.species, variety: null, sowing: defaultSowing(sp.sowing, campaign), end: '', method: null }, cover: null };
}

/**
 * Mirrors the backend's date rules (plan_write.validate_plan) so the user sees the error before the
 * request; the server stays authoritative. A perennial main crop (alfalfa) starts in the campaign,
 * ends in or after it and at most MAX_PERENNIAL_YEARS later. An annual cycle ends in the campaign
 * and opens at most in the campaign before it (autumn sowing).
 */
export function validateDraft(d: PlanDraft, campaign: number, perennialMain = false): DraftError | null {
  const cycles = d.cover ? [d.cover, d.main] : [d.main];
  for (const c of cycles) {
    if (!c.species) return 'missing_species';
    if (!c.sowing || !c.end) return 'missing_dates';
    const endYear = Number(c.end.slice(0, 4));
    if (c === d.main && perennialMain) {
      if (campaignOfStart(c.sowing) !== campaign) return 'perennial_start_outside_campaign';
      if (endYear < campaign) return 'dates_inconsistent';
      if (endYear > campaign + MAX_PERENNIAL_YEARS) return 'perennial_too_long';
    } else {
      if (endYear !== campaign) return 'termination_outside_campaign';
      if (campaignOfStart(c.sowing) < campaign - 1) return 'dates_inconsistent';
    }
    if (c.sowing > c.end) return 'dates_inconsistent';
  }
  const spans = cycles.map((c) => [c.sowing, c.end] as const).sort((a, b) => a[0].localeCompare(b[0]));
  for (let i = 1; i < spans.length; i += 1) {
    if (spans[i][0] < spans[i - 1][1]) return 'cycles_overlap';
  }
  return null;
}

export function toPlanIn(d: PlanDraft, campaign: number, regime: Regime, management: Management): PlanIn {
  const cycle = (c: CycleDraft, role: Role): PlanCycleIn => ({
    role, crop: c.species, variety: c.variety, sowing_start: c.sowing, sowing_end: null,
    expected_termination: c.end, termination_method: c.method,
  });
  return {
    campaign, regime, management,
    cycles: d.cover ? [cycle(d.cover, 'cover_crop'), cycle(d.main, 'main_crop')] : [cycle(d.main, 'main_crop')],
  };
}
