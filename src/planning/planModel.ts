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

export function defaultSowing(s: Sowing, campaign: number): string {
  let month = s.start_month;
  if (!month && s.typical_sowing_doy) {
    const d = new Date(Date.UTC(2001, 0, 1));
    d.setUTCDate(s.typical_sowing_doy);
    month = d.getUTCMonth() + 1;
  }
  if (!month) return '';
  const year = month >= AUTUMN_FROM_MONTH ? campaign - 1 : campaign;
  return `${year}-${String(month).padStart(2, '0')}-01`;
}

export function draftFor(sp: Pick<SpeciesFit, 'species' | 'sowing'>, campaign: number): PlanDraft {
  return { main: { species: sp.species, variety: null, sowing: defaultSowing(sp.sowing, campaign), end: '', method: null }, cover: null };
}

/**
 * Same rules as backend validate_plan, so the user sees the error before the request. A
 * perennial main crop (alfalfa) starts in the campaign and may end up to MAX_PERENNIAL_YEARS later.
 */
export function validateDraft(d: PlanDraft, campaign: number, perennialMain = false): DraftError | null {
  const cycles = d.cover ? [d.cover, d.main] : [d.main];
  for (const c of cycles) {
    if (!c.species) return 'missing_species';
    if (!c.sowing || !c.end) return 'missing_dates';
    const endYear = Number(c.end.slice(0, 4));
    if (c === d.main && perennialMain) {
      if (campaignOfStart(c.sowing) !== campaign) return 'perennial_start_outside_campaign';
      if (endYear < campaign || endYear > campaign + MAX_PERENNIAL_YEARS) return 'perennial_too_long';
    } else if (endYear !== campaign) {
      return 'termination_outside_campaign';
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
