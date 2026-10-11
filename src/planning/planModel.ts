import type { CycleCell, Management, PlanCycleIn, PlanIn, Regime, Role, Sowing, SpeciesFit, TerminationMethod } from './types';

/** Select sentinel for "no value" (Radix Select forbids an empty option value). */
export const NONE = '__none__';
/**
 * A crop sown from this month on is harvested the next calendar year, so its sowing falls in
 * the year before the campaign. ASSUMPTION: right for winter crops, wrong for a summer catch
 * crop sown in August; it is only a default and the user edits it.
 */
export const AUTUMN_FROM_MONTH = 8;
// Owner decision: a cover crop ends by rolling or by mowing.
export const COVER_METHODS: TerminationMethod[] = ['roller_crimper', 'mowing'];

export interface CycleDraft { species: string; variety: string | null; sowing: string; end: string; method: TerminationMethod | null }
export interface PlanDraft { main: CycleDraft; cover: CycleDraft | null }
export type DraftError = 'missing_species' | 'species_name_invalid' | 'missing_dates' | 'termination_outside_campaign' | 'dates_inconsistent'
  | 'cycles_overlap' | 'perennial_start_outside_campaign' | 'perennial_too_long';
/** Same cap as the backend (plan_write.MAX_PERENNIAL_YEARS). */
export const MAX_PERENNIAL_YEARS = 10;
/**
 * A cover species the registry does not list is written by name; the server restricts the name to
 * letters, digits, space, dot, dash, underscore and brackets, 2 to 64 characters, starting with a letter
 * (plan_write._RAW_SPECIES_RE). Underscore is allowed because registry slugs carry it (faba_bean).
 */
export const RAW_SPECIES_NAME = /^[\p{L}][\p{L}\p{N}_ .()-]{1,63}$/u;

/** Campaign a sowing opens (same rule as the backend history.campaign_of_start). */
export function campaignOfStart(iso: string): number {
  const year = Number(iso.slice(0, 4));
  return Number(iso.slice(5, 7)) >= AUTUMN_FROM_MONTH ? year + 1 : year;
}

/**
 * Whether the cover-species field is in free-text mode: the user chose "other species", or the
 * draft holds a name that the current options do not list (the form was remounted, or a reload
 * dropped the species from the options). The mode follows the data, never only local state.
 */
export function isOtherCover(species: string, listed: string[], chosenOther: boolean): boolean {
  return chosenOther || (species !== '' && !listed.includes(species));
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
 * The form's starting point when the campaign already has a plan: the planned main crop and the first
 * planned cover or catch crop, so "Change" edits the plan instead of silently replacing it. Null when
 * there is no planned main crop. The termination method is not part of the read contract, so a cover
 * starts with the default one and the form says that saving replaces the plan.
 */
export function draftFromCells(cells: CycleCell[]): PlanDraft | null {
  const planned = cells.filter((c) => c.provenance === 'planned');
  const main = planned.find((c) => c.role === 'main_crop');
  if (!main) return null;
  const cover = planned.find((c) => c.role === 'cover_crop' || c.role === 'catch_crop');
  return {
    main: { species: main.species ?? '', variety: main.variety, sowing: main.start ?? '', end: main.end ?? '', method: null },
    cover: cover
      ? { species: cover.species ?? cover.species_raw ?? '', variety: cover.variety, sowing: cover.start ?? '', end: cover.end ?? '', method: 'roller_crimper' }
      : null,
  };
}

/**
 * Mirrors the backend's date rules (plan_write.validate_plan) so the user sees the error before the
 * request; the server stays authoritative. A perennial main crop (alfalfa) starts in the campaign,
 * ends in or after it and at most MAX_PERENNIAL_YEARS later. An annual cycle ends in the campaign
 * and opens at most in the campaign before it (autumn sowing). A cover species outside `listedCovers`
 * is free text and must be a valid name.
 */
export function validateDraft(d: PlanDraft, campaign: number, perennialMain = false, listedCovers: string[] = []): DraftError | null {
  const cycles = d.cover ? [d.cover, d.main] : [d.main];
  for (const c of cycles) {
    if (!c.species) return 'missing_species';
    if (c === d.cover && !listedCovers.includes(c.species) && !RAW_SPECIES_NAME.test(c.species.trim())) return 'species_name_invalid';
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
    role, crop: c.species.trim(), variety: c.variety, sowing_start: c.sowing, sowing_end: null,
    expected_termination: c.end, termination_method: c.method,
  });
  return {
    campaign, regime, management,
    cycles: d.cover ? [cycle(d.cover, 'cover_crop'), cycle(d.main, 'main_crop')] : [cycle(d.main, 'main_crop')],
  };
}
