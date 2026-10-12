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

/**
 * Owner decision (2026-10-12): no harvest or end date is asked. An annual main crop occupies the parcel
 * until its campaign closes, a cover ends when the main crop is sown, and a perennial main crop (alfalfa)
 * stands for `years` campaigns. The server derives the dates (plan_write._ends).
 */
export interface CycleDraft { species: string; variety: string | null; sowing: string; years: number | null; method: TerminationMethod | null }
export interface PlanDraft { main: CycleDraft; cover: CycleDraft | null }
export type DraftError = 'missing_species' | 'species_name_invalid' | 'missing_dates' | 'dates_inconsistent'
  | 'perennial_start_outside_campaign' | 'perennial_too_long' | 'missing_duration' | 'sowing_outside_campaign';
/** Same cap as the backend (plan_write.MAX_PERENNIAL_YEARS). */
export const MAX_PERENNIAL_YEARS = 10;
/** ASSUMPTION: the usual stand of an alfalfa field; only a starting value, the user edits it. */
export const DEFAULT_PERENNIAL_YEARS = 4;
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
  return { species: '', variety: null, sowing: '', years: null, method: 'roller_crimper' };
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

export function draftFor(sp: Pick<SpeciesFit, 'species' | 'sowing' | 'lifecycle'>, campaign: number): PlanDraft {
  const years = sp.lifecycle === 'perennial' ? DEFAULT_PERENNIAL_YEARS : null;
  return { main: { species: sp.species, variety: null, sowing: defaultSowing(sp.sowing, campaign), years, method: null }, cover: null };
}

/**
 * The form's starting point when the campaign already has a plan: the planned main crop and the first
 * planned cover or catch crop, so "Change" edits the plan instead of silently replacing it. Null when
 * there is no planned main crop. The termination method is not part of the read contract, so a cover
 * starts with the default one and the form says that saving replaces the plan.
 */
export function draftFromCells(cells: CycleCell[], campaign: number): PlanDraft | null {
  const planned = cells.filter((c) => c.provenance === 'planned');
  const main = planned.find((c) => c.role === 'main_crop');
  if (!main) return null;
  const cover = planned.find((c) => c.role === 'cover_crop' || c.role === 'catch_crop');
  // A perennial's years are read back from its end: the last campaign it stands in.
  const years = main.lifecycle !== 'perennial' ? null : main.end ? Number(main.end.slice(0, 4)) - campaign + 1 : DEFAULT_PERENNIAL_YEARS;
  return {
    main: { species: main.species ?? '', variety: main.variety, sowing: main.start ?? '', years, method: null },
    cover: cover
      ? { species: cover.species ?? cover.species_raw ?? '', variety: cover.variety, sowing: cover.start ?? '', years: null, method: 'roller_crimper' }
      : null,
  };
}

/**
 * Mirrors the backend's date rules (plan_write.validate_plan) so the user sees the error before the
 * request; the server stays authoritative. Only sowings are dated. An annual main crop is sown in the
 * campaign; a perennial main crop (alfalfa) is sown in it and stands 1 to MAX_PERENNIAL_YEARS campaigns.
 * A cover is sown no later than the main crop (it ends when the main crop is sown) and opens at most the
 * campaign before. A cover species outside `listedCovers` is free text and must be a valid name.
 */
export function validateDraft(d: PlanDraft, campaign: number, perennialMain = false, listedCovers: string[] = []): DraftError | null {
  const cycles = d.cover ? [d.cover, d.main] : [d.main];
  for (const c of cycles) {
    if (!c.species) return 'missing_species';
    if (c === d.cover && !listedCovers.includes(c.species) && !RAW_SPECIES_NAME.test(c.species.trim())) return 'species_name_invalid';
    if (!c.sowing) return 'missing_dates';
  }
  const main = d.main;
  if (perennialMain) {
    if (campaignOfStart(main.sowing) !== campaign) return 'perennial_start_outside_campaign';
    if (main.years === null || !Number.isInteger(main.years) || main.years < 1) return 'missing_duration';
    if (main.years > MAX_PERENNIAL_YEARS) return 'perennial_too_long';
  } else if (campaignOfStart(main.sowing) !== campaign) {
    return 'sowing_outside_campaign';
  }
  if (d.cover && (campaignOfStart(d.cover.sowing) < campaign - 1 || d.cover.sowing > main.sowing)) return 'dates_inconsistent';
  return null;
}

export function toPlanIn(d: PlanDraft, campaign: number, regime: Regime, management: Management, perennialMain = false): PlanIn {
  const cycle = (c: CycleDraft, role: Role): PlanCycleIn => ({
    role, crop: c.species.trim(), variety: c.variety, sowing_start: c.sowing, sowing_end: null,
    expected_termination: null, termination_method: c.method,
    ...(role === 'main_crop' && perennialMain ? { perennial_years: c.years } : {}),
  });
  return {
    campaign, regime, management,
    cycles: d.cover ? [cycle(d.cover, 'cover_crop'), cycle(d.main, 'main_crop')] : [cycle(d.main, 'main_crop')],
  };
}
