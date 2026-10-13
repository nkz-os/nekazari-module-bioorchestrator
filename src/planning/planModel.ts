import type { TerminationMethod } from './types';

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

/** Owner decision (W7): a cover with less time than this is warned about, and allowed. Same as the backend calendar. */
export const SHORT_COVER_WEEKS = 8;
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

/**
 * Whether the cover-species field is in free-text mode: the user chose "other species", or the
 * draft holds a name that the current options do not list (the form was remounted, or a reload
 * dropped the species from the options). The mode follows the data, never only local state.
 */
export function isOtherCover(species: string, listed: string[], chosenOther: boolean): boolean {
  return chosenOther || (species !== '' && !listed.includes(species));
}
