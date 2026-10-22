import { safeLang } from './format';
import { AUTUMN_FROM_MONTH } from './planModel';

/** A sowing fortnight: day 1–15 or day 16–end of a month (ISO dates, inclusive). */
export interface Fortnight { start: string; end: string }

const utc = (iso: string) => new Date(`${iso}T00:00:00Z`);

/** Every fortnight of campaign C, Aug 1 of C-1 to Jul 31 of C (the strip's fallback when the calendar fails). */
export function campaignFortnights(campaign: number): Fortnight[] {
  const out: Fortnight[] = [];
  for (let i = 0; i < 12; i += 1) {
    const month = ((AUTUMN_FROM_MONTH - 1 + i) % 12) + 1;
    const year = month >= AUTUMN_FROM_MONTH ? campaign - 1 : campaign;
    const mm = String(month).padStart(2, '0');
    out.push(fortnightOf(`${year}-${mm}-01`), fortnightOf(`${year}-${mm}-16`));
  }
  return out;
}

/** The fortnight a day falls in (same split as the backend's planning/calendar.py). */
export function fortnightOf(iso: string): Fortnight {
  const [y, m, d] = iso.slice(0, 10).split('-').map(Number);
  const mm = String(m).padStart(2, '0');
  const last = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return d <= 15 ? { start: `${y}-${mm}-01`, end: `${y}-${mm}-15` } : { start: `${y}-${mm}-16`, end: `${y}-${mm}-${last}` };
}

export const sameFortnight = (a: Fortnight | null | undefined, b: Fortnight | null | undefined): boolean =>
  Boolean(a && b && a.start === b.start && a.end === b.end);

/** The day simulations use for a fortnight (spec §4): the 8th or the 23rd. */
export function middleDay(f: Fortnight): string {
  return `${f.start.slice(0, 8)}${f.start.slice(8, 10) === '01' ? '08' : '23'}`;
}

/** "16–31 de octubre de 2026" in the user's language (Intl only, W8). */
export function formatRange(f: Fortnight, lang: string): string {
  const fmt = new Intl.DateTimeFormat(safeLang(lang), { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC' });
  // formatRange is ES2021 (every supported browser has it); the project's lib is ES2020.
  return (fmt as Intl.DateTimeFormat & { formatRange(a: Date, b: Date): string }).formatRange(utc(f.start), utc(f.end));
}

export function monthLabel(year: number, month: number, lang: string): string {
  return new Intl.DateTimeFormat(safeLang(lang), { month: 'short', timeZone: 'UTC' })
    .format(new Date(Date.UTC(year, month - 1, 1)));
}

export interface MonthColumn { key: string; year: number; month: number; halves: Fortnight[] }

export function monthColumns(fortnights: Fortnight[]): MonthColumn[] {
  const cols: MonthColumn[] = [];
  for (const f of fortnights) {
    const key = f.start.slice(0, 7);
    const last = cols[cols.length - 1];
    if (last && last.key === key) last.halves.push(f);
    else cols.push({ key, year: Number(key.slice(0, 4)), month: Number(key.slice(5, 7)), halves: [f] });
  }
  return cols;
}

export const weeksBetween = (startIso: string, endIso: string): number =>
  Math.floor((utc(endIso).getTime() - utc(startIso).getTime()) / (7 * 86_400_000));

/** The list with `f` in its place (by start), unchanged when `f` is null or already there. */
export function withFortnight(list: Fortnight[], f: Fortnight | null): Fortnight[] {
  if (!f || list.some((x) => sameFortnight(x, f))) return list;
  return [...list, f].sort((a, b) => a.start.localeCompare(b.start));
}
