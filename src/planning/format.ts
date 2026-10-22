import type { Names } from './types';

/** Accent of the module (same values as defineModule in Module.tsx). */
export const ACCENT = { base: '#10B981', soft: '#D1FAE5', strong: '#047857' };
const PARCEL_PREFIX = 'urn:ngsi-ld:AgriParcel:';

/** The language tag when the platform's Intl accepts it, else 'en'. A malformed tag never throws. */
export function safeLang(lang: string | null | undefined): string {
  if (!lang) return 'en';
  try {
    Intl.getCanonicalLocales(lang);
    return lang;
  } catch {
    return 'en';
  }
}

export function cropName(names: Names | undefined, fallback: string | null | undefined, lang: string): string {
  const base = safeLang(lang).split('-')[0];
  return names?.[base] ?? names?.en ?? names?.es ?? fallback ?? '—';
}

export function formatHa(value: number | null | undefined, lang: string): string | null {
  return value == null ? null : value.toLocaleString(safeLang(lang), { maximumFractionDigits: 1 });
}

export function formatT(value: number, lang: string): string {
  return value.toLocaleString(safeLang(lang), { maximumFractionDigits: 1 });
}

export function monthName(month: number, lang: string): string {
  return new Date(Date.UTC(2001, month - 1, 1)).toLocaleString(safeLang(lang), { month: 'short', timeZone: 'UTC' });
}

/**
 * An ISO date (YYYY-MM-DD, any time part ignored) in the user's language. It is read as a local date:
 * parsing it as UTC would show the day before west of Greenwich. Unreadable input comes back as given.
 */
export function formatIsoDate(iso: string, lang: string): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
  if (!m) return iso;
  const [year, month, day] = [Number(m[1]), Number(m[2]), Number(m[3])];
  const date = new Date(year, month - 1, day);
  // A day that rolled over (February 30th) is not a date.
  if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) return iso;
  return date.toLocaleDateString(safeLang(lang));
}

export const parcelUrn = (id: string): string => (id.startsWith('urn:') ? id : `${PARCEL_PREFIX}${id}`);
export const shortId = (id: string): string => id.split(':').pop() ?? id;
