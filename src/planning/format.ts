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

export const parcelUrn = (id: string): string => (id.startsWith('urn:') ? id : `${PARCEL_PREFIX}${id}`);
export const shortId = (id: string): string => id.split(':').pop() ?? id;
