import type { Names } from './types';

/** Accent of the module (same values as defineModule in Module.tsx). */
export const ACCENT = { base: '#10B981', soft: '#D1FAE5', strong: '#047857' };
const PARCEL_PREFIX = 'urn:ngsi-ld:AgriParcel:';

export function cropName(names: Names | undefined, fallback: string | null | undefined, lang: string): string {
  const base = (lang || 'en').split('-')[0];
  return names?.[base] ?? names?.en ?? names?.es ?? fallback ?? '—';
}

export function formatHa(value: number | null | undefined, lang: string): string | null {
  return value == null ? null : value.toLocaleString(lang, { maximumFractionDigits: 1 });
}

export function formatT(value: number, lang: string): string {
  return value.toLocaleString(lang, { maximumFractionDigits: 1 });
}

export function monthName(month: number, lang: string): string {
  return new Date(Date.UTC(2001, month - 1, 1)).toLocaleString(lang, { month: 'short', timeZone: 'UTC' });
}

export const parcelUrn = (id: string): string => (id.startsWith('urn:') ? id : `${PARCEL_PREFIX}${id}`);
export const shortId = (id: string): string => id.split(':').pop() ?? id;
