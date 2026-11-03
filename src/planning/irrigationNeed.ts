import { num } from './regionalYield';
import type { IrrigationNeed, SpeciesFit, TFn } from './types';

export const IRRIGATION_UNAVAILABLE_REASONS = [
  'regime_not_irrigated', 'no_kc', 'kc_unavailable', 'no_stage_lengths', 'no_sowing_date', 'pending', 'weather_unavailable',
] as const;

/** Largest allotment accepted, in mm (a season rarely needs more than 1 000 mm; 2 000 leaves room for error). */
export const MAX_ALLOTMENT_MM = 2000;
export type AllotmentUnit = 'mm' | 'm3ha';

/** «Riego: ~450 mm (380–520) en año medio»: the median with the P10–P90 range, rounded to whole mm. */
export function irrigationLine(n: IrrigationNeed, lang: string, t: TFn): string {
  const { p10, p50, p90 } = n.percentiles;
  return t('planning.wizard.crop.irrigation.line', { p50: num(p50, lang, 0, false), p10: num(p10, lang, 0, false), p90: num(p90, lang, 0, false) });
}

/** The need is shown only for an irrigated parcel; a rainfed or unknown regime shows nothing. */
export const irrigatedParcel = (regime: string | null | undefined): boolean => regime === 'irrigated';

export const isPending = (s: SpeciesFit): boolean => s.irrigation_need_unavailable === 'pending';

/** Short human message for a missing figure; an unknown reason reads as the generic one. */
export function irrigationUnavailableKey(reason: string | null | undefined): string {
  const known = (IRRIGATION_UNAVAILABLE_REASONS as readonly string[]).includes(reason ?? '');
  return `planning.wizard.crop.irrigation.unavailable.${known ? reason : 'weather_unavailable'}`;
}

/** Allotment is kept in mm; 1 mm of water is 10 m³/ha. */
export const allotmentToMm = (value: number, unit: AllotmentUnit): number => (unit === 'm3ha' ? value / 10 : value);
export const mmToUnit = (mm: number, unit: AllotmentUnit): number => (unit === 'm3ha' ? mm * 10 : mm);

export type AllotmentParse = { ok: true; mm: number } | { ok: false; error: 'invalid' | 'tooLarge' };

/** Positive number (comma or dot decimals), at most MAX_ALLOTMENT_MM once converted to mm. */
export function parseAllotment(text: string, unit: AllotmentUnit): AllotmentParse {
  const clean = text.trim().replace(',', '.');
  if (!/^\d+(\.\d+)?$/.test(clean)) return { ok: false, error: 'invalid' };
  const value = Number(clean);
  if (!Number.isFinite(value) || value <= 0) return { ok: false, error: 'invalid' };
  const mm = allotmentToMm(value, unit);
  if (mm > MAX_ALLOTMENT_MM) return { ok: false, error: 'tooLarge' };
  return { ok: true, mm: Math.round(mm * 100) / 100 };
}

/**
 * Arms the single refetch for `key`: it fires once after `ms`, never again for the same key, and the
 * returned function cancels it (unmount, or the parcel/campaign/regime changed).
 */
export function armOneShotRefetch(done: { current: string | null }, key: string, ms: number, fire: () => void): () => void {
  if (done.current === key) return () => {};
  const timer = setTimeout(() => { done.current = key; fire(); }, ms);
  return () => clearTimeout(timer);
}
