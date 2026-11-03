import { cropName, safeLang } from './format';
import type { RegionalYield, TFn } from './types';

export const REGIONAL_UNAVAILABLE_REASONS = [
  'region_lookup_unavailable', 'crop_not_in_statistics', 'region_series_short', 'national_series_short', 'region_stats_unavailable',
] as const;

export const num = (value: number, lang: string, digits = 1, fixed = true): string =>
  new Intl.NumberFormat(safeLang(lang), { maximumFractionDigits: digits, minimumFractionDigits: fixed ? digits : 0 }).format(value);

/** «Region: 4.1 t/ha (3.2–4.9) · 112 % of Spain»; without the national index the last part is left out. */
export function regionalLine(r: RegionalYield, lang: string, t: TFn): string {
  const { p10, p50, p90 } = r.percentiles;
  const params = { p50: num(p50, lang), p10: num(p10, lang), p90: num(p90, lang) };
  if (r.relative_index == null) return t('planning.wizard.crop.regional.lineNoIndex', params);
  return t('planning.wizard.crop.regional.line', { ...params, index: num(r.relative_index * 100, lang, 0) });
}

/** Short human message for a missing figure; an unknown reason reads as the generic one. */
export function regionalUnavailableKey(reason: string | null | undefined): string {
  const known = (REGIONAL_UNAVAILABLE_REASONS as readonly string[]).includes(reason ?? '');
  return `planning.wizard.crop.regional.unavailable.${known ? reason : 'region_stats_unavailable'}`;
}

/** Region name in the user's language, the English label (Eurostat's) next, the NUTS code last. */
export const regionalName = (r: RegionalYield, lang: string): string => cropName(r.region_name ?? undefined, r.nuts, lang);
