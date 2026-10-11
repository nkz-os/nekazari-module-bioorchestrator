import { cropName, monthName, safeLang } from './format';
import type { Names, ParamValue, Reason, Sowing, SpeciesFit, TFn, Verdict } from './types';

const BLOCKERS = new Set(['soil_unsuitable', 'soil_ph_unsuitable', 'soil_texture_unsuitable', 'soil_drainage_unsuitable', 'frost_risk']);
const INFORMATIVE = new Set(['rotation_benefit', 'multi_year']);

/**
 * Every reason i18n key the backend can produce (contract 1.0). The locale check arrives with Task 10
 * (keys.test.ts); until then nothing enforces this list against the locale files.
 */
export const REASON_KEYS = [
  'soil_unsuitable', 'soil_marginal', 'soil_ph_unsuitable', 'soil_ph_marginal', 'soil_texture_unsuitable', 'soil_texture_marginal',
  'soil_drainage_unsuitable', 'soil_drainage_marginal', 'soil_unknown_no_tolerance', 'soil_unknown_no_parcel_soil',
  'soil_unknown_not_comparable', 'frost_risk', 'frost_unknown_no_climate', 'frost_unknown_no_tolerance',
  'frost_unknown_season_unknown', 'water_deficit_high', 'water_unknown', 'rotation_restriction', 'rotation_benefit',
  'rotation_unknown', 'regime_missing', 'multi_year', 'climate_unknown',
] as const;

export function groupFit(species: SpeciesFit[], lang: string): Record<Verdict, SpeciesFit[]> {
  const groups: Record<Verdict, SpeciesFit[]> = { fits: [], caveats: [], no_fit: [] };
  // An unknown verdict is a caveat at most: it is never shown as a fit, and it never throws.
  for (const s of species) groups[s.verdict === 'fits' || s.verdict === 'no_fit' ? s.verdict : 'caveats'].push(s);
  const tag = safeLang(lang);
  const byName = (a: SpeciesFit, b: SpeciesFit) => cropName(a.names, a.species, tag).localeCompare(cropName(b.names, b.species, tag), tag);
  for (const key of Object.keys(groups) as Verdict[]) groups[key].sort(byName);
  return groups;
}

export function speciesIndex(species: SpeciesFit[]): Record<string, Names> {
  return Object.fromEntries(species.map((s) => [s.species, s.names]));
}

const sameParams = (a: Reason['params'], b: Reason['params']): boolean => {
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every((k) => k in b && a[k] === b[k]);
};
const sameReason = (a: Reason, b: Reason): boolean => a.code === b.code && sameParams(a.params, b.params);

/**
 * Reasons that every crop carries (the backend repeats the parcel-wide ones, such as a missing regime
 * or soil, on all of them): reported once in `shared`, and removed from each crop's own list. With
 * fewer than two crops nothing is shared. Reasons are the same when code and params match by value.
 */
export function splitSharedReasons(species: SpeciesFit[]): { shared: Reason[]; perSpecies: Map<string, Reason[]> } {
  const shared: Reason[] = [];
  if (species.length >= 2) {
    for (const r of species[0].reasons) {
      if (!shared.some((x) => sameReason(x, r)) && species.every((s) => s.reasons.some((x) => sameReason(x, r)))) shared.push(r);
    }
  }
  const perSpecies = new Map<string, Reason[]>();
  for (const s of species) perSpecies.set(s.species, s.reasons.filter((r) => !shared.some((x) => sameReason(x, r))));
  return { shared, perSpecies };
}

export function reasonKey(r: Reason): string {
  const why = typeof r.params.why === 'string' ? `_${r.params.why}` : '';
  return `planning.reason.${r.code}${why}`;
}

export function reasonTone(r: Reason): 'positive' | 'info' | 'warning' | 'negative' {
  if (r.code === 'rotation_benefit') return 'positive';
  if (INFORMATIVE.has(r.code)) return 'info';
  return BLOCKERS.has(r.code) ? 'negative' : 'warning';
}

export function reasonText(r: Reason, names: Record<string, Names>, lang: string, t: TFn): string {
  const params: Record<string, ParamValue> = { ...r.params };
  if (typeof params.previous === 'string') params.previous = cropName(names[params.previous], params.previous, lang);
  // The backend sends USDA texture classes raw ("sandy-loam"); an unknown class falls back to the raw value.
  if (r.code.startsWith('soil_texture_') && typeof params.value === 'string') {
    const raw = params.value;
    params.value = t(`planning.texture.${raw}`, { defaultValue: raw });
  }
  return t(reasonKey(r), params);
}

export function sowingText(s: Sowing, lang: string, t: TFn): string | null {
  if (s.start_month && s.end_month) {
    return t('planning.fit.sowing', { from: monthName(s.start_month, lang), to: monthName(s.end_month, lang) });
  }
  return null;
}
