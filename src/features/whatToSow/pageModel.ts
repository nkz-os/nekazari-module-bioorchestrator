/** Pure helpers of the "what to sow" page: filters → query params, evidence params, formatting. */

import type { QueryParams } from '../../services/recommendApi';
import type {
  EvidenceTier, ParcelEnvironment, Purpose, Recommendation, RecommendResponse, Similarity, SowingType,
} from '../../types/recommend';

export const KOPPEN_CODES = [
  'Af', 'Am', 'Aw', 'BWh', 'BWk', 'BSh', 'BSk', 'Csa', 'Csb', 'Csc', 'Cwa', 'Cwb',
  'Cfa', 'Cfb', 'Cfc', 'Dfa', 'Dfb', 'Dfc', 'Dsa', 'Dsb', 'Dwa', 'Dwb', 'ET',
] as const;

export const SEASONS = ['all', 'autumn', 'spring', 'summer'] as const;
export const MANAGEMENTS = ['any', 'conventional', 'organic'] as const;
export const IRRIGATIONS = ['inferred', 'secano', 'regadío'] as const;
/** "Destino": the crop's main harvested product, or forage only. */
export const PURPOSES: readonly Purpose[] = ['main', 'forage'];
/** The backend's default: sent as no parameter at all, so an older backend is asked exactly what it always was. */
export const DEFAULT_PURPOSE: Purpose = 'main';

export type SeasonFilter = (typeof SEASONS)[number];
export type ManagementFilter = (typeof MANAGEMENTS)[number];
export type IrrigationFilter = (typeof IRRIGATIONS)[number];

/** "Detected" only means something for a parcel; in explore mode no option means "any". */
export function irrigationOptions(hasParcel: boolean): readonly IrrigationFilter[] {
  return hasParcel ? IRRIGATIONS : IRRIGATIONS.filter((v) => v !== 'inferred');
}

/** In explore mode the active option toggles off, back to no irrigation filter. */
export function pickIrrigation(
  current: IrrigationFilter, picked: IrrigationFilter, hasParcel: boolean,
): IrrigationFilter {
  return !hasParcel && picked === current ? 'inferred' : picked;
}

export interface Filters {
  season: SeasonFilter;
  management: ManagementFilter;
  irrigation: IrrigationFilter;
  /** `main` = harvested product (grain, fruit, tuber…); `forage` = forage records only. */
  purpose: Purpose;
  /** Raw text of the expert frost-margin input; empty = server default. */
  frostMargin: string;
  /** Köppen override (parcel) or picked class (explore); empty = none. */
  climateClass: string;
}

export const DEFAULT_FILTERS: Filters = {
  season: 'all', management: 'any', irrigation: 'inferred', purpose: DEFAULT_PURPOSE, frostMargin: '', climateClass: '',
};

export const TOP_N_REQUEST = 15;
export const MAX_COMPARE = 4;
export const FEW_TRIALS = 5;
export const EVIDENCE_PAGE_SIZE = 20;

const FROST_MIN = 0;
const FROST_MAX = 15;

export function parseFrostMargin(raw: string): number | undefined {
  const text = raw.trim().replace(',', '.');
  if (text === '') return undefined;
  const n = Number(text);
  if (!Number.isFinite(n) || n < FROST_MIN || n > FROST_MAX) return undefined;
  return n;
}

export const FROST_DEBOUNCE_MS = 500;

/**
 * The backend that computed `res` applies the evidence policy: it states the policy version. An
 * older backend ignores `purpose` and returns neither tiers nor forage counts, so the Destino chip,
 * the forage notice and the regional section must not appear for its answers (they would promise a
 * mode that is not in effect).
 */
export function carriesEvidencePolicy(res: RecommendResponse | null | undefined): boolean {
  return res?.status === 'ok' && typeof res.evidence_policy === 'string' && res.evidence_policy !== '';
}

/**
 * Whether the backend applies the evidence policy, as last observed. Only an `ok` answer can tell,
 * so a `needs_climate` one keeps the previous value (the chip does not flicker while it loads).
 */
export function nextPolicyAware(prev: boolean, res: RecommendResponse): boolean {
  return res.status === 'ok' ? carriesEvidencePolicy(res) : prev;
}

/** Filters as requested: against a backend not known to apply the policy there is only the default Destino. */
export function effectiveFilters(f: Filters, policyAware: boolean): Filters {
  return policyAware || f.purpose === DEFAULT_PURPOSE ? f : { ...f, purpose: DEFAULT_PURPOSE };
}

/** Whether the raw frost-margin text may be committed: empty (server default) or a value in 0–15. */
export function frostMarginStatus(raw: string): 'empty' | 'valid' | 'invalid' {
  if (raw.trim() === '') return 'empty';
  return parseFrostMargin(raw) === undefined ? 'invalid' : 'valid';
}

function baseQuery(f: Filters, expert: boolean): QueryParams {
  const q: QueryParams = { top_n: TOP_N_REQUEST, season: f.season, management: f.management };
  if (f.purpose && f.purpose !== DEFAULT_PURPOSE) q.purpose = f.purpose;
  if (f.irrigation !== 'inferred') q.irrigation_regime = f.irrigation;
  if (expert) {
    const margin = parseFrostMargin(f.frostMargin);
    if (margin !== undefined) q.frost_margin_c = margin;
  }
  if (f.climateClass) q.climate_class = f.climateClass;
  return q;
}

/** Params of `recommend/parcel/{id}`; overrides are only present when set. */
export function parcelQuery(f: Filters, expert: boolean): QueryParams {
  return baseQuery(f, expert);
}

/** Params of the conditions endpoint; null until a climate class is picked (it is required). */
export function conditionsQuery(f: Filters, expert: boolean): QueryParams | null {
  return f.climateClass ? baseQuery(f, expert) : null;
}

const CLIMATE_KEYS = ['annual_rainfall_mm', 'annual_et0_mm', 'coldest_month_min_c', 'annual_temp_c'] as const;
const IRRIGATION_VALUES = new Set(['secano', 'regadío']);

const nonEmptyString = (v: unknown): string | undefined =>
  typeof v === 'string' && v.trim() !== '' ? v : undefined;
const finiteNumber = (v: unknown): number | undefined =>
  typeof v === 'number' && Number.isFinite(v) ? v : undefined;

/**
 * Evidence conditions matching the recommendation: the resolved class, soil type and irrigation
 * echoed by the API, plus (for v2 similarity) the numeric climate inputs — taken from the parcel's
 * climate_detail, falling back to the echoed numbers on the conditions route. `purpose` is the
 * echoed one; `tier` (and the echoed ISO `country`) is sent only for regional recommendations (the evidence endpoint defaults to field).
 */
export function evidenceConditions(
  echo: Record<string, unknown> | null | undefined,
  environment: ParcelEnvironment | null | undefined,
  similarity: Similarity,
  tier: EvidenceTier = 'field',
  zoneId?: string | null,
): QueryParams {
  const src = echo ?? {};
  const out: QueryParams = {};
  // The trials listed must be those of the answer: same purpose (forage yields are not grain yields)
  // and, for a regional recommendation, the aggregate-site tier.
  if (src.purpose === 'main' || src.purpose === 'forage') out.purpose = src.purpose;
  if (tier === 'regional') {
    out.tier = tier;
    // The regional aggregates are matched by the parcel's country: the list must use the same one.
    const country = nonEmptyString(src.country);
    if (country && /^[A-Z]{2}$/.test(country)) out.country = country;
    // ... and the opaque zone id the recommendation matched (never a coordinate).
    if (zoneId && /^-?\d{1,2}(\.\d)?_\d{1,4}$/.test(zoneId)) out.zone = zoneId;
  }
  const cls = nonEmptyString(src.climate_class);
  if (cls) out.climate_class = cls;
  const soil = nonEmptyString(src.soil_type);
  if (soil) out.soil_type = soil;
  const irr = nonEmptyString(src.irrigation_regime);
  if (irr && IRRIGATION_VALUES.has(irr)) out.irrigation_regime = irr;
  if (similarity === 'vector_v2_fallback') {
    const detail = environment?.climate_detail ?? {};
    for (const key of CLIMATE_KEYS) {
      const v = finiteNumber(detail[key]) ?? finiteNumber(src[key]);
      if (v !== undefined) out[key] = v;
    }
  }
  return out;
}

export function toggleCompare(selected: string[], id: string): string[] {
  if (selected.includes(id)) return selected.filter((s) => s !== id);
  if (selected.length >= MAX_COMPARE) return selected;
  return [...selected, id];
}

export function seasonKey(sowing: SowingType | null | undefined): string {
  return `whatToSow.season.${sowing ?? 'unknown'}`;
}

/** null, empty and the API's "unknown" all mean missing data. */
export function knownText(v: string | null | undefined): string | null {
  if (v == null) return null;
  const s = v.trim();
  return s === '' || s === 'unknown' ? null : s;
}

export function isFewTrials(n: number): boolean {
  return n < FEW_TRIALS;
}

export function formatAssumptionValue(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

/** Shared scale for every range bar on the page, so bars are comparable between crops. */
export function rangeScaleMax(recs: Recommendation[]): number | null {
  let max: number | null = null;
  for (const r of recs) {
    for (const v of [r.yield.interval?.[1], r.yield.expected_kg_ha]) {
      if (v != null && Number.isFinite(v) && (max == null || v > max)) max = v;
    }
  }
  return max;
}

/** Minimal translator the scope formatter needs (i18next's `t` fits). */
export type ScopeTranslate = (key: string, options?: Record<string, unknown>) => string;

const REGIME_IDS: Record<string, string> = { secano: 'secano', regadio: 'regadio', 'regadío': 'regadio', any: 'any' };

/**
 * Human-readable `fit.reference.scope`: `analog_sites:Csa:secano` → "sitios análogos Csa · secano".
 * Climate is a Köppen class, `vector_v2` (sites picked by climate similarity) or `any`; regime is
 * secano | regadio | any; a trailing `:forage` marks forage mode. `regional` has no reference
 * median. Anything unrecognised is returned as is, never invented.
 */
export function describeReferenceScope(scope: string, t: ScopeTranslate): string {
  if (scope === 'regional') return t('whatToSow.scope.regional');
  const parts = scope.split(':');
  if (parts[0] !== 'analog_sites' || (parts.length !== 3 && parts.length !== 4)) return scope;
  const [, climate, regime, tail] = parts;
  const regimeId = REGIME_IDS[regime];
  if (!climate || !regimeId || (tail !== undefined && tail !== 'forage')) return scope;
  const sites = climate === 'any' ? t('whatToSow.scope.analogAny')
    : climate === 'vector_v2' ? t('whatToSow.scope.analogVector')
      : t('whatToSow.scope.analog', { climate });
  const out = [sites, t(`whatToSow.scope.regime.${regimeId}`)];
  if (tail === 'forage') out.push(t('whatToSow.scope.forage'));
  return out.join(' · ');
}
