/** Pure view-model of the "what to sow" page (no React, no I/O). */

import type {
  FrostLevel,
  Interval,
  Level,
  Recommendation,
  RecommendResponse,
  WaterLevel,
  YieldBasis,
} from '../../types/recommend';

export const TOP_N = 3;
export const MIN_TOP_TRIALS = 3;

export function partitionRecommendations(recs: Recommendation[]): {
  top: Recommendation[];
  more: Recommendation[];
} {
  const top: Recommendation[] = [];
  const more: Recommendation[] = [];
  for (const rec of recs) {
    if (top.length < TOP_N && rec.yield.n_trials >= MIN_TOP_TRIALS) top.push(rec);
    else more.push(rec);
  }
  return { top, more };
}

/** Regional/national evidence only: numbers from aggregate registries, not from field trials. */
export const isRegionalRec = (rec: Recommendation): boolean => rec.evidence.tier === 'regional';

/**
 * Field recommendations (the cards and the "more" list) and regional ones (their own section),
 * each in API order. A recommendation without a tier (older backend) is a field one.
 */
export function partitionByTier(recs: Recommendation[]): { field: Recommendation[]; regional: Recommendation[] } {
  const field: Recommendation[] = [];
  const regional: Recommendation[] = [];
  for (const rec of recs) (isRegionalRec(rec) ? regional : field).push(rec);
  return { field, regional };
}

/**
 * Whether the evidence page can list trials behind a recommendation. It lists only trials that
 * carry a number in the answer, so two states have nothing to list: presence-only crops (records
 * of an excluded source) and forage crops "not comparable" (no known basis, hence no number).
 */
export const hasListableTrials = (rec: Recommendation): boolean => {
  const status = yieldStatus(rec);
  return status !== 'no_measured' && status !== 'not_comparable';
};

/**
 * Forage trials of the crop at the same analog sites that the harvest-mode numbers leave out
 * (counted, never averaged). Null when there are none, in forage mode, or on a regional
 * recommendation (the backend only counts them for field recommendations).
 */
export function forageNoticeCount(rec: Recommendation): number | null {
  if (rec.evidence.purpose === 'forage' || rec.evidence.tier === 'regional') return null;
  const n = rec.evidence.other_purpose_trials?.forage;
  return typeof n === 'number' && Number.isFinite(n) && n > 0 ? n : null;
}

/** Unit family of a recommendation's yield numbers: plain kg/ha, or forage per its basis (shown in tonnes). */
export type YieldUnit = 'kg_ha' | YieldBasis;

export function yieldUnit(basis: YieldBasis | null | undefined): YieldUnit {
  return basis === 'dry_matter' || basis === 'fresh_matter' ? basis : 'kg_ha';
}

/**
 * Unit of a recommendation's numbers. Forage-mode numbers carry their basis in `yield.basis`; a
 * forage answer without one holds only dry-matter-normalised values (known-basis rows), never a guess
 * from magnitude.
 */
export function recYieldUnit(rec: Recommendation): YieldUnit {
  return yieldUnit(rec.yield.basis ?? (rec.evidence.purpose === 'forage' ? 'dry_matter' : null));
}

/** Common unit of a set of recommendations (all share the answer's purpose). */
export function recsYieldUnit(recs: Recommendation[]): YieldUnit {
  return recs.length > 0 ? recYieldUnit(recs.find((r) => r.yield.basis) ?? recs[0]) : 'kg_ha';
}

/** i18n key of the unit label as displayed (kg/ha, or t MS/ha for forage). */
export const unitKey = (unit: YieldUnit): string => `whatToSow.unit.${unit}`;

/** i18n key of the unit label for values left in kg (the trial table, the assign dialog). */
export const kgUnitKey = (unit: YieldUnit): string =>
  unit === 'kg_ha' ? 'whatToSow.unit.kg_ha' : `whatToSow.unit.kg_${unit}`;

/** A kg/ha value in the displayed unit: forage is shown in tonnes. Null stays null. */
export function yieldValue(kg: number | null | undefined, unit: YieldUnit): number | null {
  if (kg == null || !Number.isFinite(kg)) return null;
  return unit === 'kg_ha' ? kg : kg / 1000;
}

/** Localised yield in the displayed unit (no unit text): whole kg, or tonnes with one decimal. */
export function formatYield(
  kg: number | null | undefined, unit: YieldUnit, locale: string, tonneDigits = 1,
): string | null {
  const v = yieldValue(kg, unit);
  if (v == null) return null;
  return v.toLocaleString(locale, { maximumFractionDigits: unit === 'kg_ha' ? 0 : tonneDigits });
}

/**
 * What the yield line of a recommendation can say:
 * - `measured`: there is a number;
 * - `not_comparable`: forage trials exist but none has a known dry/fresh basis (no number, never a guess);
 * - `no_measured`: only trials of an excluded source (presence), no yield value;
 * - `none`: no data.
 */
export type YieldStatus = 'measured' | 'not_comparable' | 'no_measured' | 'none';

export function yieldStatus(rec: Recommendation): YieldStatus {
  if (rec.yield.expected_kg_ha != null) return 'measured';
  const gaps = rec.trust.data_gaps;
  if (gaps.includes('forage_basis_unknown')) return 'not_comparable';
  if (gaps.includes('no_measured_yield')) return 'no_measured';
  return 'none';
}

export interface RangeBar {
  leftPct: number;
  widthPct: number;
  markerPct: number;
}

const clamp = (n: number) => Math.min(100, Math.max(0, n));

export function rangeBar(
  interval: Interval | null | undefined,
  expected: number | null | undefined,
  scaleMax: number | null | undefined,
): RangeBar | null {
  if (!interval || interval[0] == null || interval[1] == null || expected == null) return null;
  if (scaleMax == null || !(scaleMax > 0)) return null;
  const left = clamp((interval[0] / scaleMax) * 100);
  const right = clamp((interval[1] / scaleMax) * 100);
  return {
    leftPct: left,
    widthPct: Math.max(0, right - left),
    markerPct: clamp((expected / scaleMax) * 100),
  };
}

/**
 * Soil warning to show on a card. Only for marginal/unsuitable soil: for "unknown" the backend
 * puts its operational reason in `warnings`, and the badge already says "no data".
 */
export function soilWarning(rec: Recommendation): string | null {
  const { level, warnings } = rec.suitability.soil;
  if (level !== 'marginal' && level !== 'unsuitable') return null;
  return warnings[0] ?? null;
}

export type Intent = 'positive' | 'warning' | 'negative' | 'default';
export type LevelKind = 'soil' | 'water' | 'frost';

const INTENTS: Record<LevelKind, Record<string, Intent>> = {
  soil: { suitable: 'positive', marginal: 'warning', unsuitable: 'negative' },
  water: { low: 'positive', medium: 'warning', high: 'negative' },
  frost: { none: 'positive', risk: 'negative' },
};

export function levelKey(
  kind: LevelKind,
  level: Level | WaterLevel | FrostLevel | null | undefined,
): { key: string; intent: Intent } {
  const known = level != null ? INTENTS[kind][level] : undefined;
  return {
    key: `whatToSow.level.${kind}.${known ? level : 'unknown'}`,
    intent: known ?? 'default',
  };
}

export interface CompareRow {
  id: string;
  labelKey: string;
  cells: (string | null)[];
  best: number[];
}

/** Yield cell: whole kg/ha, or tonnes with one decimal for forage. */
const fmtYieldCell = (n: number | null, unit: YieldUnit) =>
  n == null ? null : unit === 'kg_ha' ? String(Math.round(n)) : String(Math.round(n / 100) / 10);
const fmtSigned = (n: number | null) => (n == null ? null : `${n > 0 ? '+' : ''}${n}`);
const fmtCv = (n: number | null) => (n == null ? null : n.toFixed(2));

/** Indexes holding the highest score; null scores are ignored, ties all win. */
function winners(scores: (number | null)[]): number[] {
  const valid = scores.filter((s): s is number => s != null);
  if (valid.length === 0) return [];
  const max = Math.max(...valid);
  return scores.flatMap((s, i) => (s === max ? [i] : []));
}

const known = <T extends string>(level: T): T | null => ((level as string) === 'unknown' ? null : level);

export function compareRows(recs: Recommendation[], unit: YieldUnit = 'kg_ha'): CompareRow[] {
  const WATER: Record<string, number> = { low: 3, medium: 2, high: 1 };
  const SOIL: Record<string, number> = { suitable: 3, marginal: 2, unsuitable: 1 };
  const FROST: Record<string, number> = { none: 2, risk: 1 };
  const TRUST: Record<string, number> = { high: 3, medium: 2, low: 1 };
  const byMap = (map: Record<string, number>, v: string | null) => (v == null ? null : map[v] ?? null);

  const expected = recs.map((r) => r.yield.expected_kg_ha);
  const relative = recs.map((r) => r.fit.relative_yield_pct);
  const cv = recs.map((r) => r.fit.stability_cv);
  const water = recs.map((r) => known(r.suitability.water.level));
  const soil = recs.map((r) => known(r.suitability.soil.level));
  const frost = recs.map((r) => known(r.suitability.frost.level));
  const trust = recs.map((r) => r.trust.level);

  const rows: CompareRow[] = [
    { id: 'expectedYield', labelKey: 'whatToSow.compare.expectedYield',
      cells: expected.map((n) => fmtYieldCell(n, unit)), best: winners(expected) },
    { id: 'relativeYield', labelKey: 'whatToSow.compare.relativeYield',
      cells: relative.map(fmtSigned), best: winners(relative) },
    { id: 'stability', labelKey: 'whatToSow.compare.stability',
      cells: cv.map(fmtCv), best: winners(cv.map((v) => (v == null ? null : -v))) },
    { id: 'water', labelKey: 'whatToSow.compare.water',
      cells: water, best: winners(water.map((v) => byMap(WATER, v))) },
    { id: 'soil', labelKey: 'whatToSow.compare.soil',
      cells: soil, best: winners(soil.map((v) => byMap(SOIL, v))) },
    { id: 'frost', labelKey: 'whatToSow.compare.frost',
      cells: frost, best: winners(frost.map((v) => byMap(FROST, v))) },
    { id: 'trust', labelKey: 'whatToSow.compare.trust',
      cells: trust, best: winners(trust.map((v) => byMap(TRUST, v))) },
  ];
  // A star on every column says nothing: drop it when all columns tie.
  return rows.map((row) => (recs.length > 1 && row.best.length === recs.length ? { ...row, best: [] } : row));
}

export interface GrossIncome {
  value: number | null;
  low: number | null;
  high: number | null;
}

/** kg/ha → €/ha at a price in €/t. Null without a usable price. */
export function grossIncome(
  expectedKgHa: number | null | undefined,
  interval: Interval | null | undefined,
  pricePerT: number | null | undefined,
): GrossIncome | null {
  if (pricePerT == null || !Number.isFinite(pricePerT) || pricePerT <= 0) return null;
  const eur = (kg: number | null | undefined) => (kg == null ? null : (kg / 1000) * pricePerT);
  return { value: eur(expectedKgHa), low: eur(interval?.[0]), high: eur(interval?.[1]) };
}

export function lensAvailability(
  recs: Recommendation[],
  prices: Record<string, number | null | undefined>,
): { fit: true; calendar: boolean; rotation: true; euros: boolean } {
  return {
    fit: true,
    calendar: recs.some((r) => r.season.sowing_window != null || r.season.typical_sowing_doy != null),
    rotation: true,
    euros: recs.some((r) => {
      const p = prices[r.crop.eppo];
      return p != null && p > 0;
    }),
  };
}

export type PageState = 'loading' | 'error' | 'needs_climate' | 'empty' | 'ok';

export function resolvePageState(
  loading: boolean,
  error: unknown,
  response: RecommendResponse | null | undefined,
): PageState {
  if (loading) return 'loading';
  if (error) return 'error';
  if (!response) return 'loading';
  if (response.status === 'needs_climate') return 'needs_climate';
  return response.recommendations.length === 0 ? 'empty' : 'ok';
}
