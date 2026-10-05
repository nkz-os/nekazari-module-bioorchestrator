/** Pure view-model of the "what to sow" page (no React, no I/O). */

import type {
  FrostLevel,
  Interval,
  Level,
  Recommendation,
  RecommendResponse,
  WaterLevel,
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

const fmtInt = (n: number | null) => (n == null ? null : String(Math.round(n)));
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

export function compareRows(recs: Recommendation[]): CompareRow[] {
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
      cells: expected.map(fmtInt), best: winners(expected) },
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
