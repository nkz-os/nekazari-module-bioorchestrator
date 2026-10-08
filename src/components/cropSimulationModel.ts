/** View-model helpers for the crop simulation answer (POST /graph/agriculture/crop-simulation). */

export type Irrigation = 'rainfed' | 'full';
export const ENGINES = ['aquacrop'] as const;
export type Engine = (typeof ENGINES)[number];

export interface Percentiles { p10: number; p50: number; p90: number }
export interface DailyPoint {
  day: string;
  canopy_cover: number | null;
  water_stress: number | null;
  projected: boolean;
}
export interface WeatherSegment { source: string; start: string; end: string; days: number }
export interface SoilLayer {
  top_cm: number; bottom_cm: number; wp: number; fc: number; sat: number; ksat_mm_day: number;
}
export interface InitialWater { method: 'spinup' | 'assumed_fc'; spinup_days: number; start?: string }

export interface CropSimulationResult {
  engine: string;
  engine_version: string;
  crop_slug: string;
  aquacrop_crop: string;
  parcel_id: string;
  sowing_date: string;
  irrigation: Irrigation;
  initial_water: InitialWater;
  status: 'complete' | 'in_season';
  yield_t_ha: number | Percentiles;
  potential_yield_t_ha: number | Percentiles;
  water_gap_pct: number | null;
  harvest_date: string;
  ensemble: { n_years: number; method: string; years: number[] } | null;
  daily: DailyPoint[];
  inputs: {
    weather: { source: string; start: string; end: string; days: number; segments: WeatherSegment[] };
    soil: { layers: SoilLayer[] };
    sowing: { source: string };
  };
  warnings: string[];
}

export const KNOWN_ERROR_CODES = [
  'unsupported_crop', 'weather_gaps', 'soil_incomplete', 'soil_unavailable', 'weather_unavailable',
  'archive_not_configured', 'archive_unavailable', 'sowing_date_future', 'parcel_not_found',
] as const;

/** i18n key of the localized hint for a backend error code (generic when unknown or missing). */
export function errorHintKey(code: string | null | undefined): string {
  return (KNOWN_ERROR_CODES as readonly string[]).includes(code ?? '')
    ? `cropSimulation.errors.${code}`
    : 'cropSimulation.errors.generic';
}

export const isPercentiles = (v: unknown): v is Percentiles =>
  !!v && typeof v === 'object' && 'p50' in (v as object);

const num = (v: number, digits: number) => v.toFixed(digits);

/** Headline value (P50 when a range) and the secondary "P10–P90" text (null for a single value). */
export function formatYield(v: number | Percentiles | null | undefined, digits = 1): { main: string; range: string | null } {
  if (v == null) return { main: '—', range: null };
  if (isPercentiles(v)) return { main: num(v.p50, digits), range: `${num(v.p10, digits)} – ${num(v.p90, digits)}` };
  return { main: num(v, digits), range: null };
}

/** i18n key for a weather segment source; unknown sources are shown raw by the caller. */
export const WEATHER_SOURCES = ['archive', 'parcel_daily'] as const;
export const weatherSourceKey = (source: string): string | null =>
  (WEATHER_SOURCES as readonly string[]).includes(source) ? `cropSimulation.weatherSource.${source}` : null;

export const SOWING_SOURCES = ['request', 'field_operations'] as const;
export const sowingSourceKey = (source: string): string | null =>
  (SOWING_SOURCES as readonly string[]).includes(source) ? `cropSimulation.sowingSource.${source}` : null;

/** "<source> · <start> → <end> (<days>)" with the source already localized by the caller. */
export function segmentLabel(seg: WeatherSegment, sourceLabel: string): string {
  return seg.start === seg.end
    ? `${sourceLabel} · ${seg.start} (${seg.days})`
    : `${sourceLabel} · ${seg.start} → ${seg.end} (${seg.days})`;
}

export const pct = (fraction: number | null | undefined, digits = 0): string =>
  fraction == null ? '—' : `${(fraction * 100).toFixed(digits)}%`;

export const depthLabel = (l: Pick<SoilLayer, 'top_cm' | 'bottom_cm'>): string => `${l.top_cm}–${l.bottom_cm} cm`;

export interface ChartLine { observed: [number, number][]; projected: [number, number][] }

const DAY_MS = 86_400_000;

/**
 * Splits one series into its observed and projected parts (x = days since the first day).
 * The projected part starts at the last observed point so the line is continuous; null values break nothing
 * (they are skipped).
 */
export function chartLine(daily: DailyPoint[], field: 'canopy_cover' | 'water_stress'): ChartLine {
  const out: ChartLine = { observed: [], projected: [] };
  if (daily.length === 0) return out;
  const t0 = Date.parse(daily[0].day);
  let last: [number, number] | null = null;
  for (const d of daily) {
    const v = d[field];
    if (v == null) continue;
    const pt: [number, number] = [Math.round((Date.parse(d.day) - t0) / DAY_MS), v];
    if (d.projected) {
      if (out.projected.length === 0 && last) out.projected.push(last);
      out.projected.push(pt);
    } else {
      out.observed.push(pt);
      last = pt;
    }
  }
  return out;
}
