import { describe, expect, it } from 'vitest';
import {
  chartLine, depthLabel, errorHintKey, formatYield, pct, segmentLabel, sowingSourceKey, weatherSourceKey,
  type DailyPoint,
} from './cropSimulationModel';

describe('formatYield', () => {
  it('shows P50 as headline and P10-P90 as range', () => {
    expect(formatYield({ p10: 2.04, p50: 3.46, p90: 4.9 })).toEqual({ main: '3.5', range: '2.0 – 4.9' });
  });
  it('shows a single value without range', () => {
    expect(formatYield(5.234)).toEqual({ main: '5.2', range: null });
  });
  it('handles missing', () => {
    expect(formatYield(null)).toEqual({ main: '—', range: null });
  });
});

describe('segmentLabel', () => {
  it('formats a range and a single day', () => {
    expect(segmentLabel({ source: 'archive', start: '2025-10-01', end: '2026-05-31', days: 242 }, 'Archive'))
      .toBe('Archive · 2025-10-01 → 2026-05-31 (242)');
    expect(segmentLabel({ source: 'archive', start: '2026-01-01', end: '2026-01-01', days: 1 }, 'A'))
      .toBe('A · 2026-01-01 (1)');
  });
  it('maps known sources to keys', () => {
    expect(weatherSourceKey('parcel_daily')).toBe('cropSimulation.weatherSource.parcel_daily');
    expect(weatherSourceKey('other')).toBeNull();
    expect(sowingSourceKey('field_operations')).toBe('cropSimulation.sowingSource.field_operations');
    expect(sowingSourceKey('x')).toBeNull();
  });
});

describe('errorHintKey', () => {
  it('maps known codes and falls back', () => {
    expect(errorHintKey('weather_gaps')).toBe('cropSimulation.errors.weather_gaps');
    expect(errorHintKey('parcel_not_found')).toBe('cropSimulation.errors.parcel_not_found');
    expect(errorHintKey('nope')).toBe('cropSimulation.errors.generic');
    expect(errorHintKey(null)).toBe('cropSimulation.errors.generic');
  });
});

describe('misc formatting', () => {
  it('formats percent and depth', () => {
    expect(pct(0.256)).toBe('26%');
    expect(pct(null)).toBe('—');
    expect(depthLabel({ top_cm: 0, bottom_cm: 30 })).toBe('0–30 cm');
  });
});

describe('chartLine', () => {
  const d = (day: string, v: number | null, projected: boolean): DailyPoint =>
    ({ day, canopy_cover: v, water_stress: v, projected });
  it('splits observed/projected, joined at the last observed point, skipping nulls', () => {
    const daily = [d('2026-04-01', 0.1, false), d('2026-04-02', null, false), d('2026-04-03', 0.3, false),
      d('2026-04-04', 0.4, true), d('2026-04-05', 0.5, true)];
    const l = chartLine(daily, 'canopy_cover');
    expect(l.observed).toEqual([[0, 0.1], [2, 0.3]]);
    expect(l.projected).toEqual([[2, 0.3], [3, 0.4], [4, 0.5]]);
  });
  it('is empty for no data', () => {
    expect(chartLine([], 'water_stress')).toEqual({ observed: [], projected: [] });
  });
});
