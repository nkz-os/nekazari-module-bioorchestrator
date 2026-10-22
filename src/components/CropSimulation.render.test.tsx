import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import es from '../locales/es.json';

vi.mock('@nekazari/sdk', () => {
  const lookup = (key: string, opts?: Record<string, unknown>) => {
    const v = key.split('.').reduce<unknown>((n, p) => (n && typeof n === 'object' ? (n as Record<string, unknown>)[p] : undefined), es);
    if (typeof v !== 'string') return key;
    return v.replace(/\{\{(\w+)\}\}/g, (_m, k: string) => String(opts?.[k] ?? ''));
  };
  return { useTranslation: () => ({ t: lookup, i18n: { language: 'es' } }), i18n: {} };
});
vi.mock('../services/api', () => ({ useBioApi: () => ({}), ApiError: class extends Error {}, API_BASE: '', authHeaders: () => ({}) }));
vi.mock('../context/ParcelContext', () => ({ useParcelContext: () => ({}) }));
vi.mock('../context/PlanningScenarioContext', () => ({ usePlanningScenario: () => ({ enabled: false }) }));

import { CropSimulationResultView } from './CropSimulation';
import type { CropSimulationResult } from './cropSimulationModel';

const textOf = (html: string) => html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');

const NAVARRA = { scheme: 'es_province', code: '31', name: 'Navarra' };

const base: CropSimulationResult = {
  engine: 'aquacrop', engine_version: '7.2', crop_slug: 'wheat', aquacrop_crop: 'WheatGDD',
  parcel_id: 'urn:p', sowing_date: '2025-10-01', irrigation: 'rainfed',
  initial_water: { method: 'spinup', spinup_days: 364, start: '2024-10-02' },
  status: 'complete', yield_t_ha: 5.23, potential_yield_t_ha: 7.81, water_gap_pct: 33.0,
  harvest_date: '2026-06-20', ensemble: null,
  daily: [
    { day: '2025-10-01', canopy_cover: 0.05, water_stress: 0, projected: false },
    { day: '2025-10-02', canopy_cover: 0.1, water_stress: 0.1, projected: false },
  ],
  inputs: {
    weather: { source: 'archive+parcel_daily', start: '2024-10-01', end: '2026-06-20', days: 628, segments: [
      { source: 'archive', start: '2024-10-01', end: '2025-05-31', days: 243 },
      { source: 'parcel_daily', start: '2025-06-01', end: '2026-06-20', days: 385 },
    ] },
    soil: { layers: [{ top_cm: 0, bottom_cm: 30, wp: 0.12, fc: 0.28, sat: 0.43, ksat_mm_day: 120 }] },
    sowing: { source: 'field_operations', provenance: 'actual' },
    phenology: { source: 'calibrated', region: NAVARRA, params: { Maturity: 2809.1 } },
  },
  yield_basis: { moisture: 'commercial', grain_moisture_pct: 13, source: 'Lloveras et al. 2017' },
  expected_yield: {
    status: 'ok', basis: 'gap_corrected', range_t_ha: { low: 2.44, high: 4.06 }, coverage: 0.78,
    gap_factor: 0.51, region: NAVARRA,
    calibration: { version: '2026-10-11', source: 'MAPA yearbook', validation: { rmse_kg_ha: 1684, anomaly_r_median: 0.5 } },
  },
  warnings: ['Archive altitude differs from the parcel.'],
};

describe('CropSimulationResultView (es)', () => {
  it('shows the sowing method, its source and the mulch', () => {
    const result: CropSimulationResult = { ...base, inputs: { ...base.inputs, field_management:
      { sowing_method: 'direct', source: 'field_operations', mulch_pct: 30, f_mulch: 0.5 } } };
    const t = textOf(renderToStaticMarkup(<CropSimulationResultView result={result} />));
    expect(t).toContain('Método de siembra: directa (operaciones de campo) · rastrojo como acolchado al 30 %');
  });

  it('bare soil by default says there was no data', () => {
    const result: CropSimulationResult = { ...base, inputs: { ...base.inputs, field_management:
      { sowing_method: 'conventional', source: 'default', mulch_pct: null, f_mulch: null } } };
    const t = textOf(renderToStaticMarkup(<CropSimulationResultView result={result} />));
    expect(t).toContain('Método de siembra: tradicional (sin dato: suelo desnudo)');
    expect(t).not.toContain('acolchado');
  });

  it('an older result without field_management shows no method line', () => {
    const t = textOf(renderToStaticMarkup(<CropSimulationResultView result={base} />));
    expect(t).not.toContain('Método de siembra');
  });

  it('complete season: single values, no ensemble text', () => {
    const t = textOf(renderToStaticMarkup(<CropSimulationResultView result={base} />));
    expect(t).toContain('Campaña completa');
    expect(t).toContain('aquacrop 7.2');
    expect(t).toContain('wheat → WheatGDD');
    expect(t).toContain('5.2 t/ha');
    expect(t).toContain('2025-05-31 (243)');
    expect(t).toContain('0–30 cm');
    expect(t).toContain('Archive altitude differs from the parcel.');
    expect(t).toContain('precalentamiento de 364 días');
    expect(t).not.toContain('Basado en');
    expect(t).not.toContain('2.0 – 4.9');
    expect(t).toContain('Techo con el agua de este año');
    expect(t).toContain('Esperado en tu zona (Navarra)');
    expect(t).toContain('2.4 – 4.1 t/ha');
    expect(t).toContain('el 78 %');
    expect(t).toContain('humedad comercial (13 %)');
    expect(t).toContain('calibrada para Navarra');
  });

  it('preseason: before-sowing badge, ensemble basis and median harvest', () => {
    const r: CropSimulationResult = {
      ...base, status: 'preseason',
      yield_t_ha: { p10: 2.5, p50: 4.1, p90: 5.6 },
      potential_yield_t_ha: { p10: 6.0, p50: 7.0, p90: 8.0 },
      ensemble: { n_years: 5, method: 'climatological_ensemble', years: [2021, 2022, 2023, 2024, 2025] },
      daily: base.daily.map((d) => ({ ...d, projected: true })),
    };
    const t = textOf(renderToStaticMarkup(<CropSimulationResultView result={r} />));
    expect(t).toContain('Antes de sembrar');
    expect(t).toContain('Basado en 5 campañas pasadas');
    expect(t).toContain('Fecha de cosecha mediana');
    expect(t).not.toContain('Campaña completa');
  });

  it('unavailable expected yield says why; a negative water gap shows 0%', () => {
    const r: CropSimulationResult = {
      ...base, irrigation: 'full', water_gap_pct: -1.4,
      expected_yield: { status: 'unavailable', reason: 'irrigated' },
    };
    const t = textOf(renderToStaticMarkup(<CropSimulationResultView result={r} />));
    expect(t).toContain('no disponible');
    expect(t).toContain('solo está calibrado el secano');
    expect(t).toContain('0%');
    expect(t).not.toContain('-1%');
  });

  it('in-season: P50 headline, P10-P90 range, ensemble basis, assumed_fc note', () => {
    const r: CropSimulationResult = {
      ...base, status: 'in_season',
      yield_t_ha: { p10: 2.04, p50: 3.46, p90: 4.9 },
      potential_yield_t_ha: { p10: 6.1, p50: 7, p90: 7.9 },
      ensemble: { n_years: 3, method: 'climatological_ensemble', years: [2021, 2022, 2023] },
      initial_water: { method: 'assumed_fc', spinup_days: 0 },
      daily: [...base.daily, { day: '2025-10-03', canopy_cover: 0.2, water_stress: 0.2, projected: true },
        { day: '2025-10-04', canopy_cover: 0.3, water_stress: 0.2, projected: true }],
    };
    const html = renderToStaticMarkup(<CropSimulationResultView result={r} />);
    const t = textOf(html);
    expect(t).toContain('Campaña en curso');
    expect(t).toContain('3.5 t/ha');
    expect(t).toContain('2.0 – 4.9 t/ha');
    expect(t).toContain('Basado en 3 campañas pasadas (2021, 2022, 2023).');
    expect(t).toContain('Fecha de cosecha mediana');
    expect(t).toContain('supuesta a capacidad de campo');
    expect(html).toContain('stroke-dasharray="5,3"');
  });
});
