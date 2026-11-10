import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import ParcelHistorySection from './ParcelHistorySection';
import { CycleLine } from './PlanMatrix';
import type { CycleCell, ParcelRow, SpeciesFit } from './types';

const SPECIES: SpeciesFit[] = [
  {
    species: 'wheat',
    eppo: 'TRZAX',
    names: { es: 'trigo blando' },
    lifecycle: 'annual',
    verdict: 'fits',
    reasons: [],
    sowing: { sowing_type: null, start_month: 10, end_month: 11, typical_sowing_doy: null, source: null },
    has_variety_trials: true,
    estimate_providers: ['trial_evidence'],
  },
];

const cell = (over: Partial<CycleCell>): CycleCell => ({
  crop_id: 'c1',
  species: 'wheat',
  species_raw: 'TRZAX',
  names: { es: 'trigo blando' },
  variety: 'Rimbaud',
  role: 'main_crop',
  provenance: 'declared',
  status: 'harvested',
  start: null,
  end: null,
  lifecycle: 'annual',
  first_campaign: null,
  yield_kg_ha: 4200,
  yield_source: 'declared',
  ...over,
});

const row = (cells: Record<string, CycleCell[]>): ParcelRow => ({
  parcel_id: 'urn:ngsi-ld:AgriParcel:t:p1',
  name: 'Soto Alto',
  area_ha: 10,
  farm_id: null,
  regime: 'rainfed',
  regime_source: 'declared',
  cycles_available: true,
  cells,
  warnings: [],
});

describe('ParcelHistorySection', () => {
  it('renders one row per declarable campaign the backend sends, including the current year after the July rollover', () => {
    const r = row({ '2027': [cell({ crop_id: 'c2027', provenance: 'planned', status: 'planned' })] });
    const html = renderToStaticMarkup(
      <ParcelHistorySection row={r} campaigns={[2022, 2023, 2024, 2025, 2026]} species={SPECIES} onSaved={() => {}} />,
    );
    for (const c of [2022, 2023, 2024, 2025, 2026]) expect(html).toContain(`Campaña ${c}`);
    // A campaign the backend leaves out (not over yet) gets no row, even when the parcel has a cycle in it.
    expect(html).not.toContain('Campaña 2027');
  });

  it('a declared crop in the rollover campaign can be cleared from the editor', () => {
    const r = row({ '2026': [cell({ crop_id: 'c2026', provenance: 'declared', start: null, end: null })] });
    const html = renderToStaticMarkup(
      <ParcelHistorySection row={r} campaigns={[2025, 2026]} species={SPECIES} onSaved={() => {}} />,
    );
    expect(html).toContain('Campaña 2026');
    expect(html).toContain('Quitar');
  });

  it('renders nothing when the backend sends no declarable campaign', () => {
    const html = renderToStaticMarkup(<ParcelHistorySection row={row({})} campaigns={[]} species={SPECIES} onSaved={() => {}} />);
    expect(html).toBe('');
  });

  it('renders editable rows for declared/empty past campaigns and read-only rows for dated field-book cycles', () => {
    const r = row({
      '2024': [cell({ crop_id: 'c2024', provenance: 'declared', start: null, end: null, variety: 'Rimbaud', yield_kg_ha: 4100 })],
      '2025': [cell({ crop_id: 'c2025', provenance: 'actual', start: '2024-11-01', end: '2025-07-01', variety: 'Filon', yield_kg_ha: 4800, yield_source: 'measured' })],
    });
    const html = renderToStaticMarkup(
      <ParcelHistorySection row={r} campaigns={[2022, 2023, 2024, 2025]} species={SPECIES} onSaved={() => {}} />,
    );
    expect(html).toContain('Histórico de campañas (5 años)');
    expect(html).toContain('Campaña 2024');
    expect(html).toContain('Quitar');
    expect(html).toContain('Campaña 2025');
    expect(html).toContain('realizado');
    expect(html).toContain('Filon');
    expect(html).toContain('4800 kg/ha');
  });

  it('CycleLine displays yield in t/ha when yield_kg_ha is present', () => {
    const html = renderToStaticMarkup(<CycleLine cell={cell({ yield_kg_ha: 4200 })} />);
    expect(html).toContain('◐ trigo blando · Rimbaud · 4,2 t/ha');
  });
});
