import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import ParcelDetail from './ParcelDetail';
import type { CycleCell, ParcelRow } from './types';

const cell = (over: Partial<CycleCell>): CycleCell => ({
  crop_id: 'c1', species: 'maize', species_raw: 'ZEAMX', names: { es: 'maíz' }, variety: 'Var X', role: 'main_crop',
  provenance: 'planned', status: 'planned', start: '2027-04-20', end: '2027-10-01', lifecycle: 'annual', first_campaign: 2027, ...over,
});
const row = (cells: CycleCell[], over: Partial<ParcelRow> = {}): ParcelRow => ({
  parcel_id: 'urn:ngsi-ld:AgriParcel:t:p1', name: 'Soto Alto', area_ha: 12, farm_id: null, regime: 'secano',
  regime_source: 'declared', cycles_available: true, cells: { '2026': [], '2027': cells }, warnings: [], ...over,
});
const render = (r: ParcelRow) => renderToStaticMarkup(<ParcelDetail row={r} campaign={2027} onClose={() => {}} onSaved={() => {}} />);

describe('ParcelDetail (es)', () => {
  it('shows the current plan of the campaign above the regime question', () => {
    const cover = cell({ crop_id: 'c0', role: 'cover_crop', species: 'vetch', species_raw: 'VICSA', names: { es: 'veza' }, variety: null });
    const html = render(row([cover, cell({})]));
    expect(html).toContain('Plan actual:');
    expect(html).toContain('○ maíz · Var X');
    expect(html).toContain('○ veza');
    expect(html.indexOf('Plan actual')).toBeLessThan(html.indexOf('¿Secano o regadío?'));
  });
  it('shows no current-plan line when the campaign is empty', () => {
    expect(render(row([]))).not.toContain('Plan actual');
  });
});
