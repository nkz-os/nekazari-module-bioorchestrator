import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import PlanMatrix from './PlanMatrix';
import type { FarmResponse } from './types';

const DATA: FarmResponse = {
  contract_version: '1.0', campaign: 2027, campaigns: [2024, 2025, 2026, 2027],
  farms: [{ farm_id: null, name: null }],
  summary: { area_by_crop: [], undecided: 1, rotation_warnings: 1 },
  parcels: [
    { parcel_id: 'urn:ngsi-ld:AgriParcel:t:p1', name: 'Soto Alto', area_ha: 12.4, farm_id: null, regime: 'secano',
      regime_source: 'declared', cycles_available: true,
      cells: { '2024': [], '2025': [], '2026': [{ crop_id: 'c1', species: 'rapeseed', species_raw: 'BRSNN', names: { es: 'colza' },
        variety: 'Atlas', role: 'main_crop', provenance: 'actual', status: 'harvested', start: null, end: null,
        lifecycle: 'annual', first_campaign: 2026 }], '2027': [] },
      warnings: [{ code: 'rotation_restriction', campaign: 2027, params: { species: 'rapeseed', previous: 'rapeseed', previous_campaign: 2026, allowed_from: 2030 } }] },
    { parcel_id: 'urn:ngsi-ld:AgriParcel:t:p2', name: 'El Raso', area_ha: 8, farm_id: null, regime: null,
      regime_source: 'unknown', cycles_available: false, cells: { '2024': [], '2025': [], '2026': [], '2027': [] },
      warnings: [{ code: 'cycles_unavailable', campaign: null, params: {} }, { code: 'regime_missing', campaign: 2027, params: {} }] },
  ],
};

describe('PlanMatrix (es)', () => {
  it('shows history symbols, choose buttons, regimes and readable warnings', () => {
    const html = renderToStaticMarkup(<PlanMatrix data={DATA} farmId="all" onOpen={() => {}} />);
    expect(html).toContain('● colza · Atlas');
    expect(html).toContain('Elegir');
    expect(html).toContain('Sin declarar');
    expect(html).toContain('colza tras colza (2026): posible desde 2030');
    expect(html).toContain('Falta declarar secano o regadío');
    expect(html).toContain('No se ha podido leer el historial de esta parcela');
    expect(html.split('Elegir').length - 1).toBe(1);
    expect(html.indexOf('El Raso')).toBeLessThan(html.indexOf('Soto Alto'));
  });
});
