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
    { parcel_id: 'urn:ngsi-ld:AgriParcel:t:p1', name: 'Soto Alto', area_ha: 12.4, farm_id: null, regime: 'rainfed',
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
  it('offers no choose button on a campaign that is not editable', () => {
    const html = renderToStaticMarkup(<PlanMatrix data={DATA} farmId="all" editable={false} onOpen={() => {}} />);
    expect(html).not.toContain('Elegir');
    expect(html).toContain('● colza · Atlas');
  });
  it('links to the simulation from the target-campaign cell of a parcel with a sown crop, only when the page wires it', () => {
    const sown = { crop_id: 'c2', species: 'wheat', species_raw: 'TRZAX', names: { es: 'trigo' }, variety: null, role: 'main_crop',
      provenance: 'actual' as const, status: 'active', start: null, end: null, lifecycle: 'annual', first_campaign: 2027 };
    const inField: FarmResponse = { ...DATA, parcels: [
      { ...DATA.parcels[0], parcel_id: 'urn:ngsi-ld:AgriParcel:t:p3', name: 'En campo', cells: { '2024': [], '2025': [], '2026': [], '2027': [sown] } },
      { ...DATA.parcels[1], cycles_available: true, cells: { '2024': [], '2025': [], '2026': [], '2027': [] } },
    ] };
    const calls: string[] = [];
    const html = renderToStaticMarkup(<PlanMatrix data={inField} farmId="all" onOpen={() => {}} onSimulate={(id) => calls.push(id)} />);
    expect(html.split('Simulación de la campaña en curso').length - 1).toBe(1);
    expect(html.indexOf('En campo')).toBeLessThan(html.indexOf('Simulación de la campaña en curso'));
    // The planning button is still offered to the free parcel only: the sown one is locked.
    expect(html.split('Elegir').length - 1).toBe(1);
    expect(renderToStaticMarkup(<PlanMatrix data={inField} farmId="all" onOpen={() => {}} />)).not.toContain('Simulación de la campaña en curso');
    expect(calls).toEqual([]);
  });
  it('links a sown parcel to its crop-health forecast, beside the simulation, only when the page wires it', () => {
    const sown = { crop_id: 'c2', species: 'wheat', species_raw: 'TRZAX', names: { es: 'trigo' }, variety: null, role: 'main_crop',
      provenance: 'actual' as const, status: 'active', start: null, end: null, lifecycle: 'annual', first_campaign: 2027 };
    const inField: FarmResponse = { ...DATA, parcels: [
      { ...DATA.parcels[0], parcel_id: 'urn:ngsi-ld:AgriParcel:t:p3', name: 'En campo', cells: { '2024': [], '2025': [], '2026': [], '2027': [sown] } },
      { ...DATA.parcels[1], cycles_available: true, cells: { '2024': [], '2025': [], '2026': [], '2027': [] } },
    ] };
    const html = renderToStaticMarkup(<PlanMatrix data={inField} farmId="all" onOpen={() => {}} onSimulate={() => {}} onForecast={() => {}} />);
    expect(html.split('Ver previsión en Salud del cultivo').length - 1).toBe(1);
    expect(html).toContain('Simulación de la campaña en curso');
    expect(renderToStaticMarkup(<PlanMatrix data={inField} farmId="all" onOpen={() => {}} />)).not.toContain('Ver previsión en Salud del cultivo');
  });
  it('a locked parcel says why in its target cell instead of offering a button', () => {
    const started = { crop_id: 'c3', species: 'rye', species_raw: 'SECCE', names: { es: 'centeno' }, variety: null, role: 'main_crop',
      provenance: 'planned' as const, status: 'active', start: null, end: null, lifecycle: 'annual', first_campaign: 2027 };
    const locked: FarmResponse = { ...DATA, parcels: [{ ...DATA.parcels[0], warnings: [], cells: { '2024': [], '2025': [], '2026': [], '2027': [started] } }] };
    const html = renderToStaticMarkup(<PlanMatrix data={locked} farmId="all" onOpen={() => {}} />);
    expect(html).toContain('Esta campaña ya tiene un cultivo sembrado');
    expect(html).not.toContain('Cambiar');
    // a past campaign is only history: no lock text either
    expect(renderToStaticMarkup(<PlanMatrix data={locked} farmId="all" editable={false} onOpen={() => {}} />)).not.toContain('ya tiene un cultivo');
  });
  it('unreadable history is said once, as the row warning', () => {
    const html = renderToStaticMarkup(<PlanMatrix data={DATA} farmId="all" onOpen={() => {}} />);
    expect(html.split('No se ha podido leer el historial de esta parcela').length - 1).toBe(1);
  });
});
