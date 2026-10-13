import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

// Server rendering runs no effects, so the loads are answered by the loader they would call: the fit
// when `withFit` is on, nothing else. The other steps stay empty.
const loads = vi.hoisted(() => ({ withFit: false }));
vi.mock('./useLoad', () => ({
  useLoad: (load: (signal: AbortSignal) => unknown, _deps: unknown[], enabled = true) => ({
    data: enabled && loads.withFit && String(load).includes('fetchFit') ? FIT : null, error: null, loading: false, retry: () => {},
  }),
}));

import ParcelDetail from './ParcelDetail';
import type { CycleCell, FitResponse, ParcelRow, SpeciesFit } from './types';

const sp = (species: string, es: string, over: Partial<SpeciesFit> = {}): SpeciesFit => ({
  species, eppo: null, names: { es }, lifecycle: 'annual', verdict: 'fits', reasons: [], has_variety_trials: true, estimate_providers: [],
  sowing: { sowing_type: null, start_month: 4, end_month: 5, typical_sowing_doy: null, source: null }, ...over,
});
const FIT: FitResponse = {
  contract_version: '1.0', parcel_id: 'p1', campaign: 2027, graphs: ['global'],
  facts: { climate_class: 'Csa', climate_source: 'chelsa_v2.1', soil_texture: 'loam', soil_ph: 7.5, soil_source: 'soil_module',
    regime: 'rainfed', regime_source: 'declared', history_available: true },
  species: [sp('maize', 'maíz'), sp('vetch', 'veza', { has_variety_trials: false })],
};
const cell = (over: Partial<CycleCell>): CycleCell => ({
  crop_id: 'c1', species: 'maize', species_raw: 'ZEAMX', names: { es: 'maíz' }, variety: 'Var X', role: 'main_crop',
  provenance: 'planned', status: 'planned', start: '2027-04-20', end: '2027-10-01', lifecycle: 'annual', first_campaign: 2027, ...over,
});
const row = (cells: CycleCell[], over: Partial<ParcelRow> = {}): ParcelRow => ({
  parcel_id: 'urn:ngsi-ld:AgriParcel:t:p1', name: 'Soto Alto', area_ha: 12, farm_id: null, regime: 'rainfed',
  regime_source: 'declared', cycles_available: true, cells: { '2026': [], '2027': cells }, warnings: [], ...over,
});
const render = (r: ParcelRow, withFit = false, onReload?: () => void) => {
  loads.withFit = withFit;
  return renderToStaticMarkup(<ParcelDetail row={r} campaign={2027} onClose={() => {}} onSaved={() => {}} onReload={onReload} />);
};

describe('ParcelDetail (es)', () => {
  it('shows the current plan of the campaign above the wizard', () => {
    const cover = cell({ crop_id: 'c0', role: 'cover_crop', species: 'vetch', species_raw: 'VICSA', names: { es: 'veza' }, variety: null });
    const html = render(row([cover, cell({})]), true);
    expect(html).toContain('Plan actual:');
    expect(html).toContain('○ maíz · Var X');
    expect(html).toContain('○ veza');
    expect(html.indexOf('Plan actual')).toBeLessThan(html.indexOf('Paso 1 de'));
  });
  it('shows no current-plan line when the campaign is empty', () => {
    expect(render(row([]))).not.toContain('Plan actual');
  });
  it('the header says the regime and management, with Cambiar instead of open selects', () => {
    const html = render(row([]), true);
    expect(html).toContain('Secano · Convencional');
    expect(html).toContain('Cambiar');
    expect(html).not.toContain('¿Secano o regadío?');
  });
  it('a parcel without a regime asks for it first and shows no wizard until it is chosen', () => {
    const html = render(row([], { regime: null, regime_source: 'unknown' }), true);
    expect(html).toContain('¿Secano o regadío?');
    expect(html).toContain('Hace falta para ver variedades');
    expect(html).not.toContain('Paso 1 de');
  });
  it('an open campaign shows the wizard at its first step; "Cambiar" starts from the current plan', () => {
    expect(render(row([]), true)).toMatch(/Paso 1 de \d/);
    expect(render(row([cell({})]), true)).toMatch(/aria-pressed="true"[^>]*>(?:(?!<\/button>).)*maíz/);
  });
  it('shows what we know of the parcel only once the fit has answered, never as "sin datos" while it loads', () => {
    const loading = render(row([]));
    expect(loading).not.toContain('Lo que sabemos');
    expect(loading).not.toContain('Clima:');
    const loaded = render(row([]), true);
    expect(loaded).toContain('Lo que sabemos');
    expect(loaded).toContain('Clima: Csa');
  });
  it('a lock from an unreadable history offers a retry only when the page can reload', () => {
    // A locked campaign shows why instead of the wizard.
    const unreadable = row([cell({})], { cycles_available: false });
    const withReload = render(unreadable, true, () => {});
    expect(withReload).toContain('No se ha podido leer el historial de esta parcela');
    expect(withReload).toContain('Reintentar');
    expect(render(unreadable, true)).not.toContain('Reintentar');
  });
  it('a cycle of unknown origin or status is not called a sown crop', () => {
    const html = render(row([cell({}), cell({ crop_id: 'c2', provenance: 'unknown', status: null })]), true);
    expect(html).toContain('un cultivo de origen o estado desconocido');
    expect(html).not.toContain('ya tiene un cultivo sembrado');
    expect(render(row([cell({}), cell({ crop_id: 'c2', provenance: 'actual', status: 'active' })]), true)).toContain('ya tiene un cultivo sembrado');
  });
});
