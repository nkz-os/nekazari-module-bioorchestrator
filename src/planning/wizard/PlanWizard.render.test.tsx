import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('../testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
// Server rendering runs no effects: the wizard's own loads stay loading; the fit comes from the detail.
vi.mock('../useLoad', () => ({ useLoad: () => ({ data: null, error: null, loading: true, retry: () => {} }) }));
vi.mock('../useAquacropEstimate', () => ({ useAquacropEstimate: () => ({ response: null, failure: null, retry: () => {} }) }));

import PlanWizard from './PlanWizard';
import type { CycleCell, FitResponse, ParcelRow, SpeciesFit } from '../types';

const sp = (species: string, es: string): SpeciesFit => ({
  species, eppo: null, names: { es }, lifecycle: 'annual', verdict: 'fits', reasons: [], has_variety_trials: false, estimate_providers: [],
  sowing: { sowing_type: null, start_month: 4, end_month: 5, typical_sowing_doy: null, source: null },
});
const FIT = { contract_version: '2.2', parcel_id: 'p1', campaign: 2027, graphs: ['global'],
  facts: { climate_class: 'Csa', climate_source: 'chelsa', soil_texture: 'loam', soil_ph: 7.5, soil_source: 'soil_module',
    regime: 'rainfed', regime_source: 'declared', history_available: true },
  species: [sp('maize', 'maíz'), sp('wheat', 'trigo')] } as FitResponse;
const cell: CycleCell = { crop_id: 'c1', species: 'maize', species_raw: 'ZEAMX', names: { es: 'maíz' }, variety: null, role: 'main_crop',
  provenance: 'planned', status: 'planned', start: '2027-04-20', end: '2027-10-01', lifecycle: 'annual', first_campaign: 2027 };
const ROW = (cells: CycleCell[]): ParcelRow => ({ parcel_id: 'urn:ngsi-ld:AgriParcel:t:p1', name: 'Kara', area_ha: 8.9, farm_id: null,
  regime: 'rainfed', regime_source: 'parcel:irrigationRegime', cycles_available: true, cells: { '2027': cells }, warnings: [] });
const render = (cells: CycleCell[], withFit: boolean) => {
  const fit = { data: withFit ? FIT : null, error: null, loading: !withFit, retry: () => {} };
  return renderToStaticMarkup(<PlanWizard row={ROW(cells)} campaign={2027} regime="rainfed" management="conventional"
    onSaved={() => {}} fit={fit} />);
};

describe('PlanWizard (es)', () => {
  it('starts at step 1 with the step count and a loading line while the fit loads', () => {
    const html = render([], false);
    expect(html).toMatch(/Paso 1 de \d/);
    expect(html).toContain('role="status"');
  });
  it('asks one question: the crop, with Siguiente disabled until one is chosen and no Atrás on the first step', () => {
    const html = render([], true);
    expect(html).toContain('¿Qué vas a sembrar?');
    expect(html).not.toContain('Atrás');
    expect(html).toMatch(/<button[^>]*\sdisabled=""[^>]*>Siguiente<\/button>/);
  });
  it('"Cambiar" starts from the current plan: its crop is chosen and Siguiente is enabled', () => {
    const html = render([cell], true);
    expect(html).toMatch(/aria-pressed="true"[^>]*>(?:(?!<\/button>).)*maíz/);
    expect(html).not.toMatch(/<button[^>]*\sdisabled=""[^>]*>Siguiente<\/button>/);
  });
});
