import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

let isAdmin = true;
let worklistRows: unknown = null;
vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), useAuth: () => ({ hasRole: (r: string) => isAdmin && r === 'PlatformAdmin' }), i18n: {} };
});
vi.mock('react-router-dom', () => ({
  useNavigate: () => vi.fn(),
  useSearchParams: () => [new URLSearchParams(), vi.fn()],
}));

const FARM = {
  contract_version: '1.0', campaign: 2027, campaigns: [2026, 2027],
  farms: [{ farm_id: null, name: null }],
  summary: { area_by_crop: [], undecided: 0, rotation_warnings: 0 },
  parcels: [{ parcel_id: 'urn:ngsi-ld:AgriParcel:p1', name: 'Soto Alto', area_ha: 3, farm_id: null, regime: null,
    regime_source: 'unknown', cycles_available: true, cells: { '2026': [], '2027': [] }, warnings: [] }],
};
const WORK = [{ parcelId: 'p1', parcelName: 'Soto Alto', cropStatus: 'none', surprise: null, nextJob: { kind: 'irrigate', when: 'today', detail: {} } }];
// First useLoad call is the farm, the second the work list.
let call = 0;
vi.mock('./useLoad', () => ({
  useLoad: () => (call++ % 2 === 0
    ? { data: FARM, loading: false, error: null, retry: vi.fn() }
    : { data: worklistRows, loading: false, error: null, retry: vi.fn() }),
}));

import PlanningApp from './PlanningApp';

describe('PlanningApp tabs (es)', () => {
  it('keeps sources and admin out of the default Plan tab', () => {
    call = 0; isAdmin = true; worklistRows = null;
    const html = renderToStaticMarkup(<PlanningApp />);
    expect(html).toMatch(/<button[^>]*role="tab"[^>]*>Plan<\/button>/);
    expect(html).toMatch(/<button[^>]*role="tab"[^>]*>Más<\/button>/);
    expect(html).not.toContain('Ocultar fuentes');
    expect(html).not.toContain('Fuentes de datos');
    expect(html).not.toContain('Administración');
  });

  it('renders for a non-admin too', () => {
    call = 0; isAdmin = false; worklistRows = null;
    const html = renderToStaticMarkup(<PlanningApp />);
    expect(html).toContain('Más');
    expect(html).not.toContain('Administración');
  });

  it('shows the what-now block only when the work list is installed', () => {
    call = 0; isAdmin = true; worklistRows = WORK;
    expect(renderToStaticMarkup(<PlanningApp />)).toContain('Qué toca ahora');
    call = 0; worklistRows = null;
    expect(renderToStaticMarkup(<PlanningApp />)).not.toContain('Qué toca ahora');
  });

  it('hides work-list rows of parcels the farm filter does not show', () => {
    call = 0; isAdmin = true;
    worklistRows = [{ ...WORK[0], parcelId: 'other', parcelName: 'Fuera' }];
    expect(renderToStaticMarkup(<PlanningApp />)).not.toContain('Fuera');
  });
});

import SourcesFooter from './SourcesFooter';
describe('SourcesFooter in the More tab', () => {
  it('starts expanded when asked', () => {
    expect(renderToStaticMarkup(<SourcesFooter defaultOpen />)).toContain('Ocultar fuentes');
    expect(renderToStaticMarkup(<SourcesFooter />)).toContain('Fuentes de datos');
  });
});
