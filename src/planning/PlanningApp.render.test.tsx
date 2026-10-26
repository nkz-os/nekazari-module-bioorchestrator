import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), useAuth: () => ({ hasRole: () => true }), i18n: {} };
});
vi.mock('react-router-dom', () => ({
  useNavigate: () => vi.fn(),
  useSearchParams: () => [new URLSearchParams(), vi.fn()],
}));

const FARM = {
  contract_version: '1.0', campaign: 2027, campaigns: [2026, 2027],
  farms: [{ farm_id: null, name: null }],
  summary: { area_by_crop: [], undecided: 0, rotation_warnings: 0 },
  parcels: [],
};
// First useLoad call is the farm, the second the work list.
let call = 0;
vi.mock('./useLoad', () => ({
  useLoad: () => (call++ % 2 === 0
    ? { data: FARM, loading: false, error: null, retry: vi.fn() }
    : { data: null, loading: false, error: null, retry: vi.fn() }),
}));

import PlanningApp from './PlanningApp';

describe('PlanningApp tabs (es)', () => {
  it('keeps sources and admin out of the default Plan tab', () => {
    call = 0;
    const html = renderToStaticMarkup(<PlanningApp />);
    expect(html).toContain('Más');
    expect(html).toContain('Plan');
    expect(html).not.toContain('Fuentes de datos');
    expect(html).not.toContain('Administración');
  });
});

import SourcesFooter from './SourcesFooter';
describe('SourcesFooter in the More tab', () => {
  it('starts expanded when asked', () => {
    expect(renderToStaticMarkup(<SourcesFooter defaultOpen />)).toContain('Ocultar fuentes');
    expect(renderToStaticMarkup(<SourcesFooter />)).toContain('Fuentes de datos');
  });
});
