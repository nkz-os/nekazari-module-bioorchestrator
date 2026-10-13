import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
vi.mock('react-router-dom', () => ({ useNavigate: () => () => {} }));
vi.mock('@nekazari/viewer-kit', () => ({ SlotShell: ({ children }: { children: React.ReactNode }) => <section>{children}</section> }));
vi.mock('../utils/entityData', () => ({ resolveParcelContext: () => ({ parcelId: 'p1' }) }));
const farm = vi.hoisted(() => ({ data: null as unknown }));
vi.mock('./useLoad', () => ({ useLoad: () => ({ data: farm.data, error: null, loading: false, retry: () => {} }) }));

import ParcelPlanWidget from './ParcelPlanWidget';
import type { FarmResponse, ParcelRow } from './types';

const parcel = (over: Partial<ParcelRow>): ParcelRow => ({
  parcel_id: 'urn:ngsi-ld:AgriParcel:t:p1', name: 'Soto Alto', area_ha: 12, farm_id: null, regime: 'rainfed', regime_source: 'declared',
  cycles_available: true, cells: { '2026': [], '2027': [] }, warnings: [], ...over,
});
const render = (row: ParcelRow) => {
  const data: FarmResponse = { contract_version: '1.0', campaign: 2027, campaigns: [2026, 2027], farms: [],
    summary: { area_by_crop: [], undecided: 0, rotation_warnings: 0 }, parcels: [row] };
  farm.data = data;
  return renderToStaticMarkup(<ParcelPlanWidget entityData={{ id: 'p1' }} />);
};

describe('ParcelPlanWidget (es)', () => {
  it('explains the symbols under the rows, where a touch screen has no tooltip', () => {
    const html = render(parcel({}));
    expect(html).toContain('● realizado · ◐ declarado · ○ planificado');
    expect(html.indexOf('Sin decidir')).toBeLessThan(html.indexOf('● realizado'));
  });
  it('shows no legend when the history could not be read (there are no rows)', () => {
    const html = render(parcel({ cycles_available: false }));
    expect(html).not.toContain('● realizado');
    expect(html).not.toContain('Sin decidir');
  });
});
