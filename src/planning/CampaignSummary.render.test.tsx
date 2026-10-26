import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import CampaignSummary from './CampaignSummary';
import type { FarmResponse } from './types';

const DATA = {
  campaign: 2027,
  summary: {
    area_by_crop: [
      { species: 'wheat', names: { es: 'trigo' }, area_ha: 37.6 },
      { species: 'rapeseed', names: { es: 'colza' }, area_ha: 8.9 },
    ],
    undecided: 1, rotation_warnings: 2,
  },
} as unknown as FarmResponse;

describe('CampaignSummary (es)', () => {
  it('draws one bar per crop, the widest at 100%, and flags undecided parcels as danger', () => {
    const html = renderToStaticMarkup(<CampaignSummary data={DATA} />);
    expect(html).toContain('Campaña 2027');
    expect(html).toContain('trigo · 37,6 ha');
    expect(html).toContain('colza · 8,9 ha');
    expect(html).toContain('width:100%');
    expect(html).toContain('width:23.67');
    expect(html).toContain('bg-nkz-accent-base');
    expect(html).toMatch(/<[^>]*text-nkz-danger[^>]*>1 parcela sin decidir</);
    expect(html).toContain('2 avisos de rotación');
  });
  it('omits the undecided text and rotation warnings when there are none', () => {
    const clean = { ...DATA, summary: { ...DATA.summary, undecided: 0, rotation_warnings: 0 } } as FarmResponse;
    const html = renderToStaticMarkup(<CampaignSummary data={clean} />);
    expect(html).not.toContain('sin decidir');
    expect(html).not.toContain('rotación');
  });
});
