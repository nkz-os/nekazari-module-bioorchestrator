import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import EstimatesTable from './EstimatesTable';
import type { EstimatesResponse } from './types';

const DATA: EstimatesResponse = {
  contract_version: '1.0', parcel_id: 'p1', crop: 'TRZAX', variety: null, status: 'ok', disagreement: false,
  inputs: { regime: 'secano', management: 'conventional', sowing_date: '2026-11-05', sowing_source: 'planned',
    soil_source: 'soil_module', climate_source: 'chelsa_v2.1' },
  estimates: [{ provider_id: 'trial_evidence', provider_version: 'p', basis: 'trial_evidence', unit: 't/ha',
    percentiles: { p10: 5.1, p50: 6.8, p90: 8.2 }, n: 42, n_kind: 'trials',
    validation: { status: 'not_applicable', metric: null, scope: null }, warnings: ['evidence_truncated'], attributions: [], graph_id: 'global' }],
  unavailable: [{ provider_id: 'aquacrop', reason: 'preseason_not_supported' }],
};

describe('EstimatesTable (es)', () => {
  it('one row per source with its basis, and AquaCrop shown as pending', () => {
    const html = renderToStaticMarkup(<EstimatesTable data={DATA} />);
    expect(html).toContain('Mismas entradas para todas: Secano · Convencional · siembra 2026-11-05');
    expect(html).toContain('Ensayos de la zona');
    expect(html).toContain('5,1 – 8,2');
    expect(html).toContain('6,8');
    expect(html).toContain('parcela de ensayo, manejo óptimo');
    expect(html).toContain('42 ensayos');
    expect(html).toContain('Hay más ensayos de los que se han analizado; se han usado los más recientes');
    expect(html).toContain('Simulación antes de la siembra: aún no disponible');
  });
  it('disagreement is said, not averaged', () => {
    expect(renderToStaticMarkup(<EstimatesTable data={{ ...DATA, disagreement: true }} />)).toContain('Los modelos no coinciden');
  });
});
