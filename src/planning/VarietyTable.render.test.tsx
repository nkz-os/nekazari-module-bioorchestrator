import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import VarietyTable from './VarietyTable';
import type { VarietiesResponse } from './types';

const base: VarietiesResponse = {
  contract_version: '1.0', parcel_id: 'p1', crop: 'TRZAX', regime: 'rainfed', management: 'conventional',
  status: 'ok', tier: 'regional', zone_match: null, country: 'ES', n_environments: 12, graph_id: 'global',
  data_gaps: ['regional_evidence_only'], attributions: [],
  varieties: [{ variety: 'Var A', relative_yield_pct: 108, n_environments: 14, years: [2019, 2024], mean_kg_ha: 6100,
    fw_slope: 0.8, stability: 'stable', few_environments: false, low_env_relative_yield_pct: 92.4, n_low_environments: 4 }],
};

describe('VarietyTable (es)', () => {
  it('relative yield, stability and the regional tier, never a farm yield promise', () => {
    const html = renderToStaticMarkup(<VarietyTable data={base} selected={null} onSelect={() => {}} />);
    expect(html).toContain('Var A');
    expect(html).toContain('108 %');
    expect(html).toContain('estable');
    expect(html).toContain('14 / 2019–2024');
    expect(html).toContain('medias regionales');
  });
  it('shows how each variety holds in the poor trials, and a dash without enough of them', () => {
    const html = renderToStaticMarkup(<VarietyTable data={base} selected={null} onSelect={() => {}} />);
    expect(html).toContain('En ensayos flojos');
    expect(html).toContain('92 %');
    const none = { ...base, varieties: [{ ...base.varieties[0], low_env_relative_yield_pct: null }] };
    const dash = renderToStaticMarkup(<VarietyTable data={none} selected={null} onSelect={() => {}} />);
    expect(dash).not.toContain('92 %');
    expect(dash).toContain('—');
  });
  it('a crop without trials says so', () => {
    const html = renderToStaticMarkup(<VarietyTable data={{ ...base, status: 'no_trials', varieties: [] }} selected={null} onSelect={() => {}} />);
    expect(html).toContain('No hay ensayos de variedades para este cultivo en tu zona');
  });
  it('an ok answer with no comparable variety shows the gap, not an empty table', () => {
    const html = renderToStaticMarkup(<VarietyTable data={{ ...base, status: 'ok', varieties: [], data_gaps: ['insufficient_environments'] }} selected={null} onSelect={() => {}} />);
    expect(html).not.toContain('<table');
    expect(html).not.toContain('Rendimiento frente a la media');
    expect(html).toContain('Ningún ensayo tiene variedades suficientes para comparar');
  });
});
