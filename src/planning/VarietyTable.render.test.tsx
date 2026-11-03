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

describe('VarietyTable evidence depth (es)', () => {
  const depth = { level: 'pilot' as const, n_trials: 72, n_sites: 3, n_years: 4, first_year: 2023, last_year: 2026 };
  it('names the depth of the evidence and details it in the title', () => {
    const html = renderToStaticMarkup(<VarietyTable data={{ ...base, evidence_depth: depth }} selected={null} onSelect={() => {}} />);
    expect(html).toContain('red piloto');
    expect(html).toContain('72 ensayos · 3 sitios · 2023–2026');
  });
  it('a deep network reads as such', () => {
    const deep = { ...depth, level: 'deep' as const, n_trials: 4712, n_sites: 86, first_year: 2006 };
    const html = renderToStaticMarkup(<VarietyTable data={{ ...base, evidence_depth: deep }} selected={null} onSelect={() => {}} />);
    expect(html).toContain('evidencia profunda');
    expect(html).toContain('4712 ensayos · 86 sitios · 2006–2026');
  });
  it('an answer without evidence_depth renders the table as before', () => {
    const html = renderToStaticMarkup(<VarietyTable data={base} selected={null} onSelect={() => {}} />);
    expect(html).toContain('Var A');
    expect(html).not.toContain('ensayos ·');
  });
});

describe('VarietyTable actuality (es)', () => {
  it('a variety without recent trials says since when, and stays in the table', () => {
    const stale = { ...base, varieties: [{ ...base.varieties[0], variety: 'Var Old', last_year: 2021, stale: true }] };
    const html = renderToStaticMarkup(<VarietyTable data={stale} selected={null} onSelect={() => {}} />);
    expect(html).toContain('Var Old');
    expect(html).toContain('sin ensayos desde 2021');
  });
  it('a current variety carries no such note', () => {
    const current = { ...base, varieties: [{ ...base.varieties[0], last_year: 2024, stale: false }] };
    const html = renderToStaticMarkup(<VarietyTable data={current} selected={null} onSelect={() => {}} />);
    expect(html).not.toContain('sin ensayos desde');
  });
});
