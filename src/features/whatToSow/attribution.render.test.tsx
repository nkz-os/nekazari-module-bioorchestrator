import React from 'react';
/** Server-side render of the surfaces that show part of a recommend answer, with the real Spanish strings. */
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import es from '../../locales/es.json';

vi.mock('@nekazari/sdk', () => {
  const lookup = (key: string, opts?: Record<string, unknown>) => {
    const get = (k: string) => k.split('.').reduce<unknown>((n, p) => (n && typeof n === 'object' ? (n as Record<string, unknown>)[p] : undefined), es);
    let v = get(key);
    if (typeof v !== 'string' && opts && typeof opts.count === 'number') v = get(key + (opts.count === 1 ? '_one' : '_other'));
    if (typeof v !== 'string') return (opts?.defaultValue as string) ?? key;
    return v.replace(/\{\{(\w+)\}\}/g, (_m, k: string) => String(opts?.[k] ?? ''));
  };
  return { useTranslation: () => ({ t: lookup, i18n: { language: 'es' } }), i18n: {} };
});
vi.mock('../../services/api', () => ({ useBioApi: () => ({}), API_BASE: '', authHeaders: () => ({}) }));

import VarietyPanel from './VarietyPanel';
import CompareView from './CompareView';
import type { Recommendation } from '../../types/recommend';
import type { SourceAttributionItem } from '../../types/attribution';

const GENVCE_TEXT = 'Fuente: Datos Abiertos GENVCE. Url: https://genvce.org/mapa-de-resultados/ (Descarga: 01/06/2026.)';
const CREA_TEXT = 'Fonte: CREA - Consiglio per la ricerca in agricoltura. Licenza CC BY 3.0 IT.';
const attributions: SourceAttributionItem[] = [
  { source_id: 'CREA', text: CREA_TEXT, url: 'https://www.crea.gov.it/', licence_id: 'CC-BY-3.0-IT', licence_url: 'https://creativecommons.org/licenses/by/3.0/it/legalcode' },
  { source_id: 'GENVCE', text: GENVCE_TEXT, url: 'https://genvce.org/mapa-de-resultados/', licence_id: 'g', licence_url: 'https://genvce.org/' },
];
/** Visible text: links split a credit line without changing it. */
const textOf = (html: string) => html.replace(/<[^>]+>/g, '');
const CALCULATIONS = 'Las estimaciones y medias son cálculos de Nekazari';

function rec(eppo: string, sources: string[]): Recommendation {
  return {
    recommendation_id: eppo, crop: { eppo, scientific_name: eppo, sowing_type: 'autumn' },
    fit: { relative_yield_pct: 5, stability_cv: 0.2, reference: { median_kg_ha: 3000, n_trials: 9, scope: 'analog_sites:Csa:secano' } },
    yield: { expected_kg_ha: 3500, interval: [3000, 4000], interval_method: 'observed_range', basis: null, sd: 100, n_trials: 5, n_sites: 3 },
    suitability: { soil: { level: 'suitable', warnings: [] }, water: { level: 'low', etc_mm: null }, frost: { level: 'none' } },
    season: { sowing_window: null, cycle_days: null, source: null },
    trust: { level: 'medium', data_gaps: [], similarity: 'koppen' },
    varieties: [{ variety: 'V1', variety_uri: null, expected_kg_ha: 3600, interval: [3000, 4100], n_trials: 4, disease_summary: { resistant: 1, total: 2 } }],
    evidence: { trial_count: 5, sources, sites: ['S'], years: [2020, 2022], tier: 'field', purpose: 'main', regional_trial_count: 0, other_purpose_trials: {}, unknown_basis_trials: null },
    assumptions: [],
  };
}

describe('VarietyPanel attribution (es)', () => {
  it('credits only the sources of its own recommendation, with the calculations notice', () => {
    const html = renderToStaticMarkup(<VarietyPanel rec={rec('TRZAX', ['GENVCE'])} parcelId={null} attributions={attributions} />);
    expect(textOf(html)).toContain(GENVCE_TEXT);
    expect(html).not.toContain('CREA');
    expect(html).toContain(CALCULATIONS);
    expect(html).toContain('V1');
  });

  it('shows no credit block without attributions (older backend)', () => {
    const html = renderToStaticMarkup(<VarietyPanel rec={rec('TRZAX', ['GENVCE'])} parcelId={null} />);
    expect(html).not.toContain(CALCULATIONS);
    expect(html).toContain('V1');
  });
});

describe('CompareView attribution (es)', () => {
  it('credits the sources of the compared crops only', () => {
    const only = renderToStaticMarkup(
      <CompareView recs={[rec('TRZAX', ['GENVCE']), rec('HORVX', ['GENVCE'])]} parcelId={null} attributions={attributions} />);
    expect(textOf(only)).toContain(GENVCE_TEXT);
    expect(only).not.toContain('CREA');
    const both = renderToStaticMarkup(
      <CompareView recs={[rec('TRZAX', ['GENVCE']), rec('ZEAMX', ['CREA'])]} parcelId={null} attributions={attributions} />);
    expect(textOf(both)).toContain(GENVCE_TEXT);
    expect(textOf(both)).toContain(CREA_TEXT);
    expect(both).toContain(CALCULATIONS);
  });

  it('shows no credit block without attributions', () => {
    const html = renderToStaticMarkup(<CompareView recs={[rec('TRZAX', ['GENVCE']), rec('HORVX', ['GENVCE'])]} parcelId={null} />);
    expect(html).not.toContain(CALCULATIONS);
  });
});
