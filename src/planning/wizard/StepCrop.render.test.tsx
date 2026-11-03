import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('../testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
import StepCrop, { RegionalDetail, cardReasons, mainCrops, matchesCrop, recommendedCrops } from './StepCrop';
import { esT } from '../testI18n';
import type { FitResponse, RegionalYield, SpeciesFit } from '../types';

const regional = (over: Partial<RegionalYield> = {}): RegionalYield => ({
  provider_id: 'eurostat_nuts2', basis: 'regional_statistics', nuts: 'ES99', region_name: { en: 'Demo Region' },
  first_year: 2014, last_year: 2023, n_years: 10, percentiles: { p10: 3.2, p50: 4.1, p90: 4.9 }, unit: 't/ha',
  moisture: { basis: 'eu_standard', pct: 14 }, regimes: 'mixed', relative_index: 1.12,
  attributions: [{ source_id: 'eurostat_apro_cpshr', text: 'Source: Eurostat apro_cpshr', url: 'https://example.org/eurostat', licence_id: 'x', licence_url: 'https://example.org/licence' }],
  ...over });

const sp = (species: string, verdict: SpeciesFit['verdict']): SpeciesFit => ({ species, eppo: null, names: { es: species },
  lifecycle: 'annual', verdict, reasons: [], sowing: { sowing_type: null, start_month: null, end_month: null, typical_sowing_doy: null, source: null },
  has_variety_trials: false, estimate_providers: [] });
const FIT = { contract_version: '2.4', parcel_id: 'p', campaign: 2027, graphs: [], facts: {} as FitResponse['facts'],
  species: [sp('wheat', 'fits'), sp('barley', 'caveats'), sp('rice', 'no_fit'), ...['a', 'b', 'c', 'd', 'e'].map((x) => sp(x, 'fits'))] } as FitResponse;

describe('StepCrop', () => {
  it('never offers a cover-only species as the main crop, in the cards or the search', () => {
    const cover = { ...sp('veza vellosa', 'fits'), cover_only: true };
    const fit = { ...FIT, species: [cover, ...FIT.species] } as FitResponse;
    expect(recommendedCrops(fit.species).map((s) => s.species)).not.toContain('veza vellosa');
    expect(mainCrops(fit.species)).toHaveLength(FIT.species.length);
    const html = renderToStaticMarkup(<StepCrop fit={fit} selected={null} onPick={() => {}} />);
    expect(html).not.toContain('veza vellosa');
  });
  it('recommends up to 6 crops that fit or fit with caveats, in fit order, never no_fit', () => {
    const rec = recommendedCrops(FIT.species);
    expect(rec).toHaveLength(6);
    expect(rec.map((s) => s.species)).not.toContain('rice');
  });
  it('asks one question and shows the recommended cards and a search', () => {
    const html = renderToStaticMarkup(<StepCrop fit={FIT} selected={null} onPick={() => {}} />);
    expect(html).toContain('¿Qué vas a sembrar?');
    expect(html).toContain('Recomendados para esta parcela');
    expect(html).toContain('Buscar otro cultivo');
  });
  it('each card says its verdict, so a crop with caveats reads as such', () => {
    const html = renderToStaticMarkup(<StepCrop fit={FIT} selected={null} onPick={() => {}} />);
    expect(html).toContain('Con reservas');
    expect(html).toContain('Encaja');
  });
  it('search ignores accents and case', () => {
    expect(matchesCrop('Maíz', 'maiz')).toBe(true);
    expect(matchesCrop('maíz', 'MA')).toBe(true);
    expect(matchesCrop('trigo', 'maiz')).toBe(false);
  });
  it('a crop that does not fit shows all its reasons; the others one line', () => {
    const r = (code: string) => ({ code, params: {} }) as never;
    expect(cardReasons({ ...sp('rice', 'no_fit'), reasons: [r('a'), r('b')] })).toHaveLength(2);
    expect(cardReasons({ ...sp('wheat', 'caveats'), reasons: [r('a'), r('b')] })).toHaveLength(1);
  });
  it('shows the regional line with the index, in the locale format', () => {
    const fit = { ...FIT, species: [{ ...sp('wheat', 'fits'), regional_yield: regional() }] } as FitResponse;
    const html = renderToStaticMarkup(<StepCrop fit={fit} selected={null} onPick={() => {}} />);
    expect(html).toContain('Región: 4,1 t/ha (3,2–4,9) · 112 % de España');
  });
  it('drops the "% of Spain" part when the national series is short (no index)', () => {
    const fit = { ...FIT, species: [{ ...sp('wheat', 'fits'), regional_yield: regional({ relative_index: null }) }] } as FitResponse;
    const html = renderToStaticMarkup(<StepCrop fit={fit} selected={null} onPick={() => {}} />);
    expect(html).toContain('Región: 4,1 t/ha (3,2–4,9)');
    expect(html).not.toContain('de España');
  });
  it('says there is no regional statistic, without a technical reason on the card', () => {
    const fit = { ...FIT, species: [{ ...sp('wheat', 'fits'), regional_yield: null, regional_yield_unavailable: 'crop_not_in_statistics' }] } as FitResponse;
    const html = renderToStaticMarkup(<StepCrop fit={fit} selected={null} onPick={() => {}} />);
    expect(html).toContain('Sin estadística regional');
    expect(html).not.toContain('no figura');
  });
  it('the detail names region, years, mixed regimes, EU moisture, not-a-forecast and the attribution', () => {
    const html = renderToStaticMarkup(<RegionalDetail s={{ ...sp('wheat', 'fits'), regional_yield: regional() }} lang="es" t={esT} />);
    for (const part of ['Demo Region', '2014–2023', 'secano y regadío juntos', 'Humedad estándar de la UE (14 %)',
      'No es una previsión para tu parcela', 'Source: Eurostat apro_cpshr']) expect(html).toContain(part);
  });
  it('the detail gives the reason when there is no figure; an unknown reason reads as generic', () => {
    const d = (reason: string) => renderToStaticMarkup(<RegionalDetail s={{ ...sp('wheat', 'fits'), regional_yield_unavailable: reason }} lang="es" t={esT} />);
    expect(d('region_series_short')).toContain('demasiado corta');
    expect(d('something_new')).toContain('no disponible ahora mismo');
  });
  it('keeps the order the backend sent, whatever the regional index', () => {
    const species = [0.9, 1.3, 1.1].map((i, k) => ({ ...sp(`c${k}`, 'fits'), regional_yield: regional({ relative_index: i }) }));
    expect(recommendedCrops(species).map((s) => s.species)).toEqual(['c0', 'c1', 'c2']);
  });
});
