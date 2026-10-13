import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('../testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
import StepCrop, { recommendedCrops } from './StepCrop';
import type { FitResponse, SpeciesFit } from '../types';

const sp = (species: string, verdict: SpeciesFit['verdict']): SpeciesFit => ({ species, eppo: null, names: { es: species },
  lifecycle: 'annual', verdict, reasons: [], sowing: { sowing_type: null, start_month: null, end_month: null, typical_sowing_doy: null, source: null },
  has_variety_trials: false, estimate_providers: [] });
const FIT = { contract_version: '2.2', parcel_id: 'p', campaign: 2027, graphs: [], facts: {} as FitResponse['facts'],
  species: [sp('wheat', 'fits'), sp('barley', 'caveats'), sp('rice', 'no_fit'), ...['a', 'b', 'c', 'd', 'e'].map((x) => sp(x, 'fits'))] } as FitResponse;

describe('StepCrop', () => {
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
});
