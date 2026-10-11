import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import PlanForm from './PlanForm';
import type { PlanDraft } from './planModel';
import type { SpeciesFit } from './types';

const MAIN: SpeciesFit = {
  species: 'maize', eppo: 'ZEAMX', names: { es: 'maíz' }, lifecycle: 'annual', verdict: 'fits', reasons: [], has_variety_trials: true,
  estimate_providers: [], sowing: { sowing_type: null, start_month: 4, end_month: 5, typical_sowing_doy: null, source: null },
};
const DRAFT: PlanDraft = { main: { species: 'maize', variety: 'Var X', sowing: '2027-04-20', end: '2027-10-01', method: null }, cover: null };
const props = { parcelId: 'p1', campaign: 2027, regime: 'secano' as const, management: 'conventional' as const, main: MAIN,
  coverOptions: [], varieties: [{ variety: 'Var X', relative_yield_pct: 105, n_environments: 9, years: [2020, 2024], mean_kg_ha: 1,
    fw_slope: null, stability: null, few_environments: false }], draft: DRAFT, onDraft: () => {}, onSaved: () => {} };

describe('PlanForm (es)', () => {
  it('warns above the submit button that saving replaces the current plan', () => {
    const html = renderToStaticMarkup(<PlanForm {...props} replacing />);
    expect(html).toContain('Al guardar se sustituye el plan actual de esta campaña: revisa el manejo y la cubierta.');
    expect(html.indexOf('Al guardar se sustituye')).toBeLessThan(html.indexOf('Planificar</button>'));
  });
  it('says nothing about replacing when the campaign has no plan yet', () => {
    expect(renderToStaticMarkup(<PlanForm {...props} replacing={false} />)).not.toContain('Al guardar se sustituye');
  });
});
