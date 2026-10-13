import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import FitList from './FitList';
import type { FitResponse, SpeciesFit } from './types';

const sp = (species: string, es: string, verdict: SpeciesFit['verdict'], reasons: SpeciesFit['reasons'], trials = false): SpeciesFit => ({
  species, eppo: null, names: { es }, lifecycle: 'annual', verdict, reasons, has_variety_trials: trials, estimate_providers: [],
  sowing: { sowing_type: 'autumn', start_month: 10, end_month: 11, typical_sowing_doy: null, source: null },
});
const FIT: FitResponse = {
  contract_version: '1.0', parcel_id: 'p1', campaign: 2027, graphs: ['global'],
  facts: { climate_class: 'Csa', climate_source: 'chelsa_v2.1', soil_texture: null, soil_ph: null, soil_source: 'unavailable',
    regime: 'rainfed', regime_source: 'declared', history_available: true },
  species: [
    sp('wheat', 'trigo', 'fits', [{ code: 'rotation_benefit', gate: 'rotation', params: { previous: 'chickpea' } }], true),
    sp('chickpea', 'garbanzo', 'caveats', [{ code: 'soil_unknown', gate: 'soil', params: { why: 'no_parcel_soil' } }]),
    sp('rapeseed', 'colza', 'no_fit', [{ code: 'frost_risk', gate: 'frost', params: { coldest_min_c: -12, threshold_c: -10, margin_c: 5 } }]),
  ],
};

describe('FitList (es)', () => {
  it('groups by verdict with reasons in words and folds the no-fit group', () => {
    const html = renderToStaticMarkup(<FitList fit={FIT} selected={null} onSelect={() => {}} />);
    expect(html).toContain('Encaja');
    expect(html).toContain('Buena rotación tras garbanzo');
    expect(html).toContain('Con reservas');
    expect(html).toContain('Falta el suelo de la parcela');
    expect(html).toContain('variedades con ensayos');
    expect(html).toContain('Ver los que no encajan (1)');
    expect(html).not.toContain('Riesgo de helada');
    expect(html).not.toMatch(/kg\/ha|t\/ha/);
  });
  it('says a reason every crop carries once, above the groups, and not on each crop', () => {
    const noRegime = { code: 'regime_missing', gate: 'data' as const, params: {} };
    const shared: FitResponse = { ...FIT, species: FIT.species.map((x) => ({ ...x, reasons: [noRegime, ...x.reasons] })) };
    const html = renderToStaticMarkup(<FitList fit={shared} selected={null} onSelect={() => {}} />);
    expect(html.split('Falta declarar secano o regadío').length - 1).toBe(1);
    expect(html.split('Para todos los cultivos:').length - 1).toBe(1);
    expect(html.indexOf('Para todos los cultivos:')).toBeLessThan(html.indexOf('Encaja'));
    expect(html.indexOf('Falta declarar secano o regadío')).toBeLessThan(html.indexOf('Encaja'));
    // A crop's own reasons stay on the crop.
    expect(html).toContain('Buena rotación tras garbanzo');
    expect(html).toContain('Falta el suelo de la parcela');
  });
  it('shows no shared line when nothing is shared, and the trials badge only for crops that have trials', () => {
    const html = renderToStaticMarkup(<FitList fit={FIT} selected={null} onSelect={() => {}} />);
    expect(html).not.toContain('Para todos los cultivos:');
    expect(html.split('variedades con ensayos').length - 1).toBe(1);
    expect(html).not.toContain('sin ensayos de variedades');
  });
});
