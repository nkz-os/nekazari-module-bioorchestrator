import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import EstimatesTable from './EstimatesTable';
import type { Estimate, EstimatesResponse } from './types';

const DATA: EstimatesResponse = {
  contract_version: '1.0', parcel_id: 'p1', crop: 'TRZAX', variety: null, status: 'ok', disagreement: false,
  inputs: { regime: 'secano', management: 'conventional', sowing_date: '2026-11-05', sowing_source: 'planned',
    soil_source: 'soil_module', climate_source: 'chelsa_v2.1' },
  estimates: [{ provider_id: 'trial_evidence', provider_version: 'p', basis: 'trial_evidence', unit: 't/ha',
    percentiles: { p10: 5.1, p50: 6.8, p90: 8.2 }, range: null, coverage: null,
    moisture: { basis: 'as_reported', pct: null }, n: 42, n_kind: 'trials',
    validation: { status: 'not_applicable', metric: null, scope: null }, warnings: ['evidence_truncated'], attributions: [], graph_id: 'global' }],
  unavailable: [{ provider_id: 'aquacrop', reason: 'not_validated' }],
};

describe('EstimatesTable (es)', () => {
  it('one row per source with its basis, and AquaCrop shown as pending', () => {
    const html = renderToStaticMarkup(<EstimatesTable data={DATA} />);
    expect(html).toContain('Mismas entradas para todas: Secano · Convencional · siembra 5/11/2026');
    expect(html).toContain('Ensayos de la zona');
    expect(html).toContain('5,1 – 8,2');
    expect(html).toContain('6,8');
    expect(html).toContain('parcela de ensayo, manejo óptimo');
    expect(html).toContain('42 ensayos');
    expect(html).toContain('Hay más ensayos de los que se han analizado; se han usado los más recientes');
    expect(html).toContain('pendiente: aún no validado para este cultivo');
  });
  it('disagreement is said, not averaged', () => {
    expect(renderToStaticMarkup(<EstimatesTable data={{ ...DATA, disagreement: true }} />)).toContain('Los modelos no coinciden');
  });
  it('without the parcel climate there is no trial evidence, and it says so above the table', () => {
    const html = renderToStaticMarkup(<EstimatesTable data={{ ...DATA, status: 'needs_climate', estimates: [] }} />);
    expect(html).toContain('Sin clima de la parcela no se puede buscar evidencia.');
    expect(html.indexOf('Sin clima de la parcela')).toBeLessThan(html.indexOf('<table'));
    expect(renderToStaticMarkup(<EstimatesTable data={DATA} />)).not.toContain('Sin clima de la parcela');
  });
  it('shows the sowing date in the user language, as the local date it is', () => {
    const html = renderToStaticMarkup(<EstimatesTable data={{ ...DATA, inputs: { ...DATA.inputs, sowing_date: '2027-01-01' } }} />);
    expect(html).toContain('siembra 1/1/2027');
    expect(renderToStaticMarkup(<EstimatesTable data={{ ...DATA, inputs: { ...DATA.inputs, sowing_date: null } }} />)).toContain('siembra sin fecha');
  });

  const AQ = { provider_id: 'aquacrop', provider_version: 'v', unit: 't/ha' as const, n: 14, n_kind: 'years' as const,
    warnings: [], attributions: [], graph_id: null };
  const EXPECTED: Estimate = { ...AQ, basis: 'gap_corrected', percentiles: null, range: { low: 2.4, high: 5.1 }, coverage: 0.78,
    moisture: { basis: 'commercial', pct: 13 }, validation: { status: 'validated', metric: 'RMSE 1.68 t/ha, anomaly r 0.50', scope: 'ES' } };
  const CEILING: Estimate = { ...AQ, basis: 'water_limited', percentiles: { p10: 3.1, p50: 4.2, p90: 5.3 }, range: null, coverage: null,
    moisture: { basis: 'commercial', pct: 13 }, validation: { status: 'not_applicable', metric: null, scope: null } };
  const withAq = { ...DATA, estimates: [...DATA.estimates, CEILING, EXPECTED], unavailable: [] };

  it('a range-only estimate shows low – high, no median, its coverage and its moisture', () => {
    const html = renderToStaticMarkup(<EstimatesTable data={withAq} />);
    expect(html).toContain('2,4 – 5,1');
    expect(html).toContain('acierta en el 78 % de los años');
    expect(html).toContain('al 13 % de humedad');
    expect(html).toContain('14 años');
    expect(html).toContain('<td class="py-2 pr-4">—</td>');
  });
  it('the expected yield comes before the ceiling, and the ceiling is labelled as such', () => {
    const html = renderToStaticMarkup(<EstimatesTable data={withAq} />);
    expect(html.indexOf('2,4 – 5,1')).toBeLessThan(html.indexOf('3,1 – 5,3'));
    expect(html).toContain('techo con el agua disponible, no es una previsión');
    expect(html.split('techo con el agua disponible').length).toBe(2); // only the ceiling row
  });
  it('trial evidence states that it keeps the moisture each source publishes', () => {
    expect(renderToStaticMarkup(<EstimatesTable data={DATA} />)).toContain('a la humedad que publica cada fuente');
  });
  it('shows the simulation progress while AquaCrop runs', () => {
    const data = { ...DATA, unavailable: [{ provider_id: 'aquacrop', reason: 'async' }] };
    expect(renderToStaticMarkup(<EstimatesTable data={data} progress={{ done: 3, total: 12 }} />)).toContain('Simulando… 3/12 años');
    expect(renderToStaticMarkup(<EstimatesTable data={data} progress={{ done: 0, total: null }} />)).toContain('Simulando…');
  });
  it('a failed engine and a calibration reason are said, with the engine code', () => {
    const html = renderToStaticMarkup(<EstimatesTable data={{ ...DATA, unavailable: [
      { provider_id: 'aquacrop', reason: 'engine_error', code: 'soil_unavailable' },
      { provider_id: 'aquacrop', reason: 'expected_outside_calibrated_regions' }] }} />);
    expect(html).toContain('la simulación ha fallado (código soil_unavailable)');
    expect(html).toContain('sin previsión de cosecha: la parcela está fuera de las provincias calibradas');
  });
  it('a failed engine offers a retry button; a gate reason does not', () => {
    const failed = { ...DATA, unavailable: [{ provider_id: 'aquacrop', reason: 'engine_error', code: 'x' }] };
    expect(renderToStaticMarkup(<EstimatesTable data={failed} onRetry={() => undefined} />)).toContain('Reintentar');
    expect(renderToStaticMarkup(<EstimatesTable data={DATA} onRetry={() => undefined} />)).not.toContain('Reintentar');
  });
  it('no 0/null while the total is unknown', () => {
    const html = renderToStaticMarkup(<EstimatesTable data={{ ...DATA, unavailable: [{ provider_id: 'aquacrop', reason: 'async' }] }} progress={{ done: 0, total: null }} />);
    expect(html).toContain('Simulando…');
    expect(html).not.toContain('null');
  });
});
