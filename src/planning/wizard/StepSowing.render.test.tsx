import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('../testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
import StepSowing from './StepSowing';
import type { CalendarResponse } from '../types';

const F = (start: string, end: string) => ({ start, end });
const CAL = { fortnights: [F('2026-10-01', '2026-10-15'), F('2026-10-16', '2026-10-31')],
  main: { window: F('2026-10-01', '2026-10-31'), window_source: 'table', default: F('2026-10-16', '2026-10-31'),
    selected: F('2026-10-16', '2026-10-31'), options: [F('2026-10-01', '2026-10-15'), F('2026-10-16', '2026-10-31')], perennial_years_default: null },
  previous: { crop: null, end: null, end_source: 'campaign_start' }, cover: null } as unknown as CalendarResponse;

describe('StepSowing (es)', () => {
  it('asks when, shows the strip and the chosen and recommended ranges in words', () => {
    const html = renderToStaticMarkup(<StepSowing cal={CAL} cropName="trigo" selected={CAL.main.selected} years={null}
      perennial={false} onSowing={() => {}} onYears={() => {}} />);
    expect(html).toContain('¿Cuándo vas a sembrar?');
    expect(html).toMatch(/Siembra de trigo: 16.*31.*octubre.*2026/);
    expect(html).toContain('Recomendado en tu zona');
  });
  it('unknown window: says so and asks for a fortnight', () => {
    const cal = { ...CAL, main: { ...CAL.main, window: null, default: null, selected: null } };
    expect(renderToStaticMarkup(<StepSowing cal={cal} cropName="trigo" selected={null} years={null} perennial={false}
      onSowing={() => {}} onYears={() => {}} />)).toContain('No conocemos la ventana de siembra');
  });
  it('a perennial asks for its years', () => {
    expect(renderToStaticMarkup(<StepSowing cal={CAL} cropName="alfalfa" selected={CAL.main.selected} years={4} perennial
      onSowing={() => {}} onYears={() => {}} />)).toContain('¿Cuántos años?');
  });
  it('a chosen sowing outside the offered fortnights is still drawn, pressed', () => {
    const html = renderToStaticMarkup(<StepSowing cal={CAL} cropName="trigo" selected={F('2026-12-01', '2026-12-15')} years={null}
      perennial={false} onSowing={() => {}} onYears={() => {}} />);
    expect(html).toMatch(/aria-pressed="true"[^>]*aria-label="[^"]*diciembre/);
  });
});
