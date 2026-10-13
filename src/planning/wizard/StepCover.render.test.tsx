import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('../testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
import StepCover from './StepCover';
import type { CalendarResponse } from '../types';

const F = (start: string, end: string) => ({ start, end });
const base = { fortnights: [F('2026-08-01', '2026-08-15'), F('2026-08-16', '2026-08-31')], main: { selected: F('2026-10-16', '2026-10-31') } };
const props = { coverOptions: [], other: false, onOther: () => {}, onOn: () => {}, onSpecies: () => {}, onSowing: () => {}, onMethod: () => {}, species: 'vetch',
  sowing: F('2026-08-01', '2026-08-15'), method: 'roller_crimper' as const };

describe('StepCover (es)', () => {
  it('default No; with Sí shows growth in words and the end rule', () => {
    const cal = { ...base, cover: { fits: true, default: F('2026-08-01', '2026-08-15'), options: [F('2026-08-01', '2026-08-15')],
      ends: '2026-10-16', weeks: 10, short: false } } as unknown as CalendarResponse;
    expect(renderToStaticMarkup(<StepCover cal={cal} on={false} {...props} />)).toContain('¿Cubierta antes?');
    const on = renderToStaticMarkup(<StepCover cal={cal} on {...props} />);
    expect(on).toMatch(/La cubierta crece del .*agosto.*unas 10 semanas/);
    expect(on).toContain('al sembrar el cultivo principal');
  });
  it('a short cover is warned about, not refused', () => {
    const cal = { ...base, cover: { fits: true, default: F('2026-08-16', '2026-08-31'), options: [F('2026-08-16', '2026-08-31')],
      ends: '2026-10-01', weeks: 6, short: true } } as unknown as CalendarResponse;
    expect(renderToStaticMarkup(<StepCover cal={cal} on {...props} />)).toContain('Poco tiempo para una cubierta');
  });
  it('no room: says so and offers no Sí', () => {
    const cal = { ...base, cover: { fits: false, default: null, options: [], ends: '2026-10-16', weeks: null, short: false } } as unknown as CalendarResponse;
    const html = renderToStaticMarkup(<StepCover cal={cal} on={false} {...props} />);
    expect(html).toContain('No cabe una cubierta');
    expect(html).not.toContain('>Sí<');
  });
  it('the cover strip only offers fortnights that fit before the main sowing', () => {
    const fns = ['2026-08-01', '2026-08-16', '2026-09-01', '2026-09-16', '2026-10-01', '2026-10-16'].map((d) => F(d, d));
    const cal = { fortnights: fns, main: { selected: fns[5] }, cover: { fits: true, default: fns[1], options: fns.slice(1, 5),
      ends: '2026-10-16', weeks: 8, short: false } } as unknown as CalendarResponse;
    const html = renderToStaticMarkup(<StepCover cal={cal} on {...props} sowing={fns[1]} />);
    expect(html.split('data-recommended=').length - 1).toBe(4); // strip cells only
  });
  it('"Otra especie" opens the name field even before anything is typed', () => {
    const cal = { ...base, cover: { fits: true, default: F('2026-08-01', '2026-08-15'), options: [F('2026-08-01', '2026-08-15')],
      ends: '2026-10-16', weeks: 10, short: false } } as unknown as CalendarResponse;
    const html = renderToStaticMarkup(<StepCover cal={cal} on {...props} species="" other />);
    expect(html).toContain('Nombre de la especie');
    expect(renderToStaticMarkup(<StepCover cal={cal} on {...props} species="" />))
      .not.toContain('Nombre de la especie');
  });
});
