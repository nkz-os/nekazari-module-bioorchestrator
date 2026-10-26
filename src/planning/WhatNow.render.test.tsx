import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: (ns?: string) => {
    if (ns !== 'bioorchestrator') throw new Error(`wrong namespace: ${ns}`);
    return { t: esT, i18n: { language: 'es' } };
  }, i18n: {} };
});
vi.mock('react-router-dom', () => ({ useNavigate: () => vi.fn() }));

import WhatNow from './WhatNow';
import type { WorklistRow } from './cropHealthClient';

const row = (o: Partial<WorklistRow>): WorklistRow => ({ parcelId: 'p1', parcelName: 'Soto Alto', nextJob: null, surprise: null, cropStatus: 'growing', ...o });

describe('WhatNow (es)', () => {
  it('renders job and surprise per parcel with a link to crop health', () => {
    const html = renderToStaticMarkup(<WhatNow rows={[
      row({ nextJob: { kind: 'sow', when: null, detail: { species: 'cebada' } }, surprise: { code: 'new_high_alert', detail: { disease: 'roya' } } }),
      row({ parcelId: 'p2', parcelName: 'El Raso', nextJob: { kind: 'none_until', when: '2027-03-01', detail: { date: '2027-03-01' } } }),
      row({ parcelId: 'p3', parcelName: 'Quieta' }),
    ]} />);
    expect(html).toContain('Qué toca ahora');
    expect(html).toContain('Soto Alto');
    expect(html).toContain('Sembrar cebada');
    expect(html).toContain('Alerta nueva: roya');
    expect(html).toContain('Nada hasta 2027-03-01');
    expect(html).toContain('/crop-health?parcel=p1');
    expect(html).toContain('Ver en salud del cultivo');
    expect(html).not.toContain('Quieta');
    expect(html).not.toContain('Sin sorpresas');
  });

  it('renders a row with a surprise but no job', () => {
    const html = renderToStaticMarkup(<WhatNow rows={[row({ surprise: { code: 'water_drop', detail: {} } })]} />);
    expect(html).toContain('Soto Alto');
    expect(html).toContain('Caída brusca de la reserva de agua');
    expect(html).not.toContain('Sin sorpresas');
  });

  it('shows the quiet text when nothing is due', () => {
    const html = renderToStaticMarkup(<WhatNow rows={[row({})]} />);
    expect(html).toContain('Sin sorpresas ni trabajos pendientes');
    expect(html).not.toContain('Soto Alto');
  });
});
