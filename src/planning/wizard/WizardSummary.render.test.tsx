import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('../testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
import WizardSummary from './WizardSummary';

describe('WizardSummary (es)', () => {
  it('lists the plan, the yield line, the detail toggle and Guardar', () => {
    const html = renderToStaticMarkup(<WizardSummary lines={['Trigo · RGT Tocayo', 'Siembra: 16–31 de octubre de 2026']}
      yieldLine={{ low: 4, high: 6 }} detail={<p>tabla</p>} replacing={false} saving={false} error={null} regimeNotice={false} onSave={() => {}} />);
    expect(html).toContain('Tu plan');
    expect(html).toContain('Cosecha esperada 4–6 t/ha');
    expect(html).toContain('Ver detalle');
    expect(html).toContain('Guardar plan');
  });
  it('a failed regime save is a non-blocking notice', () => {
    const html = renderToStaticMarkup(<WizardSummary lines={[]} yieldLine={null} detail={null} replacing={false} saving={false}
      error={null} regimeNotice onSave={() => {}} />);
    expect(html).toContain('no se pudo recordar secano/regadío');
  });
});
