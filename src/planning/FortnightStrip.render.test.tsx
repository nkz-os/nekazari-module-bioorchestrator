import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import FortnightStrip from './FortnightStrip';

const F = (start: string, end: string) => ({ start, end });
const ALL = [F('2026-10-01', '2026-10-15'), F('2026-10-16', '2026-10-31'), F('2026-11-01', '2026-11-15'), F('2026-11-16', '2026-11-30')];

describe('FortnightStrip (es)', () => {
  it('one button per fortnight with its full localized range as label', () => {
    const html = renderToStaticMarkup(<FortnightStrip fortnights={ALL} selected={ALL[1]} label="Siembra" onSelect={() => {}} />);
    expect(html.split('<button').length - 1).toBe(4);
    expect(html).toMatch(/aria-label="[^"]*16[^"]*31[^"]*octubre[^"]*2026"/);
    expect(html).toContain('aria-pressed="true"');
  });
  it('marks the recommended window and the cover bar', () => {
    const html = renderToStaticMarkup(<FortnightStrip fortnights={ALL} recommended={ALL.slice(1, 3)} selected={null}
      bar={{ start: '2026-10-01', end: '2026-10-31' }} label="Cubierta" />);
    expect(html.split('data-recommended="true"').length - 1).toBe(2);
    expect(html.split('data-bar="true"').length - 1).toBe(2);
  });
  it('without onSelect the cells are not buttons (read-only bar)', () => {
    expect(renderToStaticMarkup(<FortnightStrip fortnights={ALL} selected={null} label="x" />)).not.toContain('<button');
  });
  it('cells outside the selectable ones are read-only (the end of a cover bar)', () => {
    const html = renderToStaticMarkup(<FortnightStrip fortnights={ALL} selectable={ALL.slice(0, 2)} selected={null} label="x" onSelect={() => {}} />);
    expect(html.split('<button').length - 1).toBe(2);
    expect(html.split('data-bar=').length - 1).toBe(4);
  });
});
