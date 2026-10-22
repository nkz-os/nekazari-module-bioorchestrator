import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import Step from './Step';

describe('Step (es)', () => {
  it('says what it is loading, with a generic text when none is given', () => {
    const own = renderToStaticMarkup(<Step title="2" loading loadingText="Buscando variedades…"><p>x</p></Step>);
    expect(own).toContain('Buscando variedades…');
    expect(own).toContain('aria-busy="true"');
    expect(own).not.toContain('<p>x</p>');
    expect(renderToStaticMarkup(<Step title="2" loading />)).toContain('Cargando…');
  });
  it('shows its content once loaded, with no loading line', () => {
    const html = renderToStaticMarkup(<Step title="2" loadingText="Buscando variedades…"><p>x</p></Step>);
    expect(html).toContain('<p>x</p>');
    expect(html).not.toContain('aria-busy');
  });
});
