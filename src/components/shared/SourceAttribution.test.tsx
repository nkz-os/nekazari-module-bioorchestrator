import React from 'react';
/** Server-side render of the attribution block with the real Spanish strings. */
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import es from '../../locales/es.json';

vi.mock('@nekazari/sdk', () => {
  const get = (k: string) => k.split('.').reduce<unknown>((n, p) => (n && typeof n === 'object' ? (n as Record<string, unknown>)[p] : undefined), es);
  const t = (key: string) => (typeof get(key) === 'string' ? (get(key) as string) : key);
  return { useTranslation: () => ({ t, i18n: { language: 'es' } }), i18n: {} };
});

import SourceAttribution from './SourceAttribution';

/** Visible text of the markup: links split the credit line, never change its characters. */
const textOf = (html: string) => html.replace(/<[^>]+>/g, '');
import type { SourceAttributionItem } from '../../types/attribution';

const GENVCE_TEXT = 'Fuente: Datos Abiertos GENVCE. Url: https://genvce.org/mapa-de-resultados/ (Descarga: 01/06/2026.)';
const genvce: SourceAttributionItem = {
  source_id: 'GENVCE', text: GENVCE_TEXT, url: 'https://genvce.org/mapa-de-resultados/',
  licence_id: 'genvce-reuse-notice-ley-37-2007', licence_url: 'https://genvce.org/',
};
const crea: SourceAttributionItem = {
  source_id: 'CREA', text: 'Fonte: CREA - Consiglio per la ricerca in agricoltura e l\'analisi dell\'economia agraria. Licenza CC BY 3.0 IT.',
  url: 'https://www.crea.gov.it/', licence_id: 'CC-BY-3.0-IT', licence_url: 'https://creativecommons.org/licenses/by/3.0/it/legalcode',
};
const CALCULATIONS = 'Las estimaciones y medias son cálculos de Nekazari a partir de estos datos; no son datos publicados por las fuentes.';

describe('SourceAttribution (es)', () => {
  it('shows the exact credit line of each source and the Nekazari-calculations notice', () => {
    const html = renderToStaticMarkup(<SourceAttribution attributions={[genvce, crea]} />);
    expect(textOf(html)).toContain(GENVCE_TEXT);
    expect(textOf(html)).toContain('Consiglio per la ricerca in agricoltura');
    expect(textOf(html)).toContain(CALCULATIONS);
    expect(html).toContain('href="https://creativecommons.org/licenses/by/3.0/it/legalcode"');
  });

  it('links the URL inside a credit line that carries it, with no extra source link', () => {
    const html = renderToStaticMarkup(<SourceAttribution attributions={[genvce]} />);
    expect(textOf(html)).toContain(GENVCE_TEXT);  // the credit line is unchanged
    expect(html.match(/href="https:\/\/genvce\.org\/mapa-de-resultados\/"/g)).toHaveLength(1);
    expect(html).not.toContain('>Fuente</a>');
  });

  it('adds a source link when the credit line does not carry its URL', () => {
    const html = renderToStaticMarkup(<SourceAttribution attributions={[crea]} />);
    expect(html).toContain('href="https://www.crea.gov.it/"');
    expect(html).toContain('>Fuente</a>');
  });

  it('renders nothing without attributions (older backend, or no source with a credit)', () => {
    expect(renderToStaticMarkup(<SourceAttribution />)).toBe('');
    expect(renderToStaticMarkup(<SourceAttribution attributions={null} />)).toBe('');
    expect(renderToStaticMarkup(<SourceAttribution attributions={[]} />)).toBe('');
  });

  it('never renders a non-http link', () => {
    const html = renderToStaticMarkup(
      <SourceAttribution attributions={[{ ...genvce, url: 'javascript:alert(1)', licence_url: 'data:text/html,x' }]} />);
    expect(textOf(html)).toContain(GENVCE_TEXT);
    expect(html).not.toContain('javascript:');
    expect(html).not.toContain('href=');
  });
});
