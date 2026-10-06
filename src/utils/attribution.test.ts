import { describe, expect, it } from 'vitest';
import { attributionsForRecs, attributionsForSources, normalizeAttributions, safeHref, splitAtUrl } from './attribution';

const genvce = {
  source_id: 'GENVCE',
  text: 'Fuente: Datos Abiertos GENVCE. Url: https://genvce.org/mapa-de-resultados/ (Descarga: 01/06/2026.)',
  url: 'https://genvce.org/mapa-de-resultados/',
  licence_id: 'genvce-reuse-notice-ley-37-2007',
  licence_url: 'https://genvce.org/',
};

describe('normalizeAttributions', () => {
  it('keeps the credit text verbatim and the order received', () => {
    const crea = { ...genvce, source_id: 'CREA', text: 'Fonte: CREA' };
    const out = normalizeAttributions([crea, genvce]);
    expect(out.map((a) => a.source_id)).toEqual(['CREA', 'GENVCE']);
    expect(out[1].text).toBe(genvce.text);
  });

  it('is empty for an answer without attributions (older backend) or a non-list', () => {
    expect(normalizeAttributions(undefined)).toEqual([]);
    expect(normalizeAttributions(null)).toEqual([]);
    expect(normalizeAttributions('GENVCE')).toEqual([]);
    expect(normalizeAttributions({ source_id: 'GENVCE' })).toEqual([]);
    expect(normalizeAttributions([])).toEqual([]);
  });

  it('drops malformed items and keeps the valid ones', () => {
    const out = normalizeAttributions([null, 3, 'x', {}, { source_id: 'A' }, { text: 'B' }, { source_id: ' ', text: 't' }, genvce]);
    expect(out.map((a) => a.source_id)).toEqual(['GENVCE']);
  });

  it('lists a source once (the first wins)', () => {
    const out = normalizeAttributions([genvce, { ...genvce, text: 'other' }]);
    expect(out).toHaveLength(1);
    expect(out[0].text).toBe(genvce.text);
  });

  it('fills missing link fields with empty strings', () => {
    expect(normalizeAttributions([{ source_id: 'X', text: 'Credit' }])).toEqual([
      { source_id: 'X', text: 'Credit', url: '', licence_id: '', licence_url: '' },
    ]);
  });
});

describe('safeHref', () => {
  it('accepts http and https links only', () => {
    expect(safeHref('https://genvce.org/mapa-de-resultados/')).toBe('https://genvce.org/mapa-de-resultados/');
    expect(safeHref('http://creativecommons.org/licenses/by/3.0/it/legalcode')).toContain('creativecommons.org');
    expect(safeHref('javascript:alert(1)')).toBeNull();
    expect(safeHref('data:text/html,x')).toBeNull();
    expect(safeHref('/relative/path')).toBeNull();
    expect(safeHref('')).toBeNull();
    expect(safeHref(undefined)).toBeNull();
    expect(safeHref(null)).toBeNull();
  });
});

const crea = { ...genvce, source_id: 'CREA', text: 'Fonte: CREA' };
const ahdb = { ...genvce, source_id: 'AHDB', text: 'Source: AHDB' };

describe('attributionsForSources', () => {
  it('keeps only the attributions of the given sources, in the order received', () => {
    expect(attributionsForSources([crea, genvce, ahdb], ['GENVCE', 'CREA']).map((a) => a.source_id))
      .toEqual(['CREA', 'GENVCE']);
  });

  it('is empty when no given source has an attribution, or the answer has none', () => {
    expect(attributionsForSources([genvce], ['CREA'])).toEqual([]);
    expect(attributionsForSources(undefined, ['GENVCE'])).toEqual([]);
    expect(attributionsForSources([genvce], [])).toEqual([]);
    expect(attributionsForSources([genvce], [null, undefined])).toEqual([]);
  });
});

describe('attributionsForRecs', () => {
  const rec = (sources: string[]) => ({ evidence: { sources } });

  it('limits the answer attributions to the sources of the given recommendations', () => {
    expect(attributionsForRecs([crea, genvce], [rec(['GENVCE'])]).map((a) => a.source_id)).toEqual(['GENVCE']);
    expect(attributionsForRecs([crea, genvce], [rec(['GENVCE']), rec(['CREA', 'GENVCE'])]).map((a) => a.source_id))
      .toEqual(['CREA', 'GENVCE']);
  });

  it('tolerates a recommendation without evidence sources', () => {
    expect(attributionsForRecs([genvce], [rec([]), { evidence: {} as { sources: string[] } }])).toEqual([]);
  });
});

describe('processing note', () => {
  it('is kept when it has text and dropped when it has none', () => {
    const note = { es: 'Nota', en: 'Note' };
    expect(normalizeAttributions([{ ...genvce, processing_note: note }])[0].processing_note).toEqual(note);
    expect(normalizeAttributions([{ ...genvce, processing_note: { es: ' ', en: 3 } }])[0].processing_note).toBeUndefined();
    expect(normalizeAttributions([{ ...genvce, processing_note: ['x'] }])[0].processing_note).toBeUndefined();
    expect(normalizeAttributions([genvce])[0].processing_note).toBeUndefined();
  });
});

describe('splitAtUrl', () => {
  it('splits a credit line around its own URL without losing a character', () => {
    const parts = splitAtUrl(genvce.text, genvce.url);
    expect(parts).not.toBeNull();
    expect(`${parts!.before}${parts!.url}${parts!.after}`).toBe(genvce.text);
    expect(parts!.url).toBe('https://genvce.org/mapa-de-resultados/');
  });

  it('is null when the text does not carry the URL, or there is no URL', () => {
    expect(splitAtUrl('Fonte: CREA', 'https://www.crea.gov.it/')).toBeNull();
    expect(splitAtUrl('Fonte: CREA', '')).toBeNull();
  });
});
