import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('../testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
import { CoverReferenceCard, coverSeason } from './CoverReference';
import type { CoverReferenceResponse } from '../types';

const M = (mean: number, se: number | null, n: number) => ({ mean, se, n });
const ok: CoverReferenceResponse = {
  species: 'hairy_vetch', season_requested: 'autumn', season_used: 'autumn', status: 'ok', coverage: 'europe_mostly_north_central',
  aerial: { dm_t_ha: M(4.99, 0.44, 15), n_pct: M(3.95, 0.21, 20), c_n: M(9.56, 0.48, 12), n_accumulated_kg_ha: 197 },
  attributions: [{ source_id: 'thiebeau_inrae_2021', text: 'Source: Thiébeau P. et al., Data INRAE, v3.', url: 'https://doi.org/10.15454/LBI3U7',
    licence_id: 'etalab-2.0', licence_url: 'https://www.etalab.gouv.fr/licence-ouverte-open-licence/' }],
};

describe('coverSeason', () => {
  it('maps the sowing start month: 8-12 autumn, 1-7 spring', () => {
    expect(coverSeason('2026-08-01')).toBe('autumn');
    expect(coverSeason('2026-12-16')).toBe('autumn');
    expect(coverSeason('2027-01-01')).toBe('spring');
    expect(coverSeason('2027-07-16')).toBe('spring');
  });
});

describe('CoverReferenceCard (es)', () => {
  it('shows mean ± SE and n, the derived N with its caveat, coverage and attribution', () => {
    const html = renderToStaticMarkup(<CoverReferenceCard view={{ data: ok, error: null, loading: false }} />);
    expect(html).toContain('4,99 ± 0,44 (n = 15)');
    expect(html).toContain('3,95 ± 0,21 (n = 20)');
    expect(html).toContain('9,56 ± 0,48 (n = 12)');
    expect(html).toContain('197 kg N/ha');
    expect(html).toContain('no es N fijado ni disponible');
    expect(html).toContain('Media de ensayos europeos, mayoría del norte y centro de Europa');
    expect(html).toContain('Thiébeau P. et al.');
  });
  it('omits the derived N when it is null and labels a season-less row', () => {
    const data = { ...ok, season_used: 'unspecified' as const, aerial: { ...ok.aerial!, n_accumulated_kg_ha: null, c_n: null } };
    const html = renderToStaticMarkup(<CoverReferenceCard view={{ data, error: null, loading: false }} />);
    expect(html).not.toContain('kg N/ha');
    expect(html).toContain('sin época indicada');
  });
  it('no_data says so, with no numbers', () => {
    const data = { ...ok, status: 'no_data' as const, season_used: null, aerial: null };
    const html = renderToStaticMarkup(<CoverReferenceCard view={{ data, error: null, loading: false }} />);
    expect(html).toContain('Sin datos de referencia para esta especie y época');
    expect(html).not.toContain('±');
  });
  it('loading and error are said, not hidden', () => {
    expect(renderToStaticMarkup(<CoverReferenceCard view={{ data: null, error: null, loading: true }} />)).toContain('aria-busy="true"');
    expect(renderToStaticMarkup(<CoverReferenceCard view={{ data: null, error: new Error('x'), loading: false }} />))
      .toContain('No se pudo cargar la referencia');
  });
});
