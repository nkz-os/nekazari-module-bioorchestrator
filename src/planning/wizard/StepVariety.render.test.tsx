import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('../testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
import StepVariety, { topVarieties } from './StepVariety';
import type { VarietyStat } from '../types';

const v = (variety: string, rel: number, stability: VarietyStat['stability']): VarietyStat =>
  ({ variety, relative_yield_pct: rel, n_environments: 6, years: [2024], mean_kg_ha: 6000, fw_slope: 1, stability, few_environments: false });

describe('StepVariety', () => {
  it('the top three: the most productive, the most stable, and a balanced one, without repeats', () => {
    const top = topVarieties([v('A', 110, 'responsive'), v('B', 104, 'stable'), v('C', 108, 'average'), v('D', 95, 'stable')]);
    expect(top.map((x) => [x.variety, x.why])).toEqual([['A', 'productive'], ['B', 'stable'], ['C', 'balanced']]);
  });
  it('offers "no variety" pre-selected and "see all"', () => {
    const html = renderToStaticMarkup(<StepVariety data={{ status: 'ok', varieties: [v('A', 110, 'stable')] } as never}
      selected={null} onSelect={() => {}} />);
    expect(html).toContain('Sin variedad / lo decido luego');
    expect(html).toContain('Ver todas las variedades');
    expect(html).toContain('La más productiva en tu zona');
  });
});
