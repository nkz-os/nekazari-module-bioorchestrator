import React from 'react';
/** Server-side render of the cards and the regional section with the real Spanish strings. */
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import es from '../../locales/es.json';

vi.mock('@nekazari/sdk', () => {
  const lookup = (key: string, opts?: Record<string, unknown>) => {
    const dict = es as unknown as Record<string, unknown>;
    const get = (k: string) => k.split('.').reduce<unknown>((n, p) => (n && typeof n === 'object' ? (n as Record<string, unknown>)[p] : undefined), dict);
    let v = get(key);
    if (typeof v !== 'string' && opts && typeof opts.count === 'number') v = get(key + (opts.count === 1 ? '_one' : '_other'));
    if (typeof v !== 'string') return (opts?.defaultValue as string) ?? key;
    return v.replace(/\{\{(\w+)\}\}/g, (_m, k: string) => String(opts?.[k] ?? ''));
  };
  return { useTranslation: () => ({ t: lookup, i18n: { language: 'es' } }), i18n: {} };
});

import RecommendationCard from './RecommendationCard';
import RegionalList from './RegionalList';
import type { Recommendation } from '../../types/recommend';

function base(eppo: string): Recommendation {
  return {
    recommendation_id: eppo, crop: { eppo, scientific_name: eppo, sowing_type: 'autumn' },
    fit: { relative_yield_pct: 12.5, stability_cv: 0.2, reference: { median_kg_ha: 3000, n_trials: 9, scope: 'analog_sites:Csa:secano' } },
    yield: { expected_kg_ha: 3500, interval: [3000, 4000], interval_method: 'observed_range', basis: null, sd: 100, n_trials: 5, n_sites: 3 },
    suitability: { soil: { level: 'suitable', warnings: [] }, water: { level: 'low', etc_mm: null }, frost: { level: 'none' } },
    season: { sowing_window: null, cycle_days: null, source: null },
    trust: { level: 'medium', data_gaps: [], similarity: 'koppen' },
    varieties: [],
    evidence: { trial_count: 5, sources: ['X'], sites: ['S'], years: [2020, 2022], tier: 'field', purpose: 'main', regional_trial_count: 7, other_purpose_trials: { forage: 115 }, unknown_basis_trials: null },
    assumptions: [],
  };
}
const noop = () => {};
const card = (rec: Recommendation, expert = false) => renderToStaticMarkup(
  <RecommendationCard rec={rec} scaleMax={5000} expert={expert} compared={false} compareDisabled={false}
    onToggleCompare={noop} onChooseVariety={noop} onOpenEvidence={noop} onReportValue={noop} onViewForage={noop} policyVersion="2026-10-04.4" />);

describe('what-to-sow components render the evidence states (es)', () => {
  it('harvest card: forage notice, reference scope and policy version in expert details', () => {
    const html = card(base('ZEAMX'), true);
    expect(html).toContain('115 ensayos de este cultivo como forraje');
    expect(html).toContain('Ver como forraje');
    expect(html).toContain('3500 kg/ha');
    expect(html).toContain('sitios análogos Csa · secano');
    expect(html).toContain('2026-10-04.4');
  });
  it('an older backend has no forage mode: the notice is not offered even if counts are present', () => {
    const html = renderToStaticMarkup(
      <RecommendationCard rec={base('ZEAMX')} scaleMax={5000} expert={false} compared={false} compareDisabled={false}
        onToggleCompare={noop} onChooseVariety={noop} onOpenEvidence={noop} onReportValue={noop} policyVersion={null} />);
    expect(html).not.toContain('como forraje');
    expect(html).not.toContain('Ver como forraje');
    expect(html).toContain('3500 kg/ha');
  });
  it('forage card: t MS/ha with a number, "not comparable" with unknown-basis trials only', () => {
    const f = base('ZEAMX');
    f.yield = { ...f.yield, expected_kg_ha: 30106, interval: [28000, 32000], basis: 'dry_matter' };
    f.evidence = { ...f.evidence, purpose: 'forage', other_purpose_trials: {}, unknown_basis_trials: 2 };
    const html = card(f);
    expect(html).toContain('30,1 t MS/ha');
    expect(html).toContain('rango observado 28');
    const u = base('SETIT');
    u.yield = { ...u.yield, expected_kg_ha: null, interval: [null, null], n_trials: 9 };
    u.trust.data_gaps = ['forage_basis_unknown'];
    u.evidence = { ...u.evidence, purpose: 'forage', other_purpose_trials: {} };
    const h2 = card(u);
    expect(h2).toContain('Rendimiento no comparable');
    expect(h2).toContain('9 ensayos como forraje');
  });
  it('regional section: own title, tier badge, measured and presence-only rows', () => {
    const r1 = base('PIBAR'); r1.evidence = { ...r1.evidence, tier: 'regional', other_purpose_trials: {} };
    r1.yield = { ...r1.yield, expected_kg_ha: 4025, n_trials: 3 };
    r1.fit.reference = { median_kg_ha: null, n_trials: 0, scope: 'regional' };
    const r2 = base('LINUS'); r2.evidence = { ...r2.evidence, tier: 'regional', other_purpose_trials: {} };
    r2.yield = { ...r2.yield, expected_kg_ha: null, interval: [null, null], n_trials: 900 };
    r2.trust.data_gaps = ['no_measured_yield', 'regional_evidence_only', 'no_expected_yield'];
    const html = renderToStaticMarkup(<RegionalList recs={[r1, r2]} expert policyVersion="v" noFieldEvidence={false} onOpenEvidence={noop} onReportValue={noop} />);
    expect(html).toContain('Evidencia regional/nacional');
    expect(html).toContain('4025 kg/ha · 3 ensayos');
    expect(html).toContain('Sin rendimiento medido · 900 ensayos');
    expect(html).toContain('registros regionales/nacionales');
  });
});
