import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  buildRecommendQuery, fetchEvidence, fetchRecommendConditions, fetchRecommendParcel,
} from './recommendApi';

afterEach(() => vi.unstubAllGlobals());

function stubFetch(status = 200, body: unknown = { status: 'ok' }) {
  const fn = vi.fn().mockResolvedValue({ ok: status >= 200 && status < 300, status, json: async () => body });
  vi.stubGlobal('fetch', fn);
  return fn;
}

describe('buildRecommendQuery', () => {
  it('drops undefined, null, empty string and empty arrays', () => {
    expect(buildRecommendQuery({ a: undefined, b: null, c: '', d: [], e: [''] })).toBe('');
  });
  it('joins arrays and keeps 0 / false', () => {
    expect(buildRecommendQuery({ crops: ['TRZAX', 'HORVX'], n: 0, f: false })).toBe('crops=TRZAX%2CHORVX&n=0&f=false');
  });
  it('encodes values', () => {
    expect(buildRecommendQuery({ irrigation_regime: 'regadío' })).toBe('irrigation_regime=regad%C3%ADo');
  });
});

describe('fetch functions', () => {
  it('parcel: encodes id, omits empty overrides, sends credentials', async () => {
    const f = stubFetch();
    await fetchRecommendParcel('urn:ngsi-ld:AgriParcel:x', { climate_class: '', top_n: 5 });
    const [url, init] = f.mock.calls[0];
    expect(url).toContain('/api/graph/recommend/parcel/urn%3Angsi-ld%3AAgriParcel%3Ax?top_n=5');
    expect(url).not.toContain('climate_class');
    expect(init.credentials).toBe('include');
  });
  it('conditions: hits the public endpoint with params', async () => {
    const f = stubFetch();
    await fetchRecommendConditions({ climate_class: 'Csa', soil_ph: undefined });
    expect(f.mock.calls[0][0]).toMatch(/\/api\/graph\/agriculture\/recommend\?climate_class=Csa$/);
  });
  it('evidence: adds crop, page and similarity', async () => {
    const f = stubFetch();
    await fetchEvidence({ climate_class: 'Csa' }, 'TRZAX', 2, 'vector_v2_fallback');
    const url = f.mock.calls[0][0] as string;
    expect(url).toContain('/api/graph/agriculture/recommend/evidence?');
    const qs = new URLSearchParams(url.split('?')[1]);
    expect(Object.fromEntries(qs)).toEqual({
      climate_class: 'Csa', crop: 'TRZAX', page: '2', similarity: 'vector_v2_fallback' });
  });
  it('throws Error(<status>) on non-2xx', async () => {
    stubFetch(422);
    await expect(fetchRecommendConditions({ climate_class: 'Csa' })).rejects.toThrow('422');
    stubFetch(500);
    await expect(fetchRecommendParcel('p')).rejects.toThrow('500');
    stubFetch(404);
    await expect(fetchEvidence({}, 'X')).rejects.toThrow('404');
  });
});
