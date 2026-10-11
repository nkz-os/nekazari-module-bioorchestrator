import { afterEach, describe, expect, it, vi } from 'vitest';
import { buildQuery, fetchFit, PlanningApiError, putPlan } from './api';

afterEach(() => vi.unstubAllGlobals());

describe('planning api client', () => {
  it('drops empty params and encodes the parcel URN in the path', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ species: [] }) });
    vi.stubGlobal('fetch', fetchMock);
    await fetchFit('urn:ngsi-ld:AgriParcel:t:p1', 2027, null);
    const url = String(fetchMock.mock.calls[0][0]);
    expect(url).toContain('/api/graph/planning/parcels/urn%3Angsi-ld%3AAgriParcel%3At%3Ap1/fit?campaign=2027');
    expect(url).not.toContain('regime');
    expect(buildQuery({ a: '', b: null, c: 0 })).toBe('?c=0');
  });

  it('turns a coded backend error into PlanningApiError', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 409, json: async () => ({ detail: { code: 'campaign_started' } }) }));
    await expect(putPlan('p1', { campaign: 2027, regime: 'secano', management: 'organic', cycles: [] }))
      .rejects.toMatchObject({ status: 409, code: 'campaign_started' });
    expect(new PlanningApiError(500, null).code).toBeNull();
  });
});
