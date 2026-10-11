import { afterEach, describe, expect, it, vi } from 'vitest';
import { buildQuery, fetchEstimates, fetchFit, PlanningApiError, putPlan } from './api';

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

  it('accepts a minor contract bump and rejects a major one', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ contract_version: '1.1', species: [] }) }));
    await expect(fetchFit('p1', 2027, null)).resolves.toMatchObject({ contract_version: '1.1' });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ contract_version: '2.0', species: [] }) }));
    await expect(fetchFit('p1', 2027, null)).rejects.toMatchObject({ status: 200, code: 'contract_mismatch' });
  });
  it('turns a coded backend error into PlanningApiError', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 409, json: async () => ({ detail: { code: 'campaign_started' } }) }));
    await expect(putPlan('p1', { campaign: 2027, regime: 'secano', management: 'organic', cycles: [] }))
      .rejects.toMatchObject({ status: 409, code: 'campaign_started' });
    expect(new PlanningApiError(500, null).code).toBeNull();
  });
  it('maps sowingDate to sowing_date and sends only the parameters that are set', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ estimates: [] }) });
    vi.stubGlobal('fetch', fetchMock);
    await fetchEstimates('p1', { crop: 'wheat', variety: null, regime: 'secano', management: 'organic', sowingDate: '2026-11-01' });
    const url = String(fetchMock.mock.calls[0][0]);
    expect(url).toContain('/api/graph/planning/parcels/p1/estimates?crop=wheat&regime=secano&management=organic&sowing_date=2026-11-01');
    expect(url).not.toContain('variety');
  });

  it('sends a PUT with a JSON body, its content type and the session cookie', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ written: [], removed: [] }) });
    vi.stubGlobal('fetch', fetchMock);
    const body = { campaign: 2027, regime: 'secano' as const, management: 'organic' as const, cycles: [] };
    await putPlan('p1', body);
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain('/api/graph/planning/parcels/p1/plan');
    expect(init.method).toBe('PUT');
    expect(init.credentials).toBe('include');
    expect(init.body).toBe(JSON.stringify(body));
    expect((init.headers as Record<string, string>)['Content-Type']).toBe('application/json');
  });

  it('a 2xx answer whose body is not JSON is invalid_response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => { throw new SyntaxError('Unexpected token <'); } }));
    await expect(fetchFit('p1', 2027, null)).rejects.toMatchObject({ status: 200, code: 'invalid_response' });
  });

  it('the error is named PlanningApiError', () => {
    const err = new PlanningApiError(500, null);
    expect(err.name).toBe('PlanningApiError');
    expect(err).toBeInstanceOf(PlanningApiError);
  });
});
