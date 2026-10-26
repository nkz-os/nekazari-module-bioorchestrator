import { afterEach, describe, expect, it, vi } from 'vitest';
import { fetchWorklist } from './cropHealthClient';

const ROW = { parcelId: 'p1', nextJob: null, surprise: null, cropStatus: 'growing' };

describe('fetchWorklist', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('returns the rows with credentials on the absolute-origin path', async () => {
    const f = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ parcels: [ROW], summary: {} }) });
    vi.stubGlobal('fetch', f);
    expect(await fetchWorklist()).toEqual([ROW]);
    expect(f.mock.calls[0][0]).toMatch(/\/api\/crop-health\/parcels$/);
    expect(f.mock.calls[0][1]).toMatchObject({ credentials: 'include' });
  });

  it.each([403, 404])('returns null on %i', async (status) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status, json: async () => ({}) }));
    expect(await fetchWorklist()).toBeNull();
  });

  it('returns null when the network fails', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('down')));
    expect(await fetchWorklist()).toBeNull();
  });

  it('returns null on any other error status, a body without parcels or rows without nextJob', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 500, json: async () => ({}) }));
    expect(await fetchWorklist()).toBeNull();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ x: 1 }) }));
    expect(await fetchWorklist()).toBeNull();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => [ROW] }));
    expect(await fetchWorklist()).toBeNull();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ parcels: [{ parcelId: 'p1', cropStatus: 'none' }] }) }));
    expect(await fetchWorklist()).toBeNull();
  });
});
