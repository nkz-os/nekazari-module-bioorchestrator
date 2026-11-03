import { describe, expect, it, vi } from 'vitest';
import { PlanningApiError } from './api';
import { saveRegimePick } from './completionModel';

const NONE = { regime: null, regime_source: 'unknown' } as const;
const OWN = { regime: 'rainfed', regime_source: 'parcel:irrigationRegime' } as const;

describe('saveRegimePick (spec §6)', () => {
  it('saves at once when the parcel has no regime of its own', async () => {
    const save = vi.fn().mockResolvedValue(undefined);
    expect(await saveRegimePick(NONE, 'irrigated', save)).toEqual({ saved: true, errorKey: null });
    expect(save).toHaveBeenCalledWith('irrigated');
  });
  it('a regime read from the crop is not the parcel\'s: still saves', async () => {
    const save = vi.fn().mockResolvedValue(undefined);
    expect((await saveRegimePick({ regime: 'irrigated', regime_source: 'crop' }, 'rainfed', save)).saved).toBe(true);
  });
  it('does not write over a regime the parcel already has', async () => {
    const save = vi.fn();
    expect(await saveRegimePick(OWN, 'irrigated', save)).toEqual({ saved: false, errorKey: null });
    expect(save).not.toHaveBeenCalled();
  });
  it('a refused save returns a message key and never throws', async () => {
    const save = vi.fn().mockRejectedValue(new PlanningApiError(503, 'store_unavailable'));
    expect(await saveRegimePick(NONE, 'rainfed', save)).toEqual({ saved: false, errorKey: 'generic' });
    expect(await saveRegimePick(NONE, 'rainfed', vi.fn().mockRejectedValue(new Error('net')))).toEqual({ saved: false, errorKey: 'generic' });
  });
});
