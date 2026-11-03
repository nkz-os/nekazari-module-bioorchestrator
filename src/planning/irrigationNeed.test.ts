import { afterEach, describe, expect, it, vi } from 'vitest';
import { allotmentToMm, armOneShotRefetch, irrigatedParcel, irrigationUnavailableKey, parseAllotment } from './irrigationNeed';

describe('allotment input', () => {
  it('converts m3/ha to mm by dividing by 10', () => {
    expect(allotmentToMm(4500, 'm3ha')).toBe(450);
    expect(parseAllotment('4500', 'm3ha')).toEqual({ ok: true, mm: 450 });
    expect(parseAllotment('450', 'mm')).toEqual({ ok: true, mm: 450 });
  });
  it('accepts a decimal comma', () => expect(parseAllotment('1234,5', 'm3ha')).toEqual({ ok: true, mm: 123.45 }));
  it.each(['', 'abc', '0', '-5', '1e3', '12.3.4'])('rejects %j', (text) => {
    expect(parseAllotment(text, 'mm')).toEqual({ ok: false, error: 'invalid' });
  });
  it('rejects more than 2000 mm in either unit', () => {
    expect(parseAllotment('2001', 'mm')).toEqual({ ok: false, error: 'tooLarge' });
    expect(parseAllotment('20010', 'm3ha')).toEqual({ ok: false, error: 'tooLarge' });
    expect(parseAllotment('2000', 'mm')).toEqual({ ok: true, mm: 2000 });
  });
});

describe('irrigation helpers', () => {
  it('only an irrigated regime shows the need', () => {
    expect(irrigatedParcel('irrigated')).toBe(true);
    expect(irrigatedParcel('rainfed')).toBe(false);
    expect(irrigatedParcel(null)).toBe(false);
  });
  it('an unknown unavailable reason reads as the generic one', () => {
    expect(irrigationUnavailableKey('no_kc')).toBe('planning.wizard.crop.irrigation.unavailable.no_kc');
    expect(irrigationUnavailableKey('nope')).toBe('planning.wizard.crop.irrigation.unavailable.weather_unavailable');
  });
});

describe('one-shot refetch', () => {
  afterEach(() => vi.useRealTimers());
  it('fires once after 90 s and never again for the same key', () => {
    vi.useFakeTimers();
    const done = { current: null as string | null };
    const fire = vi.fn();
    armOneShotRefetch(done, 'p|2027|irrigated', 90_000, fire);
    vi.advanceTimersByTime(89_999);
    expect(fire).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fire).toHaveBeenCalledTimes(1);
    armOneShotRefetch(done, 'p|2027|irrigated', 90_000, fire);
    vi.advanceTimersByTime(200_000);
    expect(fire).toHaveBeenCalledTimes(1);
  });
  it('cancel (unmount or parcel change) stops it', () => {
    vi.useFakeTimers();
    const fire = vi.fn();
    const cancel = armOneShotRefetch({ current: null }, 'k', 90_000, fire);
    cancel();
    vi.advanceTimersByTime(200_000);
    expect(fire).not.toHaveBeenCalled();
  });
});
