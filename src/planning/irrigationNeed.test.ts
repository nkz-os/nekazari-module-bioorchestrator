import { afterEach, describe, expect, it, vi } from 'vitest';
import { allotmentToMm, armOneShotRefetch, convertTyped, irrigatedParcel, irrigationUnavailableKey, parseAllotment } from './irrigationNeed';

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

describe('allotment input: thousands separators and units', () => {
  it.each(['4.500', '4,500', '12.000', '1.234.567'])('%j is ambiguous and refused', (text) => {
    expect(parseAllotment(text, 'm3ha')).toEqual({ ok: false, error: 'thousands' });
    expect(parseAllotment(text, 'mm')).toEqual({ ok: false, error: 'thousands' });
  });
  it('4500 m3/ha is 450 mm; 4,5 mm stays 4.5 mm; 1,25 rounds to 0.01', () => {
    expect(parseAllotment('4500', 'm3ha')).toEqual({ ok: true, mm: 450 });
    expect(parseAllotment('4,5', 'mm')).toEqual({ ok: true, mm: 4.5 });
    expect(parseAllotment('12,345', 'm3ha')).toEqual({ ok: false, error: 'thousands' });
  });
  it('under 1 mm after conversion is refused', () => {
    expect(parseAllotment('0.004', 'm3ha')).toEqual({ ok: false, error: 'invalid' });
    expect(parseAllotment('9', 'm3ha')).toEqual({ ok: false, error: 'tooSmall' });
    expect(parseAllotment('10', 'm3ha')).toEqual({ ok: true, mm: 1 });
    expect(parseAllotment('0.5', 'mm')).toEqual({ ok: false, error: 'tooSmall' });
    expect(parseAllotment('0.001', 'mm')).toEqual({ ok: false, error: 'invalid' });
  });
  it('switching unit converts the typed quantity or clears it', () => {
    expect(convertTyped('4500', 'm3ha', 'mm')).toBe('450');
    expect(convertTyped('450', 'mm', 'm3ha')).toBe('4500');
    expect(convertTyped('4,5', 'mm', 'm3ha')).toBe('45');
    expect(convertTyped('4.500', 'mm', 'm3ha')).toBe('');
    expect(convertTyped('abc', 'mm', 'm3ha')).toBe('');
    expect(convertTyped('', 'mm', 'm3ha')).toBe('');
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
