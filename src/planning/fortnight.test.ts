import { describe, expect, it } from 'vitest';
import { campaignFortnights, formatRange, fortnightOf, withFortnight, middleDay, monthColumns, monthLabel, sameFortnight, weeksBetween } from './fortnight';

const F = (start: string, end: string) => ({ start, end });

describe('fortnights', () => {
  it('a day falls in the 1–15 or the 16–end fortnight, leap years included', () => {
    expect(fortnightOf('2026-10-20')).toEqual(F('2026-10-16', '2026-10-31'));
    expect(fortnightOf('2027-02-03')).toEqual(F('2027-02-01', '2027-02-15'));
    expect(fortnightOf('2028-02-20').end).toBe('2028-02-29');
  });
  it('a campaign has 24 fortnights from Aug 1 of C-1 to Jul 31 of C', () => {
    const fns = campaignFortnights(2027);
    expect(fns).toHaveLength(24);
    expect([fns[0].start, fns[9].start, fns[10].start, fns[23].end]).toEqual(['2026-08-01', '2026-12-16', '2027-01-01', '2027-07-31']);
  });
  it('middle day is the 8th or the 23rd', () => {
    expect(middleDay(F('2026-10-01', '2026-10-15'))).toBe('2026-10-08');
    expect(middleDay(F('2026-10-16', '2026-10-31'))).toBe('2026-10-23');
  });
  it('formats a range in each language with Intl, never by hand', () => {
    expect(formatRange(F('2026-10-16', '2026-10-31'), 'es')).toMatch(/16.*31.*octubre.*2026/);
    expect(formatRange(F('2026-10-16', '2026-10-31'), 'en')).toMatch(/October 16\s*–\s*31, 2026/);
    expect(formatRange(F('2026-10-16', '2026-10-31'), 'eu')).toMatch(/2026/);
  });
  it('groups fortnights into month columns in order', () => {
    const cols = monthColumns([F('2026-10-01', '2026-10-15'), F('2026-10-16', '2026-10-31'), F('2026-11-01', '2026-11-15')]);
    expect(cols.map((c) => [c.year, c.month, c.halves.length])).toEqual([[2026, 10, 2], [2026, 11, 1]]);
  });
  it('month labels come from Intl in the language', () => {
    expect(monthLabel(2026, 10, 'es').toLowerCase()).toContain('oct');
    expect(monthLabel(2026, 10, 'fr').toLowerCase()).toContain('oct');
  });
  it('compares and measures', () => {
    expect(sameFortnight(F('2026-10-16', '2026-10-31'), F('2026-10-16', '2026-10-31'))).toBe(true);
    expect(sameFortnight(null, F('2026-10-16', '2026-10-31'))).toBe(false);
    expect(weeksBetween('2026-08-01', '2026-10-16')).toBe(10);
  });
  it('a fortnight outside a list is inserted in order, never duplicated', () => {
    const list = [F('2026-10-01', '2026-10-15'), F('2026-10-16', '2026-10-31')];
    expect(withFortnight(list, F('2026-09-16', '2026-09-30')).map((f) => f.start)).toEqual(['2026-09-16', '2026-10-01', '2026-10-16']);
    expect(withFortnight(list, list[1])).toBe(list);
    expect(withFortnight(list, null)).toBe(list);
  });
});
