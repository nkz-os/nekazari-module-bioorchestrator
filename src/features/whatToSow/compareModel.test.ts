import { describe, expect, it } from 'vitest';
import {
  barPct, doyPct, fmtCell, formatDoy, loadPrice, monthSegments, parsePrice, priceKey, rotationSummary, savePrice,
  typicalCalendar, typicalSowingMonth,
} from './compareModel';

describe('monthSegments', () => {
  it('returns one segment for a window inside the year', () => {
    const [seg, ...rest] = monthSegments({ start_month: 3, end_month: 5 });
    expect(rest).toEqual([]);
    expect(seg.leftPct).toBeCloseTo(200 / 12);
    expect(seg.widthPct).toBeCloseTo(300 / 12);
  });
  it('splits a window that wraps the year end (Oct to Feb)', () => {
    const segs = monthSegments({ start_month: 10, end_month: 2 });
    expect(segs).toHaveLength(2);
    expect(segs[0].leftPct).toBeCloseTo(900 / 12);
    expect(segs[0].widthPct).toBeCloseTo(300 / 12);
    expect(segs[1].leftPct).toBe(0);
    expect(segs[1].widthPct).toBeCloseTo(200 / 12);
  });
  it('covers a single month', () => {
    expect(monthSegments({ start_month: 12, end_month: 12 })).toHaveLength(1);
  });
  it('is empty without a valid window', () => {
    expect(monthSegments(null)).toEqual([]);
    expect(monthSegments({ start_month: 0, end_month: 5 })).toEqual([]);
    expect(monthSegments({ start_month: 3, end_month: 13 })).toEqual([]);
    expect(monthSegments({ start_month: 1.5, end_month: 3 })).toEqual([]);
  });
});

describe('parsePrice', () => {
  it('accepts positive numbers with dot or comma', () => {
    expect(parsePrice('230')).toBe(230);
    expect(parsePrice(' 230,5 ')).toBe(230.5);
  });
  it('rejects empty, zero, negative and junk (never 0)', () => {
    for (const raw of ['', '  ', '0', '-3', 'abc', 'Infinity', '1e999']) expect(parsePrice(raw)).toBeNull();
  });
});

describe('price storage', () => {
  const mem = () => {
    const m = new Map<string, string>();
    return { getItem: (k: string) => m.get(k) ?? null, setItem: (k: string, v: string) => void m.set(k, v),
             removeItem: (k: string) => void m.delete(k) };
  };
  it('round-trips under the documented key', () => {
    const s = mem();
    savePrice('TRZAX', '210', s);
    expect(priceKey('TRZAX')).toBe('bioorchestrator.prices.TRZAX');
    expect(loadPrice('TRZAX', s)).toBe('210');
  });
  it('clears the entry on empty text', () => {
    const s = mem();
    savePrice('TRZAX', '210', s);
    savePrice('TRZAX', '', s);
    expect(loadPrice('TRZAX', s)).toBe('');
  });
  it('survives a throwing or missing storage', () => {
    const bad = { getItem: () => { throw new Error('x'); }, setItem: () => { throw new Error('x'); },
                  removeItem: () => { throw new Error('x'); } };
    expect(loadPrice('A', bad)).toBe('');
    expect(() => savePrice('A', '5', bad)).not.toThrow();
    expect(loadPrice('A', null)).toBe('');
  });
});

describe('rotationSummary', () => {
  it('takes the year-2 crop, its warning, its N fixation and balance, and the PAC score', () => {
    const out = rotationSummary({
      plan: [{ year: 1, crop: 'TRZAX' }, { year: 2, crop: 'PIBSX', rotation_warning: 'same family',
        expected_yield_kg_ha: 5000, net_margin_eur_ha: 9, n_fixation_kg_ha: 120, n_balance_kg_ha: 35.5 }],
      pac_compliance: { score: 80 },
    });
    expect(out).toEqual({ nextCrop: 'PIBSX', warning: 'same family', nFixation: 120, nBalance: 35.5, pacScore: 80 });
  });
  it('keeps a zero N fixation and a negative balance (real values, not missing)', () => {
    const out = rotationSummary({ plan: [{ year: 1, crop: 'A' }, { year: 2, crop: 'B', n_fixation_kg_ha: 0, n_balance_kg_ha: -40 }] });
    expect(out).toMatchObject({ nFixation: 0, nBalance: -40 });
  });
  it('maps missing pieces to null, never 0', () => {
    expect(rotationSummary({ plan: [{ year: 1, crop: 'A' }] }))
      .toEqual({ nextCrop: null, warning: null, nFixation: null, nBalance: null, pacScore: null });
    expect(rotationSummary({ plan: [{ year: 1, crop: 'A' }, { year: 2, crop: 'B' }], pac_compliance: {} }))
      .toEqual({ nextCrop: 'B', warning: null, nFixation: null, nBalance: null, pacScore: null });
  });
  it('treats a repeated starting crop as no successor (backend repeats it when none exists)', () => {
    expect(rotationSummary({
      plan: [{ year: 1, crop: 'A' }, { year: 2, crop: 'A', n_fixation_kg_ha: 0, n_balance_kg_ha: 10, rotation_warning: 'x' }],
      pac_compliance: { score: 50 },
    })).toEqual({ nextCrop: null, warning: null, nFixation: null, nBalance: null, pacScore: 50 });
  });
  it('is null for an error or empty payload', () => {
    expect(rotationSummary(null)).toBeNull();
    expect(rotationSummary({ error: 'x' })).toBeNull();
    expect(rotationSummary('nope')).toBeNull();
  });
});

describe('barPct', () => {
  it('scales to the row max and skips missing cells', () => {
    expect(barPct(['100', null, '50'])).toEqual([100, null, 50]);
  });
  it('is null everywhere when there is no positive max', () => {
    expect(barPct([null, '0'])).toEqual([null, null]);
  });
});

describe('fmtCell', () => {
  it('localises numbers and keeps an explicit plus sign', () => {
    expect(fmtCell('+5.3', 'en')).toBe('+5.3');
    expect(fmtCell('+5.3', 'es')).toBe('+5,3');
    expect(fmtCell('-2', 'en')).toBe('-2');
    expect(fmtCell('1234', 'en')).toBe('1,234');
  });
});

describe('doyPct', () => {
  it('maps day 1 to 0% and 31 Dec to the last day of a 365-day year', () => {
    expect(doyPct(1)).toBe(0);
    expect(doyPct(365)).toBeCloseTo((364 / 365) * 100);
    expect(doyPct(183)).toBeCloseTo((182 / 365) * 100);
  });
  it('is null for invalid days', () => {
    for (const d of [0, 366, 367, 1.5, NaN, null, undefined]) expect(doyPct(d as number)).toBeNull();
  });
});

describe('typicalCalendar', () => {
  const season = (sow: number | null, mat: number | null, window = false) => ({
    sowing_window: window ? { start_month: 10, end_month: 11 } : null,
    cycle_days: null, source: 'GGCMI', typical_sowing_doy: sow, typical_maturity_doy: mat,
  });
  it('draws a marker at the sowing day and one bar to maturity inside the year', () => {
    const cal = typicalCalendar(season(100, 250))!;
    expect(cal.markerPct).toBeCloseTo((99 / 365) * 100);
    expect(cal.segments).toHaveLength(1);
    expect(cal.segments[0].leftPct).toBeCloseTo((99 / 365) * 100);
    expect(cal.segments[0].widthPct).toBeCloseTo((151 / 365) * 100);
  });
  it('wraps the year end into two bars (winter wheat: Nov to Jul)', () => {
    const cal = typicalCalendar(season(304, 190))!;
    expect(cal.segments).toHaveLength(2);
    expect(cal.segments[0].leftPct).toBeCloseTo((303 / 365) * 100);
    expect(cal.segments[0].leftPct + cal.segments[0].widthPct).toBeCloseTo(100);
    expect(cal.segments[1].leftPct).toBe(0);
    expect(cal.segments[1].widthPct).toBeCloseTo((190 / 365) * 100);
  });
  it('keeps the marker without a maturity day', () => {
    const cal = typicalCalendar(season(120, null))!;
    expect(cal.segments).toEqual([]);
    expect(cal.maturityDoy).toBeNull();
  });
  it('is null when a sowing window exists or there is no typical day', () => {
    expect(typicalCalendar(season(100, 250, true))).toBeNull();
    expect(typicalCalendar(season(null, 250))).toBeNull();
    const tableOnly = { sowing_window: null, cycle_days: null, source: null };
    expect(typicalCalendar(tableOnly)).toBeNull();
  });
});

describe('formatDoy', () => {
  it('formats a day of year as month-day in the locale (non-leap calendar)', () => {
    expect(formatDoy(304, 'en')).toBe('Oct 31');
    expect(formatDoy(1, 'en')).toBe('Jan 1');
    expect(formatDoy(365, 'en')).toBe('Dec 31');
    expect(formatDoy(60, 'en')).toBe('Mar 1');
    expect(formatDoy(304, 'es')).toMatch(/31 oct/);
  });
});

describe('typicalSowingMonth', () => {
  it('names the month of the typical sowing day only when there is no window', () => {
    const base = { sowing_window: null, cycle_days: null, source: 'GGCMI', typical_maturity_doy: null };
    expect(typicalSowingMonth({ ...base, typical_sowing_doy: 304 }, 'en')).toBe('October');
    expect(typicalSowingMonth({ ...base, typical_sowing_doy: null }, 'en')).toBeNull();
    expect(typicalSowingMonth({ ...base, sowing_window: { start_month: 1, end_month: 2 },
      typical_sowing_doy: 304 }, 'en')).toBeNull();
  });
});
