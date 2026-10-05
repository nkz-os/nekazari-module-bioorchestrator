import { describe, expect, it } from 'vitest';
import { toCompareRow } from './yieldProjectionModel';

describe('toCompareRow', () => {
  it('keeps the numbers of a crop with evidence', () => {
    const row = toCompareRow({
      crop: 'TRZAX', agronomics: { expected_yield_kg_ha: 5000, best_variety: 'V1' },
      economic: { net_margin_eur_ha: 1000 }, environmental: { carbon_fixed_tco2e_ha: 1.5 },
      soil_suitability: { warnings: ['pH 4 outside [5, 8]'] },
    });
    expect(row).toEqual({
      crop: 'TRZAX', best_variety: 'V1', expected_yield_kg_ha: 5000, net_margin_eur_ha: 1000,
      carbon_fixed_tco2e_ha: 1.5, soil_warnings: ['pH 4 outside [5, 8]'],
    });
  });
  it('a crop without eligible evidence has null yield and margin, never 0', () => {
    const row = toCompareRow({
      crop: 'LINUS', agronomics: { expected_yield_kg_ha: null }, economic: { net_margin_eur_ha: null },
      environmental: { carbon_fixed_tco2e_ha: 0.2 },
    });
    expect(row.expected_yield_kg_ha).toBeNull();
    expect(row.net_margin_eur_ha).toBeNull();
    expect(toCompareRow({ crop: 'X' }).expected_yield_kg_ha).toBeNull();
  });
  it('a genuine zero is kept (it is a value, unlike a missing one)', () => {
    const row = toCompareRow({ crop: 'X', agronomics: { expected_yield_kg_ha: 0 }, economic: { net_margin_eur_ha: 0 } });
    expect(row.expected_yield_kg_ha).toBe(0);
    expect(row.net_margin_eur_ha).toBe(0);
  });
});
