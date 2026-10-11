import { describe, expect, it } from 'vitest';
import { canPlan, rowsForFarm, warningText, widgetColumns } from './matrixModel';
import type { CycleCell, ParcelRow } from './types';

const cell = (over: Partial<CycleCell>): CycleCell => ({
  crop_id: 'c', species: 'wheat', species_raw: 'TRZAX', names: { es: 'trigo' }, variety: null, role: 'main_crop',
  provenance: 'planned', status: 'planned', start: null, end: null, lifecycle: 'annual', first_campaign: 2027, ...over,
});
const row = (cells: CycleCell[], over: Partial<ParcelRow> = {}): ParcelRow => ({
  parcel_id: 'urn:ngsi-ld:AgriParcel:t:p1', name: 'Soto', area_ha: 1, farm_id: null, regime: 'secano',
  regime_source: 'declared', cycles_available: true, cells: { '2026': [], '2027': cells }, warnings: [], ...over,
});

describe('matrix model', () => {
  it('a sown or declared target cycle cannot be replanned (backend answers 409)', () => {
    expect(canPlan(row([]), 2027)).toBe(true);
    expect(canPlan(row([cell({})]), 2027)).toBe(true);
    expect(canPlan(row([cell({ provenance: 'actual', status: 'active' })]), 2027)).toBe(false);
    expect(canPlan(row([cell({ provenance: 'declared' })]), 2027)).toBe(false);
    // Alfalfa planned in 2025 still occupies 2027: changed from its own campaign only.
    expect(canPlan(row([cell({ lifecycle: 'perennial', first_campaign: 2025 })]), 2027)).toBe(false);
    expect(canPlan(row([cell({ lifecycle: 'perennial', first_campaign: 2027 })]), 2027)).toBe(true);
  });
  it('filters by farm and sorts by name in the user language', () => {
    const rows = [row([], { name: 'Vega', farm_id: 'f1' }), row([], { name: 'Ábrego', farm_id: 'f2' })];
    expect(rowsForFarm(rows, 'all', 'es').map((r) => r.name)).toEqual(['Ábrego', 'Vega']);
    expect(rowsForFarm(rows, 'f1', 'es').map((r) => r.name)).toEqual(['Vega']);
  });
  it('rotation warnings name crops in the user language', () => {
    const t = (k: string, o?: Record<string, unknown>) => `${k}|${o?.species}|${o?.previous}`;
    const w = { code: 'rotation_restriction', campaign: 2027, params: { species: 'rapeseed', previous: 'rapeseed', allowed_from: 2030 } };
    expect(warningText(w, { rapeseed: { es: 'colza' } }, 'es', t)).toBe('planning.warning.rotation_restriction|colza|colza');
  });
  it('widget shows the current and the planned campaign', () => {
    expect(widgetColumns(row([cell({})]), 2027).map((c) => c.campaign)).toEqual([2026, 2027]);
  });
});
