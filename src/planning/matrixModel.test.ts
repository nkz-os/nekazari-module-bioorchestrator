import { describe, expect, it } from 'vitest';
import { canPlan, previousMain, rowsForFarm, warningText, widgetColumns } from './matrixModel';
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
  it('blocks any target cycle whose status is not planned or cancelled (backend 409 campaign_started)', () => {
    expect(canPlan(row([cell({ provenance: 'unknown', status: 'active' })]), 2027)).toBe(false);
    expect(canPlan(row([cell({ provenance: 'planned', status: null })]), 2027)).toBe(false);
    expect(canPlan(row([cell({ provenance: 'planned', status: 'harvested' })]), 2027)).toBe(false);
    expect(canPlan(row([cell({ provenance: 'planned', status: 'planned' })]), 2027)).toBe(true);
    expect(canPlan(row([cell({ provenance: 'planned', status: 'cancelled' })]), 2027)).toBe(true);
  });
  it('blocks unknown provenance and a perennial whose first campaign is unknown', () => {
    expect(canPlan(row([cell({ provenance: 'unknown', status: 'planned' })]), 2027)).toBe(false);
    expect(canPlan(row([cell({ lifecycle: 'perennial', first_campaign: null })]), 2027)).toBe(false);
  });
  it('an unreadable history never reads as free', () => {
    expect(canPlan(row([], { cycles_available: false }), 2027)).toBe(false);
    expect(canPlan(row([]), 2027)).toBe(true);
  });
  it('filters by farm and sorts by name in the user language', () => {
    const rows = [row([], { name: 'Vega', farm_id: 'f1' }), row([], { name: 'Ábrego', farm_id: 'f2' })];
    expect(rowsForFarm(rows, 'all', 'es').map((r) => r.name)).toEqual(['Ábrego', 'Vega']);
    expect(rowsForFarm(rows, 'f1', 'es').map((r) => r.name)).toEqual(['Vega']);
  });
  it('sorting parcels does not throw on a malformed language tag', () => {
    const rows = [row([], { name: 'Vega' }), row([], { name: 'Ábrego' })];
    expect(rowsForFarm(rows, 'all', '').map((r) => r.name)).toEqual(['Ábrego', 'Vega']);
  });
  it('rotation warnings name crops in the user language', () => {
    const t = (k: string, o?: Record<string, unknown>) => `${k}|${o?.species}|${o?.previous}`;
    const w = { code: 'rotation_restriction', campaign: 2027, params: { species: 'rapeseed', previous: 'rapeseed', allowed_from: 2030 } };
    expect(warningText(w, { rapeseed: { es: 'colza' } }, 'es', t)).toBe('planning.warning.rotation_restriction|colza|colza');
  });
  it('the previous main crop is the latest by start, a null start sorting first', () => {
    const early = cell({ role: 'main_crop', start: '2026-10-01', variety: 'early' });
    const late = cell({ role: 'main_crop', start: '2026-04-01', variety: 'late' });
    const unknown = cell({ role: 'main_crop', start: null, variety: 'unknown' });
    const cover = cell({ role: 'cover_crop', start: '2026-12-01', variety: 'cover' });
    expect(previousMain(row([], { cells: { '2026': [early, late] } }), 2027)?.variety).toBe('early');
    expect(previousMain(row([], { cells: { '2026': [early, unknown] } }), 2027)?.variety).toBe('early');
    expect(previousMain(row([], { cells: { '2026': [unknown] } }), 2027)?.variety).toBe('unknown');
    expect(previousMain(row([], { cells: { '2026': [cover, late] } }), 2027)?.variety).toBe('late');
    expect(previousMain(row([], { cells: { '2026': [cover] } }), 2027)).toBeNull();
  });
  it('widget shows the current and the planned campaign', () => {
    expect(widgetColumns(row([cell({})]), 2027).map((c) => c.campaign)).toEqual([2026, 2027]);
  });
});
