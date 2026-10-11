import { cropName } from './format';
import type { CycleCell, FarmResponse, Names, ParamValue, ParcelRow, PlanWarning, Provenance, TFn } from './types';

export const SYMBOL: Record<Provenance, string> = { actual: '●', declared: '◐', planned: '○', unknown: '·' };

export function rowsForFarm(parcels: ParcelRow[], farmId: string, lang: string): ParcelRow[] {
  const rows = farmId === 'all' ? parcels : parcels.filter((p) => p.farm_id === farmId);
  return [...rows].sort((a, b) => (a.name ?? a.parcel_id).localeCompare(b.name ?? b.parcel_id, lang));
}

/**
 * A target-campaign cycle that was sown or declared is changed from field operations, not here;
 * a perennial that started in an earlier campaign is changed from its own first campaign.
 */
export function canPlan(row: ParcelRow, campaign: number): boolean {
  return !(row.cells[String(campaign)] ?? []).some((c) => c.provenance === 'actual' || c.provenance === 'declared'
    || (c.lifecycle === 'perennial' && c.first_campaign != null && c.first_campaign < campaign));
}

export function namesIndex(data: FarmResponse): Record<string, Names> {
  const index: Record<string, Names> = {};
  for (const row of data.parcels) {
    for (const cells of Object.values(row.cells)) {
      for (const c of cells) if (c.species) index[c.species] = c.names;
    }
  }
  for (const a of data.summary.area_by_crop) if (a.species) index[a.species] = a.names;
  return index;
}

export function warningText(w: PlanWarning, names: Record<string, Names>, lang: string, t: TFn): string {
  const params: Record<string, ParamValue> = { ...w.params };
  for (const key of ['species', 'previous']) {
    const slug = params[key];
    if (typeof slug === 'string') params[key] = cropName(names[slug], slug, lang);
  }
  return t(`planning.warning.${w.code}`, params);
}

export function previousMain(row: ParcelRow, campaign: number): CycleCell | null {
  const cells = (row.cells[String(campaign - 1)] ?? []).filter((c) => c.role === 'main_crop');
  return cells.length ? cells[cells.length - 1] : null;
}

export function widgetColumns(row: ParcelRow, campaign: number): { campaign: number; cells: CycleCell[] }[] {
  return [campaign - 1, campaign].map((c) => ({ campaign: c, cells: row.cells[String(c)] ?? [] }));
}
