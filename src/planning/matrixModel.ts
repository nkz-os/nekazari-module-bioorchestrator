import { cropName, safeLang } from './format';
import type { CycleCell, FarmResponse, Names, ParamValue, ParcelRow, PlanWarning, Provenance, TFn } from './types';

export const SYMBOL: Record<Provenance, string> = { actual: '●', declared: '◐', planned: '○', unknown: '·' };

export function rowsForFarm(parcels: ParcelRow[], farmId: string, lang: string): ParcelRow[] {
  const rows = farmId === 'all' ? parcels : parcels.filter((p) => p.farm_id === farmId);
  const tag = safeLang(lang);
  return [...rows].sort((a, b) => (a.name ?? a.parcel_id).localeCompare(b.name ?? b.parcel_id, tag));
}

/**
 * Mirrors the backend refusal of a plan (plan_write: a target-campaign cycle whose status is not
 * planned or cancelled answers 409 campaign_started). Blocks when the history is unreadable, when
 * a target cycle has a status outside {planned, cancelled} (null included), when its provenance
 * is actual, declared or unknown, or when it is a perennial whose first campaign is unknown or
 * earlier than the target. Absent or unreadable data never reads as plannable.
 */
export function canPlan(row: ParcelRow, campaign: number): boolean {
  if (!row.cycles_available) return false;
  return !(row.cells[String(campaign)] ?? []).some((c) => (c.status !== 'planned' && c.status !== 'cancelled')
    || c.provenance === 'actual' || c.provenance === 'declared' || c.provenance === 'unknown'
    || (c.lifecycle === 'perennial' && (c.first_campaign == null || c.first_campaign < campaign)));
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

/** The main crop of the campaign before: the latest by start (a null start sorts first), not the last listed. */
export function previousMain(row: ParcelRow, campaign: number): CycleCell | null {
  let latest: CycleCell | null = null;
  for (const c of row.cells[String(campaign - 1)] ?? []) {
    if (c.role !== 'main_crop') continue;
    if (latest === null || (c.start ?? '') > (latest.start ?? '')) latest = c;
  }
  return latest;
}

export function widgetColumns(row: ParcelRow, campaign: number): { campaign: number; cells: CycleCell[] }[] {
  return [campaign - 1, campaign].map((c) => ({ campaign: c, cells: row.cells[String(c)] ?? [] }));
}
