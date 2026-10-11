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

export type LockReason = 'cycles_unavailable' | 'perennial_occupies' | 'campaign_started';

/**
 * Why a parcel cannot be planned in the campaign, or null when it can: null exactly when
 * canPlan is true. A deep link can open a parcel the matrix would not offer, and the detail
 * says which of the three refusals applies instead of showing a form the backend would reject.
 */
export function lockReason(row: ParcelRow, campaign: number): LockReason | null {
  if (!row.cycles_available) return 'cycles_unavailable';
  const occupied = (row.cells[String(campaign)] ?? []).some((c) => c.lifecycle === 'perennial'
    && (c.first_campaign == null || c.first_campaign < campaign));
  if (occupied) return 'perennial_occupies';
  return canPlan(row, campaign) ? null : 'campaign_started';
}

/**
 * The campaign of a deep link, or undefined when it is not four digits inside the range the
 * backend accepts (2000-2100): a bad value would make every farm request answer 422 with no way
 * out of the screen.
 */
export function parseCampaign(raw: string | null): number | undefined {
  if (raw == null || !/^\d{4}$/.test(raw)) return undefined;
  const n = Number(raw);
  return n >= 2000 && n <= 2100 ? n : undefined;
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

/**
 * The widget's campaign rows. With an unreadable history there are none: an empty cell would read
 * "undecided" for a history that could not be read.
 */
export function widgetRows(row: ParcelRow, campaign: number): { campaign: number; cells: CycleCell[] }[] {
  return row.cycles_available ? widgetColumns(row, campaign) : [];
}
