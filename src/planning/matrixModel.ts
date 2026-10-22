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
  return !(row.cells[String(campaign)] ?? []).some((c) => blocksPlan(c, campaign));
}

/** A target-campaign cycle the backend would refuse to replace (see canPlan). */
const blocksPlan = (c: CycleCell, campaign: number): boolean => (c.status !== 'planned' && c.status !== 'cancelled')
  || c.provenance === 'actual' || c.provenance === 'declared' || c.provenance === 'unknown'
  || (c.lifecycle === 'perennial' && (c.first_campaign == null || c.first_campaign < campaign));

/**
 * A blocking cycle that is not known to be sown: its origin or its status is unknown, and it is
 * neither sown, declared nor in a started status. The lock still holds; only the wording differs.
 */
const isUnknownCycle = (c: CycleCell): boolean => (c.provenance === 'unknown' || c.status === null)
  && c.provenance !== 'actual' && c.provenance !== 'declared' && (c.status === null || c.status === 'planned');

export type LockReason = 'cycles_unavailable' | 'perennial_occupies' | 'campaign_started' | 'campaign_unknown';

/**
 * Why a parcel cannot be planned in the campaign, or null when it can: null exactly when
 * canPlan is true. A deep link can open a parcel the matrix would not offer, and the detail
 * says which refusal applies instead of showing a form the backend would reject.
 * `campaign_unknown` replaces `campaign_started` when every blocking cycle has an unknown origin
 * or status: the parcel is locked all the same, but the text must not say a crop was sown.
 */
export function lockReason(row: ParcelRow, campaign: number): LockReason | null {
  if (!row.cycles_available) return 'cycles_unavailable';
  const cells = row.cells[String(campaign)] ?? [];
  const occupied = cells.some((c) => c.lifecycle === 'perennial' && (c.first_campaign == null || c.first_campaign < campaign));
  if (occupied) return 'perennial_occupies';
  const blockers = cells.filter((c) => blocksPlan(c, campaign));
  if (blockers.length === 0) return null;
  return blockers.every(isUnknownCycle) ? 'campaign_unknown' : 'campaign_started';
}

/** The words for a lock reason: the parcel detail and the matrix cell say the same. */
export function lockMessage(locked: LockReason, campaign: number, t: TFn): string {
  if (locked === 'cycles_unavailable') return t('planning.warning.cycles_unavailable');
  if (locked === 'campaign_unknown') return t('planning.lock.campaign_unknown');
  return t(`planning.plan.error.${locked}`, { campaign });
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

/** Something is sown or declared in that campaign: the campaign simulation has a crop to follow. */
export function hasSownCrop(row: ParcelRow, campaign: number): boolean {
  return (row.cells[String(campaign)] ?? []).some((c) => c.provenance === 'actual' || c.provenance === 'declared');
}

/**
 * The campaign simulation link: a crop sown in the target campaign, or in the one before (the target
 * rolls over in autumn, so an autumn-sown crop already sits in the target campaign).
 */
export function simulationAvailable(row: ParcelRow, campaign: number): boolean {
  return hasSownCrop(row, campaign) || hasSownCrop(row, campaign - 1);
}
