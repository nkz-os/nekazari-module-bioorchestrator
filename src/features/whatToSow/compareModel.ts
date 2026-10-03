/** Pure helpers of the compare view (no React, no I/O beyond an injected Storage). */

export interface SowingWindow {
  start_month: number;
  end_month: number;
}

export interface MonthSegment {
  leftPct: number;
  widthPct: number;
}

const isMonth = (n: unknown): n is number => Number.isInteger(n) && (n as number) >= 1 && (n as number) <= 12;

/** Bar segments (in % of the 12-month grid) of a window; a window like Oct→Feb wraps into two. */
export function monthSegments(win: SowingWindow | null | undefined): MonthSegment[] {
  if (!win || !isMonth(win.start_month) || !isMonth(win.end_month)) return [];
  const seg = (from: number, to: number): MonthSegment => ({
    leftPct: ((from - 1) / 12) * 100,
    widthPct: ((to - from + 1) / 12) * 100,
  });
  if (win.start_month <= win.end_month) return [seg(win.start_month, win.end_month)];
  return [seg(win.start_month, 12), seg(1, win.end_month)];
}

/** €/t from free text (dot or comma); null unless a finite number above zero. */
export function parsePrice(raw: string): number | null {
  const text = raw.trim().replace(',', '.');
  if (text === '') return null;
  const n = Number(text);
  return Number.isFinite(n) && n > 0 ? n : null;
}

type StorageLike = Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;

export const priceKey = (eppo: string) => `bioorchestrator.prices.${eppo}`;

function defaultStorage(): StorageLike | null {
  try {
    return typeof window !== 'undefined' ? window.localStorage : null;
  } catch {
    return null;
  }
}

/** Saved price text for a crop; '' when absent or storage is unavailable. */
export function loadPrice(eppo: string, storage: StorageLike | null = defaultStorage()): string {
  try {
    return storage?.getItem(priceKey(eppo)) ?? '';
  } catch {
    return '';
  }
}

export function savePrice(eppo: string, text: string, storage: StorageLike | null = defaultStorage()): void {
  try {
    if (text.trim() === '') storage?.removeItem(priceKey(eppo));
    else storage?.setItem(priceKey(eppo), text);
  } catch {
    // Storage is a convenience only.
  }
}

export interface RotationSummary {
  nextCrop: string | null;
  warning: string | null;
  /** kg N/ha fixed by the next crop (crop reference, not parcel-specific). */
  nFixation: number | null;
  /** kg N/ha balance after the next crop. */
  nBalance: number | null;
  pacScore: number | null;
}

const asString = (v: unknown): string | null => (typeof v === 'string' && v !== '' ? v : null);
const asNumber = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);

/**
 * Next crop (year 2 of the plan), its rotation warning, N fixation and N balance, and the PAC score.
 * Yields and margins are deliberately dropped: they are not parcel-specific.
 * When no successor exists the backend repeats the starting crop; that is reported as no next crop.
 */
export function rotationSummary(data: unknown): RotationSummary | null {
  if (!data || typeof data !== 'object') return null;
  const d = data as { error?: unknown; plan?: unknown; pac_compliance?: { score?: unknown } | null };
  if (d.error || !Array.isArray(d.plan)) return null;
  type Step = { crop?: unknown; rotation_warning?: unknown; n_fixation_kg_ha?: unknown; n_balance_kg_ha?: unknown };
  const first = d.plan[0] as Step | undefined;
  const candidate = d.plan[1] as Step | undefined;
  const crop = asString(candidate?.crop);
  const next = crop != null && crop !== asString(first?.crop) ? candidate : undefined;
  return {
    nextCrop: next ? crop : null,
    warning: asString(next?.rotation_warning),
    nFixation: asNumber(next?.n_fixation_kg_ha),
    nBalance: asNumber(next?.n_balance_kg_ha),
    pacScore: asNumber(d.pac_compliance?.score),
  };
}

/** Localised numeric cell; an explicit leading "+" (signed rows) is kept. */
export function fmtCell(v: string, locale: string): string {
  const out = Number(v).toLocaleString(locale, { maximumFractionDigits: 2 });
  return v.trim().startsWith('+') ? `+${out}` : out;
}

/** Each numeric cell as % of the row maximum; null for missing cells or when no positive max exists. */
export function barPct(cells: (string | null)[]): (number | null)[] {
  const nums = cells.map((c) => (c == null || c.trim() === '' ? null : Number(c)))
    .map((n) => (n != null && Number.isFinite(n) ? n : null));
  const max = Math.max(0, ...nums.filter((n): n is number => n != null));
  return nums.map((n) => (n == null || max <= 0 ? null : (n / max) * 100));
}
