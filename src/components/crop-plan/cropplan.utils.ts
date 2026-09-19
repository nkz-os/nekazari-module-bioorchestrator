import type { CropPlan, PlanSegment, IssuedOp, PhenologyStatus } from '../../types/cropplan';

const URG: Record<string, number> = { high: 0, medium: 1, low: 2 };
const rank = (u?: string) => (u && u in URG ? URG[u] : 3);

// NGSI-LD Date/DateTime literals arrive as {"@type":"Date","@value":"..."}
// (keyValues keeps the inner literal). Unwrap to a plain string before parsing
// — otherwise Date.parse(object) → NaN → RangeError: invalid date (and React
// error #31 when the object is rendered as a child).
type MaybeDate = string | { '@value'?: string; value?: string } | undefined;
export function dt(v: MaybeDate): string | undefined {
  if (typeof v === 'string') return v;
  if (v && typeof v === 'object') return v['@value'] || v.value;
  return undefined;
}

/** Recommended actions ordered by urgency (high→low→unknown), tiebreak earliest dueDate. */
export function sortByUrgency(ops: IssuedOp[]): IssuedOp[] {
  return [...ops].sort((a, b) => {
    const r = rank(a.urgency) - rank(b.urgency);
    if (r !== 0) return r;
    const daRaw = dt(a.dueDate as MaybeDate);
    const dbRaw = dt(b.dueDate as MaybeDate);
    const da = daRaw ? Date.parse(daRaw) : Infinity;
    const db = dbRaw ? Date.parse(dbRaw) : Infinity;
    return da - db;
  });
}

export function pickActiveSegment(plan: CropPlan): PlanSegment | undefined {
  if (!plan?.segments) return undefined;
  return plan.segments.find((s) => s.id === plan.active) ?? plan.segments.find((s) => s.status === 'active');
}

export interface TimelineBand {
  id: string;
  label: string;
  status: string;
  startPct: number;
  endPct: number;
}
export interface TimelineMark {
  label: string;
  pct: number;
  current?: boolean;
}
export interface TimelineModel {
  start: string;
  end: string;
  segments: TimelineBand[];
  stages: TimelineMark[];
  todayPct: number;
}

const segStart = (s: PlanSegment) => dt(s.plantingDate as MaybeDate) || dt(s.sowingWindowStart as MaybeDate);
const segEnd = (s: PlanSegment) => dt(s.terminationDate as MaybeDate) || dt(s.expectedTerminationDate as MaybeDate);

/** Normalise plan segments + projected phenology stages onto a 0..100 axis. */
export function buildTimeline(plan: CropPlan, status: PhenologyStatus, today: Date): TimelineModel {
  const dates: number[] = [];
  for (const s of plan.segments ?? []) {
    for (const d of [segStart(s), segEnd(s)]) if (d) dates.push(Date.parse(d));
  }
  for (const st of status.stages ?? []) {
    for (const d of [dt(st.startDate as MaybeDate), dt(st.endDate as MaybeDate)]) if (d) dates.push(Date.parse(d));
  }
  dates.push(today.getTime());
  const min = Math.min(...dates);
  const max = Math.max(...dates);
  const span = Math.max(max - min, 1);
  const pct = (t: number) => ((t - min) / span) * 100;

  const segments: TimelineBand[] = (plan.segments ?? []).map((s) => {
    const a = segStart(s) ? Date.parse(segStart(s)!) : min;
    const b = segEnd(s) ? Date.parse(segEnd(s)!) : max;
    return { id: s.id, label: s.species || `#${s.seq}`, status: s.status, startPct: pct(a), endPct: pct(b) };
  });
  const stages: TimelineMark[] = (status.stages ?? [])
    .filter((st) => dt(st.startDate as MaybeDate))
    .map((st) => ({ label: st.stage, pct: pct(Date.parse(dt(st.startDate as MaybeDate)!)), current: st.current }));

  return {
    start: new Date(min).toISOString().slice(0, 10),
    end: new Date(max).toISOString().slice(0, 10),
    segments,
    stages,
    todayPct: pct(today.getTime()),
  };
}
