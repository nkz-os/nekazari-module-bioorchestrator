import type { CropPlan, PlanSegment, IssuedOp, PhenologyStatus } from '../../types/cropplan';

const URG: Record<string, number> = { high: 0, medium: 1, low: 2 };
const rank = (u?: string) => (u && u in URG ? URG[u] : 3);

/** Recommended actions ordered by urgency (high→low→unknown), tiebreak earliest dueDate. */
export function sortByUrgency(ops: IssuedOp[]): IssuedOp[] {
  return [...ops].sort((a, b) => {
    const r = rank(a.urgency) - rank(b.urgency);
    if (r !== 0) return r;
    const da = a.dueDate ? Date.parse(a.dueDate) : Infinity;
    const db = b.dueDate ? Date.parse(b.dueDate) : Infinity;
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

const segStart = (s: PlanSegment) => s.plantingDate || s.sowingWindowStart;
const segEnd = (s: PlanSegment) => s.terminationDate || s.expectedTerminationDate;

/** Normalise plan segments + projected phenology stages onto a 0..100 axis. */
export function buildTimeline(plan: CropPlan, status: PhenologyStatus, today: Date): TimelineModel {
  const dates: number[] = [];
  for (const s of plan.segments ?? []) {
    for (const d of [segStart(s), segEnd(s)]) if (d) dates.push(Date.parse(d));
  }
  for (const st of status.stages ?? []) {
    for (const d of [st.startDate, st.endDate]) if (d) dates.push(Date.parse(d));
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
    .filter((st) => st.startDate)
    .map((st) => ({ label: st.stage, pct: pct(Date.parse(st.startDate!)), current: st.current }));

  return {
    start: new Date(min).toISOString().slice(0, 10),
    end: new Date(max).toISOString().slice(0, 10),
    segments,
    stages,
    todayPct: pct(today.getTime()),
  };
}
