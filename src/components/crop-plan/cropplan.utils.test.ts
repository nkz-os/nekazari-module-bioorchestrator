import { describe, it, expect } from 'vitest';
import { sortByUrgency, pickActiveSegment, buildTimeline } from './cropplan.utils';
import type { CropPlan } from '../../types/cropplan';

describe('sortByUrgency', () => {
  it('orders high before medium before low/unknown, then by dueDate', () => {
    const out = sortByUrgency([
      { id: 'a', operationType: 'x', status: 'issued', urgency: 'low', dueDate: '2026-07-01' },
      { id: 'b', operationType: 'x', status: 'issued', urgency: 'high', dueDate: '2026-07-10' },
      { id: 'c', operationType: 'x', status: 'issued', urgency: 'high', dueDate: '2026-07-02' },
      { id: 'd', operationType: 'x', status: 'issued' },
    ]);
    expect(out.map((o) => o.id)).toEqual(['c', 'b', 'a', 'd']);
  });
});

describe('pickActiveSegment', () => {
  it('returns the segment whose id matches plan.active', () => {
    const plan: CropPlan = {
      parcel_id: 'p',
      season: '2026',
      active: 'seg-1',
      segments: [
        { id: 'seg-0', seq: 0, status: 'harvested' },
        { id: 'seg-1', seq: 1, status: 'active' },
      ],
    };
    expect(pickActiveSegment(plan)?.id).toBe('seg-1');
  });
});

describe('buildTimeline', () => {
  it('places today between span start and end as a 0..100 pct', () => {
    const plan: CropPlan = {
      parcel_id: 'p',
      season: '2026',
      active: null,
      segments: [
        { id: 's0', seq: 0, status: 'planned', sowingWindowStart: '2026-03-01', expectedTerminationDate: '2026-09-01' },
      ],
    };
    const tl = buildTimeline(plan, { stages: [] }, new Date('2026-06-01'));
    expect(tl.todayPct).toBeGreaterThan(0);
    expect(tl.todayPct).toBeLessThan(100);
    expect(tl.segments).toHaveLength(1);
  });
});
