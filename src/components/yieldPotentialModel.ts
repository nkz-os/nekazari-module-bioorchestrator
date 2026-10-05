import type { YieldPotentialResponse } from '../services/api';

/**
 * What the parcel's yield card says about the assigned variety: the yield gap, "no data" (the
 * variety has no measured field yield: the API answers null, never 0), or nothing (no answer, or a
 * number without a current estimate to compare it with).
 */
export type YieldPotentialView = 'gap' | 'no_data' | 'none';

export function yieldPotentialView(yp: YieldPotentialResponse | null | undefined): YieldPotentialView {
  if (!yp) return 'none';
  if (yp.expected_yield_kg_ha == null) return 'no_data';
  return yp.yield_gap_pct != null ? 'gap' : 'none';
}
