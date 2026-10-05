/** Pure helpers of the crop comparison table of the yield projection. */

export interface CompareRow {
  crop: string;
  best_variety: string;
  /** Null when the crop has no eligible numeric yield evidence: shown as "no data", never 0. */
  expected_yield_kg_ha: number | null;
  net_margin_eur_ha: number | null;
  carbon_fixed_tco2e_ha: number;
  soil_warnings: string[];
}

const finiteOrNull = (v: unknown): number | null =>
  typeof v === 'number' && Number.isFinite(v) ? v : null;

/** One `compare-crops` comparison as a table row; a missing yield or margin stays null. */
export function toCompareRow(c: any): CompareRow {
  return {
    crop: c.crop,
    best_variety: c.agronomics?.best_variety || c.best_variety || '—',
    expected_yield_kg_ha: finiteOrNull(c.agronomics?.expected_yield_kg_ha),
    net_margin_eur_ha: finiteOrNull(c.economic?.net_margin_eur_ha),
    carbon_fixed_tco2e_ha: c.environmental?.carbon_fixed_tco2e_ha || 0,
    soil_warnings: c.soil_suitability?.warnings || [],
  };
}
