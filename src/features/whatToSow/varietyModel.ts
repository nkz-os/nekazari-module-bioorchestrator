import type { Recommendation, VarietyRec } from '../../types/recommend';
import type { VarietyInfo } from '../../components/AssignVarietyModal';

export const TOP_VARIETIES = 5;

export function topVarieties(rec: Recommendation): VarietyRec[] {
  return rec.varieties.slice(0, TOP_VARIETIES);
}

/**
 * Maps a variety to the assign modal's contract. Null when the modal could not show it
 * (it needs a variety URI, a numeric expected yield and a complete interval) — missing data is never invented.
 */
export function toVarietyInfo(v: VarietyRec, crop: Recommendation['crop']): VarietyInfo | null {
  const [lo, hi] = v.interval;
  if (!v.variety_uri || v.expected_kg_ha == null || lo == null || hi == null) return null;
  return {
    name: v.variety,
    scientificName: crop.scientific_name,
    cropUri: `urn:ngsi-ld:AgriCrop:${crop.eppo}`,
    varietyUri: v.variety_uri,
    expectedYield: v.expected_kg_ha,
    confidenceInterval: [lo, hi],
    trialCount: v.n_trials,
  };
}

/** Disease summary is only meaningful when at least one disease was assessed. */
export function showDiseaseSummary(v: VarietyRec): boolean {
  return v.disease_summary.total > 0;
}

/** Scale for the varieties' range bars: the largest interval end or expected value. */
export function varietyScaleMax(vs: VarietyRec[]): number | null {
  let max: number | null = null;
  for (const v of vs) {
    for (const n of [v.interval[1], v.expected_kg_ha]) {
      if (n != null && Number.isFinite(n) && (max == null || n > max)) max = n;
    }
  }
  return max;
}
