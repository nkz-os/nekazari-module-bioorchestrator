import { describe, expect, it } from 'vitest';
import type { Recommendation, VarietyRec } from '../../types/recommend';
import { TOP_VARIETIES, showDiseaseSummary, toVarietyInfo, topVarieties, varietyScaleMax } from './varietyModel';

const crop = { eppo: 'TRZAX', scientific_name: 'Triticum aestivum', sowing_type: 'autumn' as const };
const variety = (over: Partial<VarietyRec> = {}): VarietyRec => ({
  variety: 'Alpha', variety_uri: 'urn:ngsi-ld:AgriCrop:TRZAX:Alpha', expected_kg_ha: 5000,
  interval: [4000, 6000], n_trials: 12, disease_summary: { resistant: 2, total: 5 }, ...over,
});

describe('toVarietyInfo', () => {
  it('maps to the modal contract', () => {
    expect(toVarietyInfo(variety(), crop)).toEqual({
      name: 'Alpha', scientificName: 'Triticum aestivum', cropUri: 'urn:ngsi-ld:AgriCrop:TRZAX',
      varietyUri: 'urn:ngsi-ld:AgriCrop:TRZAX:Alpha', expectedYield: 5000,
      confidenceInterval: [4000, 6000], trialCount: 12,
    });
  });
  it('is null when the modal could not show or assign it', () => {
    expect(toVarietyInfo(variety({ expected_kg_ha: null }), crop)).toBeNull();
    expect(toVarietyInfo(variety({ interval: [null, 6000] }), crop)).toBeNull();
    expect(toVarietyInfo(variety({ interval: [4000, null] }), crop)).toBeNull();
    expect(toVarietyInfo(variety({ variety_uri: null }), crop)).toBeNull();
  });
});

describe('showDiseaseSummary', () => {
  it('hides when no disease was assessed', () => {
    expect(showDiseaseSummary(variety({ disease_summary: { resistant: 0, total: 0 } }))).toBe(false);
    expect(showDiseaseSummary(variety())).toBe(true);
  });
});

describe('topVarieties', () => {
  it('keeps the first five', () => {
    const rec = { varieties: Array.from({ length: 8 }, (_, i) => variety({ variety: `v${i}` })) } as Recommendation;
    expect(topVarieties(rec).map((v) => v.variety)).toEqual(['v0', 'v1', 'v2', 'v3', 'v4']);
    expect(TOP_VARIETIES).toBe(5);
  });
});

describe('varietyScaleMax', () => {
  it('takes the largest known value, null when none', () => {
    expect(varietyScaleMax([variety(), variety({ interval: [1, 7000] })])).toBe(7000);
    expect(varietyScaleMax([variety({ expected_kg_ha: null, interval: [null, null] })])).toBeNull();
    expect(varietyScaleMax([])).toBeNull();
  });
});
