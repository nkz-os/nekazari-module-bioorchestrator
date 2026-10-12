// src/planning/keys.test.ts
/// <reference types="vite/client" />
import { describe, expect, it } from 'vitest';
import es from '../locales/es.json';
import en from '../locales/en.json';
import ca from '../locales/ca.json';
import eu from '../locales/eu.json';
import fr from '../locales/fr.json';
import pt from '../locales/pt.json';
import { REASON_KEYS } from './fitModel';
import { COVER_METHODS } from './planModel';

const SOURCES = import.meta.glob('./**/*.{ts,tsx}', { query: '?raw', import: 'default', eager: true }) as Record<string, string>;
const FILES = Object.keys(SOURCES).filter((f) => !/\.test\.tsx?$/.test(f));

const lookup = (dict: unknown, key: string): unknown =>
  key.split('.').reduce<unknown>((n, p) => (n && typeof n === 'object' ? (n as Record<string, unknown>)[p] : undefined), dict);
const has = (dict: unknown, key: string) => ['', '_one', '_other'].some((s) => typeof lookup(dict, key + s) === 'string');

function literalKeys(): string[] {
  const re = /\bt\(\s*(['"])(planning\.[A-Za-z0-9_.]+)\1/g;
  return [...new Set(FILES.flatMap((f) => [...SOURCES[f].matchAll(re)].map((m) => m[2])))];
}

const FAMILIES = [
  ...REASON_KEYS.map((k) => `planning.reason.${k}`),
  ...['cycles_unavailable', 'regime_missing', 'rotation_restriction', 'rotation_unknown'].map((k) => `planning.warning.${k}`),
  ...['secano', 'regadío', 'unknown'].map((k) => `planning.regime.${k}`),
  ...['conventional', 'organic'].map((k) => `planning.management.${k}`),
  ...['actual', 'declared', 'planned', 'unknown'].map((k) => `planning.provenance.${k}`),
  ...['main_crop', 'cover_crop', 'catch_crop'].map((k) => `planning.role.${k}`),
  ...['fits', 'caveats', 'no_fit'].map((k) => `planning.fit.${k}`),
  ...['field', 'regional'].map((k) => `planning.variety.tier.${k}`),
  ...['stable', 'average', 'responsive'].map((k) => `planning.variety.${k}`),
  ...['trial_evidence', 'water_limited', 'gap_corrected', 'potential'].map((k) => `planning.estimate.basis.${k}`),
  ...['trial_evidence', 'aquacrop'].map((k) => `planning.estimate.provider.${k}`),
  ...['preseason_not_supported', 'engine_not_deployed', 'no_trials', 'no_trials_for_variety',
    'variety_not_in_evidence_sample', 'not_validated', 'needs_sowing_date', 'sowing_date_out_of_range', 'engine_error',
    'timeout', 'request_failed', 'async', 'water_limited_missing',
    // calibration reasons of the engine, prefixed by the backend: expected_<reason>
    ...['crop_not_calibrated', 'irrigated', 'outside_calibrated_regions', 'region_not_calibrated', 'missing', 'unknown']
      .map((r) => `expected_${r}`)].map((k) => `planning.estimate.unavailable.${k}`),
  ...COVER_METHODS.map((k) => `planning.plan.methods.${k}`),
  ...['sand', 'loamy-sand', 'sandy-loam', 'loam', 'silt-loam', 'silt', 'sandy-clay-loam', 'clay-loam', 'silty-clay-loam',
    'sandy-clay', 'silty-clay', 'clay'].map((k) => `planning.texture.${k}`),
  ...['missing_species', 'species_name_invalid', 'missing_dates', 'termination_outside_campaign', 'dates_inconsistent', 'cycles_overlap',
    'campaign_past', 'campaign_started', 'unknown_crop', 'generic', 'perennial_start_outside_campaign',
    'perennial_too_long', 'perennial_occupies', 'perennial_overlaps_plans', 'parcel_not_found',
    'woody_not_campaign_crop', 'contract_mismatch', 'invalid_response'].map((k) => `planning.plan.error.${k}`),
  'planning.plan.otherSpecies', 'planning.plan.speciesName',
  // Keys chosen at run time: loadErrorKey() and the lock reasons of ParcelDetail.
  'planning.error.session', 'planning.errorLoad', 'planning.lock.campaign_unknown',
  'planning.estimate.trials', 'planning.estimate.years', 'planning.estimate.simulating', 'planning.estimate.simulatingStart',
  ...['commercial', 'commercialNoPct', 'dry_matter', 'as_reported'].map((k) => `planning.estimate.moisture.${k}`),
  // Keys picked inside a conditional, t(cond ? 'a' : 'b'), which literalKeys() does not see.
  'planning.matrix.choose', 'planning.matrix.change', 'planning.sources.open', 'planning.sources.close',
  'planning.admin.open', 'planning.admin.close', 'planning.fit.trials', 'planning.fit.noTrials',
  'planning.variety.choose', 'planning.variety.chosen',
  // data_gaps the backend emits (app/graph/recommend.py, dao.py), shown by VarietyTable.
  ...['no_expected_yield', 'reference_too_small', 'reference_zero', 'cv_undefined', 'low_trial_count',
    'sowing_window_unavailable', 'conventional_only_trials', 'soil_unavailable', 'climate_detail_unavailable',
    'frost_tolerance_unavailable', 'sources_unavailable', 'no_measured_yield', 'regional_evidence_only',
    'regional_not_comparable', 'forage_basis_unknown', 'regional_zone_matched', 'zone_match_climatology_basis',
    'regional_country_level', 'irrigation_regime_unknown', 'irrigation_regime_unknown_all',
    'organic_units_excluded', 'evidence_truncated', 'insufficient_environments'].map((k) => `planning.gap.${k}`),
];

describe('planning strings', () => {
  it.each([['es', es], ['en', en]])('%s has every key the planning UI uses', (_lang, dict) => {
    const missing = [...literalKeys(), ...FAMILIES].filter((k) => !has(dict, k));
    expect(missing).toEqual([]);
  });
  it.each([['ca', ca], ['eu', eu], ['fr', fr], ['pt', pt]])('%s has the same planning keys as en', (_lang, dict) => {
    const flat = (node: unknown, prefix = ''): string[] => (node && typeof node === 'object'
      ? Object.entries(node as Record<string, unknown>).flatMap(([k, v]) => flat(v, prefix ? `${prefix}.${k}` : k))
      : [prefix]);
    const enKeys = flat((en as Record<string, unknown>).planning, 'planning');
    expect(enKeys.filter((k) => typeof lookup(dict, k) !== 'string')).toEqual([]);
  });
});
