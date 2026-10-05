/// <reference types="vite/client" />
import { describe, expect, it } from 'vitest';
import es from './es.json';
import en from './en.json';
import { KOPPEN_CODES, SEASONS, MANAGEMENTS, IRRIGATIONS, PURPOSES } from '../features/whatToSow/pageModel';
import { CAMPAIGN_TOOL_IDS, LIBRARY_TOOL_IDS } from '../utils/navigation';


const NAMESPACES = ['whatToSow', 'home', 'expert', 'app.doors', 'app.hubs', 'app.expertMode', 'app.cards'];

const SOURCES: Record<string, string> = {
  ...import.meta.glob('../features/whatToSow/**/*.{ts,tsx}', { query: '?raw', import: 'default', eager: true }),
  ...import.meta.glob('../components/Home.tsx', { query: '?raw', import: 'default', eager: true }),
  ...import.meta.glob('../App.tsx', { query: '?raw', import: 'default', eager: true }),
} as Record<string, string>;

const FILES = Object.keys(SOURCES).filter((f) => !/\.test\.tsx?$/.test(f));

/** Literal keys only: t('a.b') / t("a.b"); template-string keys are covered by the families below. */
function literalKeys(): { key: string; file: string }[] {
  const out: { key: string; file: string }[] = [];
  const re = /\bt\(\s*(['"])([A-Za-z0-9_.]+)\1/g;
  for (const file of FILES) {
    const text = SOURCES[file];
    for (const m of text.matchAll(re)) out.push({ key: m[2], file });
  }
  return out.filter(({ key }) => NAMESPACES.some((ns) => key === ns || key.startsWith(`${ns}.`)));
}

function lookup(dict: unknown, key: string): unknown {
  return key.split('.').reduce<unknown>(
    (node, part) => (node && typeof node === 'object' ? (node as Record<string, unknown>)[part] : undefined),
    dict,
  );
}

const exists = (dict: unknown, key: string): boolean =>
  ['', '_one', '_other'].some((suffix) => typeof lookup(dict, key + suffix) === 'string');

/** Gap ids emitted by backend/app/graph/recommend.py and dao.py (data_gaps). */
const GAP_IDS = [
  'climate_detail_unavailable', 'frost_tolerance_unavailable', 'sources_unavailable', 'soil_unavailable',
  'low_trial_count', 'conventional_only_trials', 'reference_too_small', 'reference_zero', 'cv_undefined',
  'sowing_window_unavailable', 'no_expected_yield',
];

const LEVELS = {
  soil: ['suitable', 'marginal', 'unsuitable', 'unknown'],
  water: ['low', 'medium', 'high', 'unknown'],
  frost: ['none', 'risk', 'unknown'],
};

const dynamicKeys = (): string[] => [
  ...KOPPEN_CODES.map((c) => `whatToSow.koppen.${c}`),
  ...GAP_IDS.map((g) => `whatToSow.gap.${g}`),
  ...Object.entries(LEVELS).flatMap(([kind, ls]) => ls.map((l) => `whatToSow.level.${kind}.${l}`)),
  ...Object.keys(LEVELS).map((kind) => `whatToSow.badge.${kind}`),
  ...['autumn', 'spring', 'summer', 'perennial', 'unknown'].map((s) => `whatToSow.season.${s}`),
  ...['secano', 'regadío'].map((i) => `whatToSow.irrigation.${i}`),
  ...['season', 'management', 'irrigation', 'purpose'].map((n) => `whatToSow.filter.${n}.label`),
  ...SEASONS.map((v) => `whatToSow.filter.season.${v}`),
  ...MANAGEMENTS.map((v) => `whatToSow.filter.management.${v}`),
  ...IRRIGATIONS.map((v) => `whatToSow.filter.irrigation.${v}`),
  ...PURPOSES.map((v) => `whatToSow.filter.purpose.${v}`),
  ...['kg_ha', 'dry_matter', 'fresh_matter', 'kg_dry_matter', 'kg_fresh_matter'].map((u) => `whatToSow.unit.${u}`),
  ...['dry_matter', 'fresh_matter'].map((b) => `whatToSow.basis.${b}`),
  ...['not_comparable', 'no_measured'].map((s) => `whatToSow.yieldStatus.${s}`),
  ...['high', 'medium', 'low', 'unknown'].map((l) => `whatToSow.compare.trustLevel.${l}`),
  ...Array.from({ length: 12 }, (_, i) => `whatToSow.compare.calendar.month.${i + 1}`),
  ...['expectedYield', 'relativeYield', 'stability', 'water', 'soil', 'frost', 'trust']
    .map((r) => `whatToSow.compare.${r}`),
  ...[...CAMPAIGN_TOOL_IDS, ...LIBRARY_TOOL_IDS].flatMap((id) => [`app.cards.${id}.title`, `app.cards.${id}.subtitle`]),
  ...['whatToSow', 'campaign', 'library'].flatMap((d) => [`app.doors.${d}.title`, `app.doors.${d}.description`]),
];

describe.each([['es', es], ['en', en]])('locale %s', (_name, dict) => {
  it('has every literal key used by the what-to-sow flow', () => {
    const used = literalKeys();
    expect(used.length).toBeGreaterThan(50);
    const missing = used.filter(({ key }) => !exists(dict, key)).map(({ key }) => key);
    expect([...new Set(missing)]).toEqual([]);
  });

  it('has every dynamic key family member', () => {
    const missing = dynamicKeys().filter((key) => !exists(dict, key));
    expect(missing).toEqual([]);
  });
});
