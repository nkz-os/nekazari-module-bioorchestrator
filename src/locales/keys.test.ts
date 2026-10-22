/// <reference types="vite/client" />
import { describe, expect, it } from 'vitest';
import es from './es.json';
import en from './en.json';
import ca from './ca.json';
import eu from './eu.json';
import fr from './fr.json';
import pt from './pt.json';
import { KNOWN_ERROR_CODES, WEATHER_SOURCES, SOWING_SOURCES } from '../components/cropSimulationModel';

const NAMESPACES = ['sourceAttribution', 'cropSimulation', 'scenarioMode'];

const SOURCES: Record<string, string> = {
  ...import.meta.glob('../components/CropSimulation.tsx', { query: '?raw', import: 'default', eager: true }),
  ...import.meta.glob('../components/shared/SourceAttribution.tsx', { query: '?raw', import: 'default', eager: true }),
} as Record<string, string>;

const FILES = Object.keys(SOURCES).filter((f) => !/\.test\.tsx?$/.test(f));

/** Literal keys only: t('a.b') / t("a.b"); template-string keys are covered by the families below. */
function literalKeys(): { key: string; file: string }[] {
  const out: { key: string; file: string }[] = [];
  const re = /\bt\(\s*(['"])([A-Za-z0-9_.]+)\1/g;
  for (const file of FILES) {
    for (const m of SOURCES[file].matchAll(re)) out.push({ key: m[2], file });
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

const dynamicKeys = (): string[] => [
  ...[...KNOWN_ERROR_CODES, 'generic'].map((c) => `cropSimulation.errors.${c}`),
  ...WEATHER_SOURCES.map((c) => `cropSimulation.weatherSource.${c}`),
  ...SOWING_SOURCES.map((c) => `cropSimulation.sowingSource.${c}`),
  ...['complete', 'in_season'].map((c) => `cropSimulation.status.${c}`),
  ...['spinup', 'assumed_fc'].map((c) => `cropSimulation.initialWater.${c}`),
];

describe.each([['es', es], ['en', en]])('locale %s', (_name, dict) => {
  it('has every literal key used by the simulation and attribution components', () => {
    const used = literalKeys();
    expect(used.length).toBeGreaterThan(10);
    const missing = used.filter(({ key }) => !exists(dict, key)).map(({ key }) => key);
    expect([...new Set(missing)]).toEqual([]);
  });

  it('has every dynamic key family member', () => {
    const missing = dynamicKeys().filter((key) => !exists(dict, key));
    expect(missing).toEqual([]);
  });
});

describe.each([['ca', ca], ['eu', eu], ['fr', fr], ['pt', pt], ['es', es], ['en', en]])('cropSimulation in %s', (_name, dict) => {
  it('has the same keys as en', () => {
    const flat = (n: unknown, p = ''): string[] =>
      n && typeof n === 'object'
        ? Object.entries(n as object).flatMap(([k, v]) => flat(v, p ? `${p}.${k}` : k))
        : [p];
    expect(flat(lookup(dict, 'cropSimulation')).sort()).toEqual(flat(lookup(en, 'cropSimulation')).sort());
  });
});
