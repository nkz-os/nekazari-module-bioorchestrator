/// <reference types="vite/client" />
import { describe, expect, it } from 'vitest';
import es from './es.json';
import en from './en.json';

/** The source registry the API serves the credits from (single source of the fidelity line). */
const RAW = Object.values(
  import.meta.glob('../../backend/data/sources_registry.json', { query: '?raw', import: 'default', eager: true }),
)[0] as string;

interface RegistryEntry {
  source_id: string;
  attribution_text?: string;
  processing_note?: Record<string, string>;
}
const REGISTRY = JSON.parse(RAW) as RegistryEntry[];
const WITH_CREDIT = REGISTRY.filter((s) => s.attribution_text);

describe('the UI fidelity line equals the registry processing note', () => {
  it('has sources with a credit to check (GENVCE, CREA)', () => {
    expect(WITH_CREDIT.map((s) => s.source_id)).toEqual(expect.arrayContaining(['GENVCE', 'CREA']));
  });

  it.each([['es', es], ['en', en]])('every credited source: note (%s) is the sourceAttribution.calculations text', (lang, dict) => {
    const line = (dict as { sourceAttribution: { calculations: string } }).sourceAttribution.calculations;
    expect(line.length).toBeGreaterThan(20);
    for (const s of WITH_CREDIT) {
      expect(s.processing_note?.[lang], `${s.source_id} ${lang}`).toBe(line);
    }
  });
});
