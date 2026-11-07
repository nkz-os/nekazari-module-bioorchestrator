import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('../testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});
import CompletionBar from './CompletionBar';
import StepCrop, { cardReasons } from './StepCrop';
import { anyCompletion, attemptWrite, completionNeeds, declarableCrops, remainingNeeds, type CompletionState } from '../completionModel';
import { PlanningApiError, fetchFit, clearDeclaredTexture, saveDeclaredCrop, saveDeclaredTexture, saveParcelRegime, USDA_TEXTURE_CLASSES } from '../api';
import type { FitResponse, Reason, SpeciesFit } from '../types';

afterEach(() => vi.unstubAllGlobals());

const reason = (code: string, params: Reason['params'] = {}): Reason => ({ code, gate: 'data', params });
const sp = (species: string, eppo: string | null, reasons: Reason[] = [], over: Partial<SpeciesFit> = {}): SpeciesFit => ({
  species, eppo, names: { es: species }, lifecycle: 'annual', verdict: 'caveats', reasons,
  sowing: { sowing_type: null, start_month: null, end_month: null, typical_sowing_doy: null, source: null },
  has_variety_trials: false, estimate_providers: [], ...over });
const fitOf = (species: SpeciesFit[], facts: Partial<FitResponse['facts']> = {}): FitResponse => ({
  contract_version: '2.5', parcel_id: 'p', campaign: 2027, graphs: [], species,
  facts: { history_missing_campaigns: [], ...facts } as FitResponse['facts'] });
type Row = { parcel_id: string; regime: 'rainfed' | null; regime_source: string };
const OWN: Row = { parcel_id: 'urn:ngsi-ld:AgriParcel:t:p1', regime: 'rainfed', regime_source: 'parcel:irrigationRegime' };
const NONE: Row = { ...OWN, regime: null, regime_source: 'unknown' };
const MISSING = [reason('regime_missing'), reason('soil_unknown', { why: 'no_parcel_soil' })];
const state = (skipped: string[] = [], saved: CompletionState['saved'] = new Set()): CompletionState =>
  ({ skipped: new Set(skipped), saved, skip: () => {}, markSaved: () => {} });
const html = (fit: FitResponse, row = NONE, st = state()) => renderToStaticMarkup(<CompletionBar fit={fit} row={row} onSaved={() => {}} state={st} />);

describe('completion needs', () => {
  const fit = fitOf([sp('wheat', 'TRZAX', MISSING), sp('barley', 'HORVX', MISSING)], { history_missing_campaigns: [2025, 2026] });
  it('asks for regime, soil and each missing campaign', () => {
    expect(completionNeeds(fit, NONE)).toEqual({ regime: true, soil: true, history: [2025, 2026] });
  });
  it('does not ask the regime of a parcel that has its own', () => {
    expect(completionNeeds(fit, OWN).regime).toBe(false);
  });
  it('a crop with no tolerance data says nothing about the soil; a complete history asks nothing', () => {
    const f = fitOf([sp('wheat', 'TRZAX', [reason('soil_unknown', { why: 'no_tolerance' })])], { history_missing_campaigns: null });
    expect(completionNeeds(f, OWN)).toEqual({ regime: false, soil: false, history: [] });
    expect(anyCompletion(completionNeeds(f, OWN))).toBe(false);
  });
  it('offers only main crops with a valid EPPO code, once', () => {
    const list = declarableCrops([sp('wheat', 'TRZAX'), sp('wheat2', 'TRZAX'), sp('cover', 'VICVI', [], { cover_only: true }), sp('x', null), sp('y', 'bad')]);
    expect(list.map((s) => s.species)).toEqual(['wheat']);
  });
});

describe('CompletionBar markup', () => {
  const fit = fitOf([sp('wheat', 'TRZAX', MISSING), sp('barley', 'HORVX', MISSING)], { history_missing_campaigns: [2025, 2026] });
  it('shows one control per missing fact, each with its reason', () => {
    const out = html(fit);
    expect(out).toContain('¿Secano o regadío?');
    expect(out).toContain('Secano');
    expect(out).toContain('Regadío');
    expect(out).toContain('¿Cuál es la textura del suelo?');
    expect(out).toContain('Campaña 2025');
    expect(out).toContain('Campaña 2026');
    expect(out).toContain('No lo sé');
    expect(out).toContain('Con el régimen sabremos');
  });
  it('shows nothing for a parcel with a regime, known soil and a complete history', () => {
    expect(html(fitOf([sp('wheat', 'TRZAX')]), OWN)).toBe('');
  });
  it('leaves out the regime control for a parcel with a regime of its own', () => {
    expect(html(fit, OWN)).not.toContain('¿Secano o regadío?');
  });
  it('StepCrop puts the bar above the recommended list and stops repeating the reasons it asks about', () => {
    const out = renderToStaticMarkup(<StepCrop fit={fit} selected={null} onPick={() => {}} row={NONE} onCompleted={() => {}} completion={state()} />);
    expect(out.indexOf('Completa los datos')).toBeGreaterThan(-1);
    expect(out.indexOf('Completa los datos')).toBeLessThan(out.indexOf('Recomendados para esta parcela'));
    expect(cardReasons(fit.species[0], (r) => r.code === 'regime_missing').map((r) => r.code)).toEqual(['soil_unknown']);
  });
});

describe('skipped and saved controls', () => {
  const fit = fitOf([sp('wheat', 'TRZAX', MISSING), sp('barley', 'HORVX', MISSING)], { history_missing_campaigns: [2025, 2026] });
  const step = (st: CompletionState) => renderToStaticMarkup(
    <StepCrop fit={fit} selected={null} onPick={() => {}} row={NONE} onCompleted={() => {}} completion={st} />);
  it('while the soil control is offered the card does not repeat the soil reason; after "No lo sé" it does', () => {
    expect(step(state())).not.toContain('Falta el suelo de la parcela');
    const after = step(state(['soil']));
    expect(after).toContain('Falta el suelo de la parcela');
    expect(after).not.toContain('¿Cuál es la textura del suelo?');
  });
  it('a skipped campaign leaves the other one', () => {
    expect(remainingNeeds(completionNeeds(fit, NONE), new Set(['history:2025'])).history).toEqual([2026]);
  });
  it('a saved fact is acknowledged even when the control is still needed', () => {
    expect(html(fit, NONE, state([], new Set(['soil'])))).toContain('Textura guardada');
  });
});

describe('writes', () => {
  const ok = (body: unknown = {}, status = 200) => vi.fn().mockResolvedValue({ ok: true, status, json: async () => body });
  const refused = (status: number, body: unknown = {}) => vi.fn().mockResolvedValue({ ok: false, status, json: async () => body });
  const PID = 'urn:ngsi-ld:AgriParcel:t:p1';

  it('regime: PATCHes the parcel in entity-manager', async () => {
    const f = ok(); vi.stubGlobal('fetch', f);
    await saveParcelRegime(PID, 'irrigated');
    expect(String(f.mock.calls[0][0])).toContain('/api/entities/parcels/urn%3Angsi-ld%3AAgriParcel%3At%3Ap1');
    expect(f.mock.calls[0][1].method).toBe('PATCH');
  });
  it('soil: PUTs the USDA class to the soil module with credentials', async () => {
    const f = ok(); vi.stubGlobal('fetch', f);
    await saveDeclaredTexture(PID, 'clay-loam');
    expect(String(f.mock.calls[0][0])).toMatch(/\/api\/soil\/parcel\/t%3Ap1\/declared-texture$/);
    expect(f.mock.calls[0][1]).toMatchObject({ method: 'PUT', credentials: 'include', body: JSON.stringify({ usda_texture_class: 'clay-loam' }) });
  });
  it('soil: a full URN is sent the way the soil module builds it, without the URN prefix', async () => {
    const f = ok(); vi.stubGlobal('fetch', f);
    await saveDeclaredTexture('urn:ngsi-ld:AgriParcel:t:p1', 'loam');
    expect(String(f.mock.calls[0][0])).toMatch(/\/api\/soil\/parcel\/t%3Ap1\/declared-texture$/);
    await clearDeclaredTexture('urn:ngsi-ld:AgriParcel:t:p1');
    expect(String(f.mock.calls[1][0])).toMatch(/\/api\/soil\/parcel\/t%3Ap1\/declared-texture$/);
    expect(f.mock.calls[1][1].method).toBe('DELETE');
  });
  it('fit: only the read after a soil write carries the refresh flag', async () => {
    const f = ok({ species: [] }); vi.stubGlobal('fetch', f);
    await fetchFit(PID, 2027, null);
    await fetchFit(PID, 2027, null, undefined, true);
    expect(String(f.mock.calls[0][0])).not.toContain('refresh_soil');
    expect(String(f.mock.calls[1][0])).toContain('refresh_soil=1');
  });
  it('soil: offers the 12 classes the soil module emits', () => {
    expect(USDA_TEXTURE_CLASSES).toHaveLength(12);
    expect(USDA_TEXTURE_CLASSES).toContain('silty-clay-loam');
  });
  it('history: POSTs campaign, EPPO and optional variety/yield to field operations and returns the crop id', async () => {
    const f = ok({ crop_id: 'urn:c1' }, 201); vi.stubGlobal('fetch', f);
    expect(await saveDeclaredCrop(PID, 2026, 'TRZAX')).toBe('urn:c1');
    expect(String(f.mock.calls[0][0])).toMatch(/\/api\/field-operations\/parcels\/urn%3Angsi-ld%3AAgriParcel%3At%3Ap1\/declared-crops$/);
    expect(f.mock.calls[0][1]).toMatchObject({ method: 'POST', credentials: 'include', body: JSON.stringify({ campaign: 2026, eppo: 'TRZAX' }) });
    await saveDeclaredCrop(PID, 2025, 'HORVX', { variety: 'Pewter', yieldKgHa: 4200 });
    expect(f.mock.calls[1][1]).toMatchObject({
      method: 'POST',
      credentials: 'include',
      body: JSON.stringify({ campaign: 2025, eppo: 'HORVX', variety: 'Pewter', yield_kg_ha: 4200 }),
    });
  });
  it('a refused history write carries the module error code', async () => {
    vi.stubGlobal('fetch', refused(409, { detail: { code: 'campaign_has_dated_crop' } }));
    await expect(saveDeclaredCrop(PID, 2026, 'TRZAX')).rejects.toMatchObject({ status: 409, code: 'campaign_has_dated_crop' });
  });
  it('attemptWrite: success means refetch; each failure becomes its message and does not throw', async () => {
    expect(await attemptWrite('regime', async () => undefined)).toEqual({ ok: true });
    const fail = (kind: 'soil' | 'history' | 'regime', status: number, code: string | null) =>
      attemptWrite(kind, async () => { throw new PlanningApiError(status, code); });
    expect(await fail('soil', 404, null)).toEqual({ ok: false, errorKey: 'soilNotReady' });
    for (const code of ['campaign_not_past', 'invalid_campaign', 'invalid_species', 'campaign_has_dated_crop']) {
      expect(await fail('history', code === 'campaign_has_dated_crop' ? 409 : 422, code)).toEqual({ ok: false, errorKey: code });
    }
    expect(await fail('regime', 500, null)).toEqual({ ok: false, errorKey: 'generic' });
    expect(await attemptWrite('history', async () => { throw new TypeError('network'); })).toEqual({ ok: false, errorKey: 'generic' });
  });
});
