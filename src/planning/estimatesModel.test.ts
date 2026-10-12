import { describe, expect, it } from 'vitest';
import {
  AQUACROP_POLL_MS, AQUACROP_POLL_CAP_MS, aquacropPending, disagrees, mergeAquacrop, moistureLabel, pollDecision, progressLabel, retryFlag, canRetry,
  failureFor, jobIdForPoll, nextRetry, retryCountFor, unavailableKey,
  sortEstimates, uniqueAttributions, validationTone,
} from './estimatesModel';
import type { AquacropResponse, Estimate, EstimatesResponse } from './types';

const est = (basis: Estimate['basis'], source = 'GENVCE'): Estimate => ({
  provider_id: basis, provider_version: 'v', basis, unit: 't/ha', percentiles: { p10: 1, p50: 2, p90: 3 }, range: null, coverage: null,
  moisture: { basis: 'as_reported', pct: null }, n: 1,
  n_kind: 'trials', validation: { status: 'not_applicable', metric: null, scope: null }, warnings: [], graph_id: 'global',
  attributions: [{ source_id: source, text: source, url: '', licence_id: '', licence_url: '' }],
});

describe('estimates model', () => {
  it('orders trials first, then gap-corrected (the expected yield), then the water-limited ceiling', () => {
    expect(sortEstimates([est('water_limited'), est('trial_evidence'), est('gap_corrected')]).map((e) => e.basis))
      .toEqual(['trial_evidence', 'gap_corrected', 'water_limited']);
  });
  it('an unknown basis sorts last, never first', () => {
    expect(sortEstimates([est('bogus' as unknown as Estimate['basis']), est('trial_evidence')]).map((e) => e.basis))
      .toEqual(['trial_evidence', 'bogus']);
  });
  it('validation tone: validated is positive, unvalidated and experimental warn, not_applicable is neutral', () => {
    expect(validationTone('validated')).toBe('positive');
    expect(validationTone('unvalidated')).toBe('warning');
    expect(validationTone('experimental')).toBe('warning');
    expect(validationTone('not_applicable')).toBe('default');
  });
  it('attributions once per source; experimental is a warning', () => {
    expect(uniqueAttributions([est('trial_evidence'), est('water_limited')]).length).toBe(1);
    expect(validationTone('experimental')).toBe('warning');
  });
});

const C13 = { basis: 'commercial', pct: 13 } as const;
const DM = { basis: 'dry_matter', pct: null } as const;
const model = (p50: number, moisture: Estimate['moisture']): Estimate => ({ ...est('water_limited'), provider_id: 'm', percentiles: { p10: p50, p50, p90: p50 }, moisture });
const rangeOnly: Estimate = { ...est('gap_corrected'), percentiles: null, range: { low: 2, high: 5 }, coverage: 0.78, moisture: C13 };

describe('disagreement (same rule as the backend)', () => {
  it('compares P50 only at the same basis AND the same moisture', () => {
    expect(disagrees([model(8, C13), model(3, DM)])).toBe(false);
    expect(disagrees([model(8, C13), model(3, { basis: 'commercial', pct: 14 })])).toBe(false);
    expect(disagrees([model(8, C13), model(3, C13)])).toBe(true);
  });
  it('range-only figures and trials never take part', () => {
    expect(disagrees([rangeOnly, rangeOnly, model(5, C13)])).toBe(false);
    expect(disagrees([est('trial_evidence'), est('trial_evidence')])).toBe(false);
  });
});

const aq = (over: Partial<AquacropResponse>): AquacropResponse => ({
  contract_version: '1.1', provider_id: 'aquacrop', state: 'queued', done: 0, total: null, estimates: [], unavailable: [], job_id: 'j', ...over,
});
const BASE: EstimatesResponse = {
  contract_version: '1.1', parcel_id: 'p', crop: 'TRZAX', variety: null, status: 'ok', disagreement: false,
  inputs: { regime: 'secano', management: 'conventional', sowing_date: '2026-11-05', sowing_source: 'planned', soil_source: 's', climate_source: 'c' },
  estimates: [est('trial_evidence')], unavailable: [{ provider_id: 'aquacrop', reason: 'async' }],
};

describe('polling', () => {
  it('knows when AquaCrop will answer', () => {
    expect(aquacropPending(BASE)).toBe(true);
    expect(aquacropPending({ ...BASE, unavailable: [{ provider_id: 'aquacrop', reason: 'not_validated' }] })).toBe(false);
  });
  it('polls while queued or running, stops on any settled state', () => {
    expect(AQUACROP_POLL_MS).toBe(3000);
    for (const s of ['queued', 'running'] as const) expect(pollDecision(s, 1000)).toBe('poll');
    for (const s of ['done', 'failed', 'unavailable'] as const) expect(pollDecision(s, 1000)).toBe('stop');
    expect(pollDecision(null, 0)).toBe('poll'); // first request not answered yet
  });
  it('gives up after the 4-minute cap, and says it did', () => {
    expect(AQUACROP_POLL_CAP_MS).toBe(240000);
    expect(pollDecision('running', AQUACROP_POLL_CAP_MS)).toBe('timeout');
    expect(pollDecision('done', AQUACROP_POLL_CAP_MS)).toBe('stop');
  });
});

describe('merging the AquaCrop answer into the table', () => {
  it('keeps the async row while it runs', () => {
    expect(mergeAquacrop(BASE, aq({ state: 'running', done: 3, total: 12 })).data.unavailable).toEqual(BASE.unavailable);
    expect(mergeAquacrop(BASE, aq({ state: 'running', done: 3, total: 12 })).progress).toEqual({ done: 3, total: 12 });
    expect(mergeAquacrop(BASE, null).progress).toEqual({ done: 0, total: null });
  });
  it('done: its estimates join and the async row goes', () => {
    const out = mergeAquacrop(BASE, aq({ state: 'done', estimates: [rangeOnly, model(3, C13)], unavailable: [{ provider_id: 'aquacrop', reason: 'expected_missing' }] }));
    expect(out.progress).toBeNull();
    expect(out.data.estimates.length).toBe(3);
    expect(out.data.unavailable).toEqual([{ provider_id: 'aquacrop', reason: 'expected_missing' }]);
  });
  it('recomputes the disagreement client-side', () => {
    const out = mergeAquacrop({ ...BASE, estimates: [model(8, C13)] }, aq({ state: 'done', estimates: [model(3, C13)] }));
    expect(out.data.disagreement).toBe(true);
  });
  it('failed is shown as failed, never as an empty result', () => {
    const out = mergeAquacrop(BASE, aq({ state: 'failed', unavailable: [{ provider_id: 'aquacrop', reason: 'engine_error', code: 'x' }] }));
    expect(out.data.unavailable).toEqual([{ provider_id: 'aquacrop', reason: 'engine_error', code: 'x' }]);
    expect(out.progress).toBeNull();
  });
  it('a timeout or a failed request is a reason too', () => {
    expect(mergeAquacrop(BASE, null, 'still_queued').data.unavailable).toEqual([{ provider_id: 'aquacrop', reason: 'still_queued' }]);
    expect(mergeAquacrop(BASE, null, 'request_failed').data.unavailable[0].reason).toBe('request_failed');
  });
  it('without async pending the data is untouched', () => {
    const settled = { ...BASE, unavailable: [{ provider_id: 'aquacrop', reason: 'not_validated' }] };
    expect(mergeAquacrop(settled, null)).toEqual({ data: settled, progress: null });
  });
});

describe('moisture label', () => {
  it('names the basis next to every figure', () => {
    expect(moistureLabel({ basis: 'commercial', pct: 13 })).toEqual({ key: 'planning.estimate.moisture.commercial', params: { pct: 13 } });
    expect(moistureLabel({ basis: 'commercial', pct: null })?.key).toBe('planning.estimate.moisture.commercialNoPct');
    expect(moistureLabel({ basis: 'dry_matter', pct: null })?.key).toBe('planning.estimate.moisture.dry_matter');
    expect(moistureLabel({ basis: 'as_reported', pct: null })?.key).toBe('planning.estimate.moisture.as_reported');
  });
});

describe('progress label', () => {
  it('shows only "Simulando…" until the total is a positive number', () => {
    for (const total of [null, 0]) expect(progressLabel({ done: 0, total }).key).toBe('planning.estimate.simulatingStart');
    expect(progressLabel({ done: 3, total: 12 })).toEqual({ key: 'planning.estimate.simulating', params: { done: 3, total: 12 } });
  });
});

describe('retry', () => {
  it('only the first request after a retry carries retry=true', () => {
    expect(retryFlag(0, 0)).toBe(false);
    expect(retryFlag(1, 0)).toBe(true);
    expect(retryFlag(1, 1)).toBe(false);
    expect(retryFlag(2, 5)).toBe(false);
  });
  it('is offered for an engine error only', () => {
    expect(canRetry({ provider_id: 'aquacrop', reason: 'engine_error' })).toBe(true);
    for (const reason of ['request_failed', 'still_queued']) expect(canRetry({ provider_id: 'aquacrop', reason })).toBe(true);
    for (const reason of ['timeout', 'not_validated', 'engine_not_deployed']) expect(canRetry({ provider_id: 'aquacrop', reason })).toBe(false);
  });
});

describe('deploy skew', () => {
  it('a payload without the 1.1 fields renders nothing for them', () => {
    expect(moistureLabel(undefined)).toBeNull();
    expect(moistureLabel(null)).toBeNull();
  });
  it('disagreement tolerates estimates without moisture', () => {
    const old = (p50: number) => ({ ...model(p50, C13), moisture: undefined } as unknown as Estimate);
    expect(disagrees([old(8), old(3)])).toBe(true);
  });
  it('a 404 or 405 from the endpoint means the backend is not deployed; anything else is a failed request', () => {
    expect(failureFor({ status: 404 })).toBe('engine_not_deployed');
    expect(failureFor({ status: 405 })).toBe('engine_not_deployed');
    expect(failureFor({ status: 500 })).toBe('request_failed');
    expect(failureFor(new TypeError('network'))).toBe('request_failed');
    expect(mergeAquacrop(BASE, null, 'engine_not_deployed').data.unavailable).toEqual([{ provider_id: 'aquacrop', reason: 'engine_not_deployed' }]);
  });
});

describe('retry state', () => {
  const IN = 'inputs-a';
  it('retry=true applies to the click, for the same inputs only', () => {
    const clicked = nextRetry({ inputs: '', retries: 0, resumes: 0 }, IN, 'retry');
    expect(retryCountFor(clicked, IN)).toBe(1);
    expect(retryCountFor(clicked, 'inputs-b')).toBe(0); // any input change resets it
    expect(retryFlag(retryCountFor(clicked, 'inputs-b'), 0)).toBe(false);
  });
  it('a click under other inputs starts from zero', () => {
    const a = nextRetry({ inputs: '', retries: 0, resumes: 0 }, IN, 'retry');
    const b = nextRetry(a, 'inputs-b', 'retry');
    expect(b).toEqual({ inputs: 'inputs-b', retries: 1, resumes: 0 });
  });
  it('resume restarts polling without retry=true', () => {
    const r = nextRetry({ inputs: IN, retries: 0, resumes: 0 }, IN, 'resume');
    expect(r.resumes).toBe(1);
    expect(retryFlag(retryCountFor(r, IN), 0)).toBe(false);
  });
});

describe('light polls', () => {
  it('the first request has no job id, later ones carry the previous answer\'s', () => {
    expect(jobIdForPoll('j1', 0)).toBeNull();
    expect(jobIdForPoll('j1', 1)).toBe('j1');
    expect(jobIdForPoll(null, 3)).toBeNull();
  });
});

describe('unavailable reason keys', () => {
  it('an unknown expected_* reason falls back to expected_unknown', () => {
    expect(unavailableKey('expected_future_reason')).toBe('planning.estimate.unavailable.expected_unknown');
    expect(unavailableKey('expected_irrigated')).toBe('planning.estimate.unavailable.expected_irrigated');
    expect(unavailableKey('something_else')).toBe('planning.estimate.unavailable.something_else');
  });
});
