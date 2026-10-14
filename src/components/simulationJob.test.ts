import { describe, expect, it } from 'vitest';
import { runSimulationJob, type SimJobView } from './simulationJob';

const view = (over: Partial<SimJobView>): SimJobView =>
  ({ job_id: 'j1', state: 'queued', done: 0, total: null, result: null, error: null, ...over });

function fakeApi(answers: SimJobView[]) {
  const polled: string[] = [];
  return {
    polled,
    start: async () => answers.shift()!,
    get: async (id: string) => { polled.push(id); return answers.shift()!; },
  };
}
const noSleep = async () => {};

describe('runSimulationJob', () => {
  it('starts, reports progress and returns the result once done', async () => {
    const api = fakeApi([view({}), view({ state: 'running', done: 1, total: 3 }), view({ state: 'done', done: 3, total: 3, result: { ok: 1 } })]);
    const seen: [number, number | null][] = [];
    const out = await runSimulationJob(api, { sleep: noSleep, onProgress: (d, t) => seen.push([d, t]) });
    expect(out).toEqual({ ok: 1 });
    expect(api.polled).toEqual(['j1', 'j1']);
    expect(seen).toContainEqual([1, 3]);
  });

  it('a job already finished (shared with another request) answers without polling', async () => {
    const api = fakeApi([view({ state: 'done', result: { ok: 2 } })]);
    expect(await runSimulationJob(api, { sleep: noSleep })).toEqual({ ok: 2 });
    expect(api.polled).toEqual([]);
  });

  it('a failed job raises its code and message', async () => {
    const api = fakeApi([view({ state: 'failed', error: { code: 'soil_unavailable', message: 'No soil.' } })]);
    await expect(runSimulationJob(api, { sleep: noSleep })).rejects.toMatchObject({ code: 'soil_unavailable', message: 'No soil.' });
  });

  it('gives up at the cap with job_timeout', async () => {
    let now = 0;
    const api = { start: async () => view({}), get: async () => view({ state: 'running' }) };
    const sleep = async (ms: number) => { now += ms; };
    await expect(runSimulationJob(api, { sleep, now: () => now, capMs: 10_000, pollMs: 3000 }))
      .rejects.toMatchObject({ code: 'job_timeout' });
  });

  it('stops quietly when aborted', async () => {
    const ctrl = new AbortController();
    const api = { start: async () => { ctrl.abort(); return view({}); }, get: async () => view({ state: 'running' }) };
    await expect(runSimulationJob(api, { sleep: noSleep, signal: ctrl.signal })).rejects.toMatchObject({ name: 'AbortError' });
  });
});
