/**
 * A crop simulation run as a job (POST /graph/agriculture/crop-simulation/jobs, then poll it by id).
 * Identical inputs on the same day share one job, so a link from the in-season forecast joins the
 * simulation that forecast already ran instead of starting another.
 */
export type SimJobState = 'queued' | 'running' | 'done' | 'failed';
export interface SimJobView {
  job_id: string;
  state: SimJobState;
  done: number;
  total: number | null;
  result: unknown;
  error: { code?: string; message?: string } | null;
}

export class SimJobError extends Error {
  constructor(readonly code: string | null, message: string) {
    super(message);
    this.name = 'SimJobError';
  }
}

export const SIM_JOB_POLL_MS = 2000;
// ASSUMPTION: 5 minutes covers a queued job behind others (about a minute each, at most 3 per tenant).
export const SIM_JOB_CAP_MS = 5 * 60 * 1000;

interface JobApi {
  start: () => Promise<SimJobView>;
  get: (jobId: string) => Promise<SimJobView>;
}
interface Options {
  onProgress?: (done: number, total: number | null) => void;
  signal?: AbortSignal;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  pollMs?: number;
  capMs?: number;
}

const wait = (ms: number) => new Promise<void>((resolve) => { setTimeout(resolve, ms); });

function aborted(): Error {
  const e = new Error('aborted');
  e.name = 'AbortError';
  return e;
}

/** Starts (or joins) the job and polls it until done; a failed job, or the cap, raises SimJobError. */
export async function runSimulationJob<T = unknown>(api: JobApi, opts: Options = {}): Promise<T> {
  const sleep = opts.sleep ?? wait;
  const now = opts.now ?? Date.now;
  const pollMs = opts.pollMs ?? SIM_JOB_POLL_MS;
  const capMs = opts.capMs ?? SIM_JOB_CAP_MS;
  const started = now();
  let job = await api.start();
  for (;;) {
    if (opts.signal?.aborted) throw aborted();
    opts.onProgress?.(job.done, job.total);
    if (job.state === 'done') return job.result as T;
    if (job.state === 'failed') {
      throw new SimJobError(job.error?.code ?? null, job.error?.message || 'The simulation failed.');
    }
    if (now() - started >= capMs) throw new SimJobError('job_timeout', 'The simulation is taking too long.');
    await sleep(pollMs);
    if (opts.signal?.aborted) throw aborted();
    job = await api.get(job.job_id);
  }
}
