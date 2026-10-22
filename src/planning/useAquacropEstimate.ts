import { useCallback, useEffect, useRef, useState } from 'react';
import { fetchAquacrop, type AquacropQuery } from './api';
import {
  AQUACROP_POLL_MS, failureFor, jobIdForPoll, nextRetry, pollDecision, retryCountFor, retryFlag, type AquacropFailure,
} from './estimatesModel';
import type { AquacropResponse } from './types';

export interface AquacropView {
  response: AquacropResponse | null; failure: AquacropFailure | null; retry: () => void;
}
const IDLE = { response: null, failure: null } as const;

/**
 * Polls the AquaCrop provider while it is queued or running (every AQUACROP_POLL_MS, at most the
 * 4-minute cap); stops on done, failed, unavailable, a failed request, or unmount. The endpoint is
 * idempotent, so a poll after a pod restart simply restarts the job. `enabled` false = no request.
 */
export function useAquacropEstimate(parcelId: string, q: AquacropQuery, enabled: boolean): AquacropView {
  const [view, setView] = useState<{ key: string } & Omit<AquacropView, 'retry'>>({ key: '', ...IDLE });
  const inputs = JSON.stringify([parcelId, q.crop, q.regime, q.sowingDate, q.sowingMethod ?? null, enabled]);
  const [clicks, setClicks] = useState({ inputs: '', retries: 0, resumes: 0 });
  const failed = useRef(false);
  const retries = retryCountFor(clicks, inputs);
  const key = JSON.stringify([inputs, retries, clicks.inputs === inputs ? clicks.resumes : 0]);
  // A request that failed or gave up is resumed without relaunching; a failed engine is retried.
  const retry = useCallback(
    () => setClicks((c) => nextRetry(c, inputs, failed.current ? 'resume' : 'retry')), [inputs]);
  useEffect(() => {
    if (!enabled) return undefined;
    const ctrl = new AbortController();
    const started = Date.now();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;
    let lastJobId: string | null = null;
    const step = async () => {
      let response: AquacropResponse;
      try {
        response = await fetchAquacrop(parcelId, q, ctrl.signal,
          { retry: retryFlag(retries, attempt), jobId: jobIdForPoll(lastJobId, attempt) });
        attempt += 1;
        lastJobId = response.job_id;
      } catch (error) {
        if (!ctrl.signal.aborted) setView({ key, response: null, failure: failureFor(error) });
        return;
      }
      if (ctrl.signal.aborted) return;
      const next = pollDecision(response.state, Date.now() - started);
      setView({ key, response, failure: next === 'timeout' ? 'still_queued' : null });
      if (next === 'poll') timer = setTimeout(step, AQUACROP_POLL_MS);
    };
    step();
    return () => { ctrl.abort(); if (timer) clearTimeout(timer); };
    // `q`'s fields are serialised into `key`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);
  // An answer for other inputs is never shown.
  failed.current = view.key === key && view.failure !== null && view.failure !== 'engine_not_deployed';
  return view.key === key ? { response: view.response, failure: view.failure, retry } : { ...IDLE, retry };
}
