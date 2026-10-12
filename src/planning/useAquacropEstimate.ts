import { useCallback, useEffect, useState } from 'react';
import { fetchAquacrop, type EstimatesQuery } from './api';
import { AQUACROP_POLL_MS, pollDecision, retryFlag } from './estimatesModel';
import type { AquacropResponse } from './types';

export interface AquacropView {
  response: AquacropResponse | null; failure: 'timeout' | 'request_failed' | null; retry: () => void;
}
const IDLE = { response: null, failure: null } as const;

/**
 * Polls the AquaCrop provider while it is queued or running (every AQUACROP_POLL_MS, at most the
 * 4-minute cap); stops on done, failed, unavailable, a failed request, or unmount. The endpoint is
 * idempotent, so a poll after a pod restart simply restarts the job. `enabled` false = no request.
 */
export function useAquacropEstimate(parcelId: string, q: EstimatesQuery, enabled: boolean): AquacropView {
  const [view, setView] = useState<{ key: string } & Omit<AquacropView, 'retry'>>({ key: '', ...IDLE });
  const [retries, setRetries] = useState(0);
  const retry = useCallback(() => setRetries((n) => n + 1), []);
  const key = JSON.stringify([parcelId, q, enabled, retries]);
  useEffect(() => {
    if (!enabled) return undefined;
    const ctrl = new AbortController();
    const started = Date.now();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;
    const step = async () => {
      let response: AquacropResponse;
      try {
        response = await fetchAquacrop(parcelId, q, ctrl.signal, retryFlag(retries, attempt++));
      } catch {
        if (!ctrl.signal.aborted) setView({ key, response: null, failure: 'request_failed' });
        return;
      }
      if (ctrl.signal.aborted) return;
      const next = pollDecision(response.state, Date.now() - started);
      setView({ key, response, failure: next === 'timeout' ? 'timeout' : null });
      if (next === 'poll') timer = setTimeout(step, AQUACROP_POLL_MS);
    };
    step();
    return () => { ctrl.abort(); if (timer) clearTimeout(timer); };
    // `q` is serialised into `key`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);
  // An answer for other inputs is never shown.
  return view.key === key ? { response: view.response, failure: view.failure, retry } : { ...IDLE, retry };
}
