import { useEffect, useState } from 'react';

interface LoadState<T> { data: T | null; error: unknown; loading: boolean }

/**
 * Loads on mount and whenever `deps` change; aborts the previous request and clears its data
 * first, so a slow answer for the previous parcel or crop is never shown for the new one.
 */
export function useLoad<T>(load: (signal: AbortSignal) => Promise<T>, deps: unknown[], enabled = true) {
  const [state, setState] = useState<LoadState<T>>({ data: null, error: null, loading: enabled });
  const [nonce, setNonce] = useState(0);
  useEffect(() => {
    if (!enabled) {
      setState({ data: null, error: null, loading: false });
      return undefined;
    }
    const ctrl = new AbortController();
    setState({ data: null, error: null, loading: true });
    load(ctrl.signal)
      .then((data) => { if (!ctrl.signal.aborted) setState({ data, error: null, loading: false }); })
      .catch((error: unknown) => { if (!ctrl.signal.aborted) setState({ data: null, error, loading: false }); });
    return () => ctrl.abort();
    // `load` is a fresh closure on every render; `deps` name what it depends on.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce, enabled]);
  return { ...state, retry: () => setNonce((n) => n + 1) };
}
