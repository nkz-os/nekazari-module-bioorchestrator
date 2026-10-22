import { useEffect, useState } from 'react';

export interface LoadState<T> { key: string; data: T | null; error: unknown; loading: boolean }
export interface LoadView<T> { data: T | null; error: unknown; loading: boolean }

/**
 * The view for the current dependency key. A state produced for another key is never shown: the
 * render that follows a key change reports loading (or idle when disabled) with no data, not the
 * previous request's data.
 */
export function deriveLoadState<T>(state: LoadState<T>, key: string, enabled: boolean): LoadView<T> {
  if (state.key === key) return { data: state.data, error: state.error, loading: state.loading };
  return { data: null, error: null, loading: enabled };
}

/**
 * Loads on mount and whenever `deps` change, or `retry` is called. The previous request is aborted
 * on change and on unmount, and a late answer of an aborted request is ignored. A render after a
 * key change never returns the previous request's data (see deriveLoadState). `deps` must hold
 * JSON-serialisable values.
 */
export function useLoad<T>(load: (signal: AbortSignal) => Promise<T>, deps: unknown[], enabled = true) {
  const [nonce, setNonce] = useState(0);
  const key = JSON.stringify([...deps, nonce, enabled]);
  const [state, setState] = useState<LoadState<T>>({ key, data: null, error: null, loading: enabled });
  useEffect(() => {
    if (!enabled) {
      setState({ key, data: null, error: null, loading: false });
      return undefined;
    }
    const ctrl = new AbortController();
    setState({ key, data: null, error: null, loading: true });
    load(ctrl.signal)
      .then((data) => { if (!ctrl.signal.aborted) setState({ key, data, error: null, loading: false }); })
      .catch((error: unknown) => { if (!ctrl.signal.aborted) setState({ key, data: null, error, loading: false }); });
    return () => ctrl.abort();
    // `load` is a fresh closure on every render; `deps` name what it depends on.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce, enabled]);
  return { ...deriveLoadState(state, key, enabled), retry: () => setNonce((n) => n + 1) };
}
