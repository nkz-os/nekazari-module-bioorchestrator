import { PlanningApiError } from './api';

/**
 * The i18n key for a failed load. A contract mismatch and an expired session cannot be fixed by
 * retrying, so they say what to do; everything else is a plain "try again".
 */
export function loadErrorKey(error: unknown): string {
  if (error instanceof PlanningApiError) {
    if (error.code === 'contract_mismatch') return 'planning.plan.error.contract_mismatch';
    if (error.status === 401) return 'planning.error.session';
  }
  return 'planning.errorLoad';
}

const CHUNK_MESSAGE = /dynamically imported module|Loading chunk|Failed to fetch/i;

/**
 * A lazily imported part that could not be fetched (a deploy replaced the file, or the network
 * dropped). React.lazy caches the rejected import, so only a page reload recovers it.
 */
export function isChunkLoadError(error: unknown): boolean {
  if (typeof error === 'string') return CHUNK_MESSAGE.test(error);
  if (error === null || typeof error !== 'object') return false;
  const { name, message } = error as { name?: unknown; message?: unknown };
  return name === 'ChunkLoadError' || (typeof message === 'string' && CHUNK_MESSAGE.test(message));
}
