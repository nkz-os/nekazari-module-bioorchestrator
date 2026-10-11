import { describe, expect, it } from 'vitest';
import { PlanningApiError } from './api';
import { isChunkLoadError, loadErrorKey } from './errors';

describe('loadErrorKey', () => {
  it('a contract mismatch asks for a reload, an expired session for a new sign-in, anything else for a retry', () => {
    expect(loadErrorKey(new PlanningApiError(200, 'contract_mismatch'))).toBe('planning.plan.error.contract_mismatch');
    expect(loadErrorKey(new PlanningApiError(401, null))).toBe('planning.error.session');
    expect(loadErrorKey(new PlanningApiError(500, 'boom'))).toBe('planning.errorLoad');
    expect(loadErrorKey(new PlanningApiError(404, null))).toBe('planning.errorLoad');
    expect(loadErrorKey(new PlanningApiError(200, 'invalid_response'))).toBe('planning.errorLoad');
  });
  it('an error that is not an API error is a plain load failure', () => {
    for (const e of [new Error('network'), new TypeError('Failed to fetch'), null, undefined, 'x', { status: 401 }]) {
      expect(loadErrorKey(e)).toBe('planning.errorLoad');
    }
  });
});

describe('isChunkLoadError', () => {
  it('recognises a chunk that failed to load, by name or by the browsers\' messages', () => {
    const named = new Error('x');
    named.name = 'ChunkLoadError';
    expect(isChunkLoadError(named)).toBe(true);
    expect(isChunkLoadError(new TypeError('Failed to fetch dynamically imported module: a.js'))).toBe(true);
    expect(isChunkLoadError(new Error('Loading chunk 12 failed.'))).toBe(true);
    expect(isChunkLoadError(new TypeError('Failed to fetch'))).toBe(true);
    expect(isChunkLoadError({ name: 'ChunkLoadError' })).toBe(true);
    expect(isChunkLoadError('Loading chunk 3 failed')).toBe(true);
  });
  it('any other error is not one', () => {
    expect(isChunkLoadError(new Error('Cannot read properties of undefined'))).toBe(false);
    expect(isChunkLoadError(null)).toBe(false);
    expect(isChunkLoadError(undefined)).toBe(false);
    expect(isChunkLoadError(42)).toBe(false);
  });
});
