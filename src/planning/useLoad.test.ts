import { describe, expect, it } from 'vitest';
import { deriveLoadState } from './useLoad';

const settled = { key: '["p1",2027,0,true]', data: { v: 1 }, error: null, loading: false };

describe('deriveLoadState', () => {
  it('same key: the state is returned as it is', () => {
    expect(deriveLoadState(settled, '["p1",2027,0,true]', true)).toEqual({ data: { v: 1 }, error: null, loading: false });
  });
  it('new key while enabled: loading, and the previous data is not returned', () => {
    expect(deriveLoadState(settled, '["p2",2027,0,true]', true)).toEqual({ data: null, error: null, loading: true });
  });
  it('new key while disabled: not loading, and the previous data is not returned', () => {
    expect(deriveLoadState(settled, '["p2",2027,0,false]', false)).toEqual({ data: null, error: null, loading: false });
  });
  it('an error of the previous key is not returned for the new key', () => {
    const failed = { key: '["p1",2027,0,true]', data: null, error: new Error('boom'), loading: false };
    expect(deriveLoadState(failed, '["p2",2027,0,true]', true)).toEqual({ data: null, error: null, loading: true });
  });
});
