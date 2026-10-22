import { describe, expect, it } from 'vitest';
import { apiErrorFrom, ApiError } from './api';

const resp = (status: number, body: string) => new Response(body, { status });

describe('apiErrorFrom', () => {
  it('surfaces detail code and message', async () => {
    const e = await apiErrorFrom(resp(422, JSON.stringify({ detail: { code: 'weather_gaps', message: 'gap on 2026-01-02' } })));
    expect(e).toBeInstanceOf(ApiError);
    expect([e.status, e.code, e.message]).toEqual([422, 'weather_gaps', 'gap on 2026-01-02']);
  });
  it('accepts a string detail', async () => {
    const e = await apiErrorFrom(resp(400, JSON.stringify({ detail: 'bad' })));
    expect([e.code, e.message]).toEqual([null, 'bad']);
  });
  it('falls back to HTTP status on a non-JSON body', async () => {
    const e = await apiErrorFrom(resp(502, '<html>'));
    expect([e.status, e.code, e.message]).toEqual([502, null, 'HTTP 502']);
  });
});
