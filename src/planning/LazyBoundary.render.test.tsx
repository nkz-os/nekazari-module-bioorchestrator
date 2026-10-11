import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@nekazari/sdk', async () => {
  const { esT } = await import('./testI18n');
  return { useTranslation: () => ({ t: esT, i18n: { language: 'es' } }), i18n: {} };
});

import LazyBoundary from './LazyBoundary';

// No DOM environment here: the failed state is asserted through the class contract and static markup.
describe('LazyBoundary (es)', () => {
  it('renders its children while nothing failed', () => {
    const html = renderToStaticMarkup(<LazyBoundary><p>contenido</p></LazyBoundary>);
    expect(html).toContain('contenido');
    expect(html).not.toContain('Reintentar');
  });

  it('turns a render error into a failed state', () => {
    expect(LazyBoundary.getDerivedStateFromError(new Error('boom'))).toEqual({ failed: true, chunk: false });
  });

  it('shows the message and a retry button instead of the children once failed, and retry remounts them', () => {
    const boundary = new LazyBoundary({ children: <p>contenido</p> });
    boundary.state = { failed: true, attempt: 0, chunk: false };
    const html = renderToStaticMarkup(boundary.render() as React.ReactElement);
    expect(html).toContain('No se ha podido cargar');
    expect(html).toContain('Reintentar');
    expect(html).not.toContain('contenido');

    let next = boundary.state;
    boundary.setState = ((u: (s: typeof next) => typeof next) => { next = { ...next, ...u(boundary.state) }; }) as never;
    (boundary as unknown as { reset: () => void }).reset();
    expect(next).toEqual({ failed: false, attempt: 1, chunk: false });
  });

  it('tells a chunk that failed to load apart: it needs a page reload, not a remount', () => {
    expect(LazyBoundary.getDerivedStateFromError(new TypeError('Failed to fetch dynamically imported module: x.js'))).toEqual({ failed: true, chunk: true });
    const named = new Error('x');
    named.name = 'ChunkLoadError';
    expect(LazyBoundary.getDerivedStateFromError(named)).toEqual({ failed: true, chunk: true });
    const boundary = new LazyBoundary({ children: <p>contenido</p> });
    boundary.state = { failed: true, attempt: 0, chunk: true };
    const html = renderToStaticMarkup(boundary.render() as React.ReactElement);
    expect(html).toContain('Recargar la página');
    expect(html).not.toContain('Reintentar');
  });

  it('logs the error with the component stack', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const boundary = new LazyBoundary({ children: null });
    const error = new Error('chunk');
    boundary.componentDidCatch(error, { componentStack: '\n at X' } as React.ErrorInfo);
    expect(spy).toHaveBeenCalledWith('planning: lazy part failed', { where: 'planning', error, componentStack: '\n at X' });
    spy.mockRestore();
  });
});
