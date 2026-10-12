import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { renderToStaticMarkup } from 'react-dom/server';
import es from '../locales/es.json';

vi.mock('@nekazari/sdk', () => {
  const lookup = (key: string, opts?: Record<string, unknown>) => {
    const v = key.split('.').reduce<unknown>((n, p) => (n && typeof n === 'object' ? (n as Record<string, unknown>)[p] : undefined), es);
    if (typeof v !== 'string') return key;
    return v.replace(/\{\{(\w+)\}\}/g, (_m, k: string) => String(opts?.[k] ?? ''));
  };
  return { useTranslation: () => ({ t: lookup, i18n: { language: 'es' } }), i18n: {} };
});

import DirectSowingOption from './DirectSowingOption';

const textOf = (html: string) => html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');

describe('DirectSowingOption (es)', () => {
  it('offers direct sowing and says what it simulates', () => {
    const t = textOf(renderToStaticMarkup(<DirectSowingOption checked={false} onChange={() => {}} fromOperations={false} notApplied={false} />));
    expect(t).toContain('Siembra directa sobre el rastrojo');
    expect(t).toContain('cubre el 30 % del suelo');
    expect(t).not.toContain('Operaciones de campo');
  });
  it('says when the recorded sowing set direct sowing', () => {
    const t = textOf(renderToStaticMarkup(<DirectSowingOption checked={false} onChange={() => {}} fromOperations notApplied={false} />));
    expect(t).toContain('así consta la siembra registrada en Operaciones de campo');
  });
  it('says when a ticked box was not applied (a server without the option)', () => {
    const t = textOf(renderToStaticMarkup(<DirectSowingOption checked onChange={() => {}} fromOperations={false} notApplied />));
    expect(t).toContain('no ha aplicado la siembra directa');
  });
  it('a ticked box needs no note: the request decides', () => {
    const t = textOf(renderToStaticMarkup(<DirectSowingOption checked onChange={() => {}} fromOperations notApplied={false} />));
    expect(t).not.toContain('Operaciones de campo');
  });
});
