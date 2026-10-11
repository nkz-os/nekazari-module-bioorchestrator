/** Test-only lookup of the real Spanish strings (mirrors i18next interpolation and plurals). */
import es from '../locales/es.json';

export function esT(key: string, opts?: Record<string, unknown>): string {
  const dict = es as unknown as Record<string, unknown>;
  const get = (k: string) => k.split('.').reduce<unknown>((n, p) => (n && typeof n === 'object' ? (n as Record<string, unknown>)[p] : undefined), dict);
  let v = get(key);
  if (typeof v !== 'string' && opts && typeof opts.count === 'number') v = get(key + (opts.count === 1 ? '_one' : '_other'));
  if (typeof v !== 'string') return (opts?.defaultValue as string) ?? key;
  return v.replace(/\{\{(\w+)\}\}/g, (_m, k: string) => String(opts?.[k] ?? ''));
}
