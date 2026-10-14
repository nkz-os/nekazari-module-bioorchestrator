import { describe, expect, it } from 'vitest';
import { closeParams, forecastHref, openParcelParams, openSimulationParams, readSimulationPrefill, readView } from './urlState';

const P = (q: string) => new URLSearchParams(q);

describe('planning URL state', () => {
  it('opening a parcel sets parcel and campaign, drops the view, keeps unrelated params and does not mutate the input', () => {
    const current = P('view=simulation&x=1');
    const next = openParcelParams(current, 'urn:ngsi-ld:AgriParcel:t:p1', 2027);
    expect(next.get('parcel')).toBe('urn:ngsi-ld:AgriParcel:t:p1');
    expect(next.get('campaign')).toBe('2027');
    expect(next.has('view')).toBe(false);
    expect(next.get('x')).toBe('1');
    expect(current.toString()).toBe('view=simulation&x=1');
  });
  it('opening the simulation sets view=simulation and the parcel, keeping the campaign', () => {
    const next = openSimulationParams(P('parcel=old&campaign=2026'), 'p2');
    expect(next.get('view')).toBe('simulation');
    expect(next.get('parcel')).toBe('p2');
    expect(next.get('campaign')).toBe('2026');
    expect(openSimulationParams(P(''), 'p2').toString()).toBe('view=simulation&parcel=p2');
  });
  it('closing removes parcel, campaign and view, and only those', () => {
    expect(closeParams(P('parcel=p1&campaign=2027&view=simulation&x=1')).toString()).toBe('x=1');
    expect(closeParams(P('')).toString()).toBe('');
  });
  it('reads the simulation only with a parcel, and never also as an open detail', () => {
    expect(readView(P('view=simulation&parcel=p1'))).toEqual({ parcel: null, simulationFor: 'p1' });
    expect(readView(P('parcel=p1&campaign=2027'))).toEqual({ parcel: 'p1', simulationFor: null });
    expect(readView(P('view=simulation'))).toEqual({ parcel: null, simulationFor: null });
    expect(readView(P('view=other&parcel=p1'))).toEqual({ parcel: 'p1', simulationFor: null });
    expect(readView(P(''))).toEqual({ parcel: null, simulationFor: null });
    expect(readView(P('parcel='))).toEqual({ parcel: null, simulationFor: null });
  });
  it('forecastHref opens the crop-health page on the parcel (its API takes the bare id)', () => {
    expect(forecastHref('urn:ngsi-ld:AgriParcel:d2e5-11')).toBe('/crop-health?parcel=d2e5-11');
    expect(forecastHref('d2e5-11')).toBe('/crop-health?parcel=d2e5-11');
  });

  it('reads a simulation link: crop, sowing date and irrigation', () => {
    expect(readSimulationPrefill(P('view=simulation&parcel=p1&crop_slug=TRZAX&sowing_date=2025-11-05&irrigation=full')))
      .toEqual({ cropSlug: 'TRZAX', sowingDate: '2025-11-05', irrigation: 'full' });
  });

  it('ignores a malformed date or irrigation instead of passing it on', () => {
    expect(readSimulationPrefill(P('crop_slug=%20&sowing_date=5-11-2025&irrigation=drip')))
      .toEqual({ cropSlug: undefined, sowingDate: undefined, irrigation: undefined });
    expect(readSimulationPrefill(P('sowing_date=2025-02-30')).sowingDate).toBeUndefined();
  });

  it('leaving the simulation drops its inputs, so they do not come back on the next one', () => {
    const link = P('view=simulation&parcel=p1&crop_slug=TRZAX&sowing_date=2025-11-05&irrigation=full&x=1');
    expect(closeParams(link).toString()).toBe('x=1');
    const detail = openParcelParams(link, 'p2', 2027);
    expect(['crop_slug', 'sowing_date', 'irrigation'].some((k) => detail.has(k))).toBe(false);
  });
});
