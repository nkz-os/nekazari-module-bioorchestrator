import { describe, it, expect } from 'vitest';
import { centroidOfGeometry, resolveParcelContext } from './entityData';

describe('centroidOfGeometry', () => {
  it('returns lat/lon directly for a Point', () => {
    expect(centroidOfGeometry({ type: 'Point', coordinates: [-2.5, 42.1] }))
      .toEqual({ lat: 42.1, lon: -2.5 });
  });

  it('averages the outer ring vertices for a Polygon', () => {
    const geometry = {
      type: 'Polygon',
      coordinates: [[[0, 0], [0, 2], [2, 2], [2, 0]]],
    };
    expect(centroidOfGeometry(geometry)).toEqual({ lat: 1, lon: 1 });
  });

  it('returns null when geometry is undefined', () => {
    expect(centroidOfGeometry(undefined)).toBeNull();
  });

  it('returns null when coordinates are missing', () => {
    expect(centroidOfGeometry({ type: 'Polygon', coordinates: undefined })).toBeNull();
  });

  it('returns null for an unrecognized geometry type', () => {
    expect(centroidOfGeometry({ type: 'LineString', coordinates: [[0, 0], [1, 1]] })).toBeNull();
  });
});

describe('resolveParcelContext', () => {
  it('resolves id, name, cropType and a Polygon centroid', () => {
    const entityData = {
      id: 'urn:ngsi-ld:AgriParcel:t1:P1',
      name: 'Sevilla_2',
      cropType: 'Olea europaea',
      geometry: { type: 'Polygon', coordinates: [[[0, 0], [0, 2], [2, 2], [2, 0]]] },
    };
    expect(resolveParcelContext(entityData)).toEqual({
      parcelId: 'urn:ngsi-ld:AgriParcel:t1:P1',
      parcelName: 'Sevilla_2',
      cropType: 'Olea europaea',
      lat: 1,
      lon: 1,
    });
  });

  it('treats an empty-string cropType as absent (no crop assigned)', () => {
    const entityData = { id: 'P1', name: 'P1', cropType: '' };
    expect(resolveParcelContext(entityData).cropType).toBeUndefined();
  });

  it('returns an empty object when entityData is undefined', () => {
    expect(resolveParcelContext(undefined)).toEqual({});
  });

  it('omits lat/lon when geometry is absent', () => {
    const entityData = { id: 'P1', name: 'P1', cropType: 'Wheat' };
    const resolved = resolveParcelContext(entityData);
    expect(resolved.lat).toBeUndefined();
    expect(resolved.lon).toBeUndefined();
  });
});
