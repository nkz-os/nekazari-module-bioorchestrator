/** Shape of the `entityData` prop the host passes to every `context-panel`
 * slot widget (`additionalProps={{ entityData: selectedEntityForMap }}` in
 * UnifiedViewer.tsx) — a plain, already-unwrapped object (host's own
 * `Parcel` type, not raw NGSI-LD), not individual flat props. */
export interface ParcelEntityData {
  id?: string;
  name?: string;
  cropType?: string;
  geometry?: { type: string; coordinates: unknown };
}

export interface ResolvedParcelContext {
  parcelId?: string;
  parcelName?: string;
  cropType?: string;
  lat?: number;
  lon?: number;
}

/** Approximate centroid (simple vertex average, not area-weighted) — good
 * enough for parcel-scale API calls (terrain, climate, weather), not for
 * cartographic precision. */
export function centroidOfGeometry(
  geometry: { type: string; coordinates: unknown } | undefined,
): { lat: number; lon: number } | null {
  if (!geometry?.coordinates) return null;

  if (geometry.type === 'Point') {
    const coords = geometry.coordinates as [number, number];
    if (!Array.isArray(coords) || coords.length !== 2) return null;
    const [lon, lat] = coords;
    return { lat, lon };
  }

  if (geometry.type === 'Polygon') {
    const rings = geometry.coordinates as number[][][];
    const ring = rings?.[0];
    if (!ring || ring.length === 0) return null;
    const sum = ring.reduce(
      (acc, point) => ({ lon: acc.lon + point[0], lat: acc.lat + point[1] }),
      { lon: 0, lat: 0 },
    );
    return { lat: sum.lat / ring.length, lon: sum.lon / ring.length };
  }

  return null;
}

/** Derives the flat parcel fields this module's context-panel widgets need
 * from the host's `entityData` prop. */
export function resolveParcelContext(
  entityData: ParcelEntityData | undefined,
): ResolvedParcelContext {
  if (!entityData) return {};
  const centroid = centroidOfGeometry(entityData.geometry);
  return {
    parcelId: entityData.id,
    parcelName: entityData.name,
    cropType: entityData.cropType || undefined,
    lat: centroid?.lat,
    lon: centroid?.lon,
  };
}
