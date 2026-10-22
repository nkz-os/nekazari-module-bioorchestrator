import type { SimulationPrefill } from '../components/cropSimulationModel';

/** The planning page keeps the opened parcel, the campaign and the simulation view in the URL, so Back and reload behave. */
// The simulation inputs a link may carry (named as the crop-simulation API), dropped with the view.
const SIMULATION_KEYS = ['crop_slug', 'sowing_date', 'irrigation'] as const;
const KEYS = ['parcel', 'campaign', 'view', ...SIMULATION_KEYS] as const;

/** The detail of a parcel: its id and the campaign shown. Any simulation view is left. */
export function openParcelParams(current: URLSearchParams, parcelId: string, campaign: number): URLSearchParams {
  const next = new URLSearchParams(current);
  next.set('parcel', parcelId);
  next.set('campaign', String(campaign));
  next.delete('view');
  for (const key of SIMULATION_KEYS) next.delete(key);
  return next;
}

/** The campaign simulation of a parcel. */
export function openSimulationParams(current: URLSearchParams, parcelId: string): URLSearchParams {
  const next = new URLSearchParams(current);
  next.set('view', 'simulation');
  next.set('parcel', parcelId);
  return next;
}

/** Back to the matrix: the planning keys go, anything else in the query stays. */
export function closeParams(current: URLSearchParams): URLSearchParams {
  const next = new URLSearchParams(current);
  for (const key of KEYS) next.delete(key);
  return next;
}

/** What the URL asks to show: the simulation of a parcel, or the detail of a parcel, never both. */
export function readView(params: URLSearchParams): { parcel: string | null; simulationFor: string | null } {
  const parcel = params.get('parcel') || null;
  if (parcel && params.get('view') === 'simulation') return { parcel: null, simulationFor: parcel };
  return { parcel, simulationFor: null };
}

/**
 * Crop-health's page on the parcel, where its yield tab shows the in-season forecast.
 * Not the viewer: /viewer redirects to /entities and drops the query. Crop-health's API takes the bare id.
 */
export function forecastHref(parcelId: string): string {
  return `/crop-health?parcel=${encodeURIComponent(parcelId.replace(/^urn:ngsi-ld:AgriParcel:/, ''))}`;
}

/** A real calendar day as YYYY-MM-DD (2025-02-30 is not one). */
function isoDay(v: string | null): string | undefined {
  if (!v || !/^\d{4}-\d{2}-\d{2}$/.test(v)) return undefined;
  const d = new Date(`${v}T00:00:00Z`);
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === v ? v : undefined;
}

/** The simulation inputs a link carries (e.g. from crop health); anything malformed is left out, not guessed. */
export function readSimulationPrefill(params: URLSearchParams): SimulationPrefill {
  const crop = params.get('crop_slug')?.trim();
  const irrigation = params.get('irrigation');
  return {
    cropSlug: crop || undefined,
    sowingDate: isoDay(params.get('sowing_date')),
    irrigation: irrigation === 'rainfed' || irrigation === 'full' ? irrigation : undefined,
  };
}
