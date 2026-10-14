/** The planning page keeps the opened parcel, the campaign and the simulation view in the URL, so Back and reload behave. */
const KEYS = ['parcel', 'campaign', 'view'] as const;

/** The detail of a parcel: its id and the campaign shown. Any simulation view is left. */
export function openParcelParams(current: URLSearchParams, parcelId: string, campaign: number): URLSearchParams {
  const next = new URLSearchParams(current);
  next.set('parcel', parcelId);
  next.set('campaign', String(campaign));
  next.delete('view');
  return next;
}

/** The temporary campaign simulation of a parcel. */
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

/** The viewer with the parcel selected: its crop-health panel shows the in-season forecast. */
export function forecastHref(parcelId: string): string {
  return `/viewer?selectedEntity=${encodeURIComponent(parcelId)}`;
}
