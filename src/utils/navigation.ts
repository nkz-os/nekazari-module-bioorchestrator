import type { CropContextLike } from './cropContext';

export type BioorchestratorTool = 'whatToSow' | 'cropPlanner' | 'varietyFinder';

export type Door = 'whatToSow' | 'campaign' | 'library';

export const DOORS: readonly Door[] = ['whatToSow', 'campaign', 'library'];

/** Tools listed under the "Mi campaña" door (TOOL_MAP ids). */
export const CAMPAIGN_TOOL_IDS = [
  'cropManagement',
  'parcelStatus',
  'yieldProjection',
  'waterBudget',
  'cropSimulation',
  'simulateScenario',
] as const;

/** Tools listed under the "Biblioteca" door (TOOL_MAP ids). */
export const LIBRARY_TOOL_IDS = [
  'regenerative',
  'catalog',
  'climate',
  'phenology',
  'thermal',
  'npk',
  'soil',
  'rotation',
  'organic',
  'pipeline',
  'sources',
  'dadis',
  'speciesExplorer',
] as const;

/** Old tools replaced by the "¿Qué siembro?" page; deep links to them land there. */
export const WHAT_TO_SOW_REDIRECT_IDS = [
  'whatToSow',
  'cropPlanner',
  'varietyFinder',
  'comparator',
  'rotationPlanner',
] as const;

export interface DoorTarget {
  door: Door;
  tool?: string;
}

/** Builds a deep-link into this module's own routes, carrying an optional
 * parcel selection and which tool to land on. Used by every CTA elsewhere
 * in the module (and in other modules' slot widgets) that wants to send a
 * user straight into a specific BioOrchestrator tool for a given parcel. */
export function buildBioorchestratorToolUrl(
  parcelId: string | undefined,
  tool: BioorchestratorTool,
): string {
  const params = new URLSearchParams({ tool });
  if (parcelId) {
    params.set('parcel', parcelId);
  }
  return `/bioorchestrator?${params.toString()}`;
}

const includes = (list: readonly string[], v: string | null): v is string =>
  v !== null && list.includes(v);

/** Reads the landing door from the module URL. `?tool=` wins over `?door=`;
 * retired tools redirect to whatToSow. Returns null when the URL does not
 * decide (the caller picks the default door). */
export function resolveDoor(searchParams: URLSearchParams): DoorTarget | null {
  const tool = searchParams.get('tool');
  if (includes(WHAT_TO_SOW_REDIRECT_IDS, tool)) return { door: 'whatToSow' };
  if (includes(CAMPAIGN_TOOL_IDS, tool)) return { door: 'campaign', tool };
  if (includes(LIBRARY_TOOL_IDS, tool)) return { door: 'library', tool };

  const door = searchParams.get('door');
  if (includes(DOORS, door)) return { door: door as Door };
  return null;
}

/** True when crop-context reports a real assigned crop (not absent / "unknown"). */
export function hasAssignedCrop(ctx: CropContextLike | null | undefined): boolean {
  const eppo = ctx?.crop?.eppo;
  return Boolean(eppo && eppo !== 'unknown');
}

/** Default door when the URL does not decide. */
export function pickDefaultDoor(input: { hasParcel: boolean; hasAssignedCrop: boolean }): Door {
  return input.hasParcel && input.hasAssignedCrop ? 'campaign' : 'whatToSow';
}
