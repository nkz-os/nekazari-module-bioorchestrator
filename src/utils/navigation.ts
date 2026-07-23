export type BioorchestratorTool = 'cropPlanner' | 'varietyFinder';

const VALID_TOOLS: readonly BioorchestratorTool[] = ['cropPlanner', 'varietyFinder'];

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

/** Reads `?tool=` from the module's own URL on initial mount, so a deep
 * link can land the user directly on a tool instead of the Dashboard. */
export function resolveToolFromSearchParams(
  searchParams: URLSearchParams,
): BioorchestratorTool | null {
  const tool = searchParams.get('tool');
  return (VALID_TOOLS as string[]).includes(tool ?? '') ? (tool as BioorchestratorTool) : null;
}
