/** Shape of the relevant part of BioOrchestrator's own
 * GET /agriculture/crop-context response (see services/api.ts,
 * CropContextResponse) — resolves AgriParcel.hasAgriCrop directly,
 * unlike the host's legacy free-text `cropType` field. */
export interface CropContextLike {
  crop?: {
    name?: string;
    scientific_name?: string | null;
    eppo?: string;
  };
}

/** Picks the best available display value for the assigned crop. */
export function resolveCropTypeFromContext(
  ctx: CropContextLike | null | undefined,
): string | null {
  if (!ctx?.crop) return null;
  return ctx.crop.name || ctx.crop.scientific_name || ctx.crop.eppo || null;
}
