/** Client of crop-health's work list (a sibling module): absent or unreachable means "not installed". */
export const API_ORIGIN: string = (
  (typeof window !== 'undefined' ? (window as any).__ENV__?.VITE_API_URL : undefined) ?? (import.meta as any).env?.VITE_API_URL ?? ''
).replace(/\/$/, '');

export interface WorklistRow {
  parcelId: string;
  parcelName?: string;
  nextJob: { kind: string; when: string | null; detail: Record<string, unknown> } | null;
  surprise: { code: string; detail: Record<string, unknown> } | null;
  cropStatus: string;
}

/** The rows, or null on 403/404, any other failure or a network error: the block is then hidden. */
export async function fetchWorklist(signal?: AbortSignal): Promise<WorklistRow[] | null> {
  try {
    const res = await fetch(`${API_ORIGIN}/api/crop-health/parcels`, { credentials: 'include', signal });
    if (!res.ok) return null;
    const body: unknown = await res.json();
    return Array.isArray(body) ? (body as WorklistRow[]) : null;
  } catch {
    return null;
  }
}
