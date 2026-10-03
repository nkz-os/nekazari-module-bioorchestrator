export const EXPERT_STORAGE_KEY = 'bioorchestrator.expertMode';
export const EXPERT_ROLES = ['TechnicalConsultant', 'PlatformAdmin'] as const;

export interface ExpertModeInput {
  urlParam?: string | null;
  stored?: string | null;
  roles?: readonly string[] | null;
}

/** Precedence: explicit URL param > stored toggle > role default. */
export function resolveExpertMode({ urlParam, stored, roles }: ExpertModeInput): boolean {
  if (urlParam === '1' || urlParam === '0') return urlParam === '1';
  if (stored === '1' || stored === '0') return stored === '1';
  return (roles ?? []).some((r) => (EXPERT_ROLES as readonly string[]).includes(r));
}
