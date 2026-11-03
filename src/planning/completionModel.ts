import { EPPO_PATTERN, PlanningApiError } from './api';
import { regimeToSave } from './wizardModel';
import type { FitResponse, ParcelRow, Reason, SpeciesFit } from './types';

/** The data the farmer can still complete from the planning wizard: a control for each, none when it is known. */
export interface Completion { regime: boolean; soil: boolean; history: number[] }

const mainCrops = (species: SpeciesFit[]): SpeciesFit[] => species.filter((s) => !s.cover_only);
const someMain = (fit: FitResponse, pick: (s: SpeciesFit) => boolean) => mainCrops(fit.species).some(pick);

export function completionNeeds(fit: FitResponse, row: Pick<ParcelRow, 'regime' | 'regime_source'>): Completion {
  const has = (code: string, ok: (why: unknown) => boolean = () => true) =>
    someMain(fit, (s) => s.reasons.some((r) => r.code === code && ok(r.params?.why)));
  return {
    // A parcel with a regime of its own is never asked again (the rule that decides what the plan saves).
    regime: has('regime_missing') && regimeToSave(row, 'rainfed') !== null,
    // A crop with no tolerance data says nothing about the parcel's soil.
    soil: has('soil_unknown', (why) => why !== 'no_tolerance'),
    history: fit.facts?.history_missing_campaigns ?? [],
  };
}

/** A reason the bar already asks about: a card does not repeat it. */
export const askedByBar = (needs: Completion) => (r: Reason): boolean =>
  (needs.regime && r.code === 'regime_missing') || (needs.soil && r.code === 'soil_unknown' && r.params?.why !== 'no_tolerance');

export const anyCompletion = (c: Completion): boolean => c.regime || c.soil || c.history.length > 0;

/** Species the farmer can name as a past crop: a main crop with a valid EPPO code, once each. */
export function declarableCrops(species: SpeciesFit[]): SpeciesFit[] {
  const seen = new Set<string>();
  return mainCrops(species).filter((s) => {
    if (!s.eppo || !EPPO_PATTERN.test(s.eppo) || seen.has(s.eppo)) return false;
    seen.add(s.eppo);
    return true;
  });
}

/** i18n key (under planning.wizard.crop.complete.error) of a refused write. */
export function writeErrorKey(kind: WriteKind, status: number | null, code: string | null): string {
  if (kind === 'soil' && status === 404) return 'soilNotReady';
  if (kind === 'history') {
    if (code === 'campaign_not_past' || code === 'invalid_campaign' || code === 'invalid_species' || code === 'campaign_has_dated_crop') return code;
  }
  return 'generic';
}

export type WriteKind = 'regime' | 'soil' | 'history';
export type WriteOutcome = { ok: true } | { ok: false; errorKey: string };

/** Runs one write; a refusal or a network error becomes the key of its message, never a throw. */
export async function attemptWrite(kind: WriteKind, write: () => Promise<unknown>): Promise<WriteOutcome> {
  try {
    await write();
    return { ok: true };
  } catch (e) {
    const known = e instanceof PlanningApiError;
    return { ok: false, errorKey: writeErrorKey(kind, known ? e.status : null, known ? e.code : null) };
  }
}
