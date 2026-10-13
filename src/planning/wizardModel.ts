import { fortnightOf, type Fortnight } from './fortnight';
import { DEFAULT_PERENNIAL_YEARS, MAX_PERENNIAL_YEARS, RAW_SPECIES_NAME } from './planModel';
import type { CycleCell, Management, PlanCycleIn, PlanIn, Regime, TerminationMethod } from './types';

/** The wizard's steps, in order (W5). `variety` only exists for a crop with variety trials. */
export type WizardStep = 'crop' | 'variety' | 'sowing' | 'cover' | 'summary';

export interface WizardState {
  step: WizardStep; crop: string | null; hasTrials: boolean; perennial: boolean;
  variety: string | null; sowing: Fortnight | null; years: number | null;
  coverOn: boolean; coverSpecies: string; coverSowing: Fortnight | null; coverMethod: TerminationMethod;
  /** "Otra especie" chosen for the cover: its name is typed. */
  coverOther: boolean;
}

export function initialState(prefill?: Partial<WizardState>): WizardState {
  return { step: 'crop', crop: null, hasTrials: false, perennial: false, variety: null, sowing: null, years: null,
    coverOn: false, coverSpecies: '', coverSowing: null, coverMethod: 'roller_crimper', coverOther: false, ...prefill };
}

/**
 * "Cambiar": the campaign's planned cycles pre-fill the choices (only planned ones: actual and declared
 * cycles are facts, not a plan). Whether the crop has trials is learnt when the fit loads.
 */
export function prefillFromCells(cells: CycleCell[], campaign: number): Partial<WizardState> {
  const planned = cells.filter((c) => c.provenance === 'planned');
  const main = planned.find((c) => c.role === 'main_crop');
  if (!main?.species) return {};
  const cover = planned.find((c) => c.role === 'cover_crop' || c.role === 'catch_crop');
  const perennial = main.lifecycle === 'perennial';
  return {
    crop: main.species, perennial, variety: main.variety ?? null,
    sowing: main.start ? fortnightOf(main.start) : null,
    // A perennial's years are read back from its end: the last campaign it stands in.
    years: !perennial ? null : main.end ? Number(main.end.slice(0, 4)) - campaign + 1 : DEFAULT_PERENNIAL_YEARS,
    coverOn: Boolean(cover), coverSpecies: cover ? (cover.species ?? cover.species_raw ?? '') : '',
    coverSowing: cover?.start ? fortnightOf(cover.start) : null,
  };
}

export const stepsOf = (s: WizardState): WizardStep[] =>
  (['crop', 'variety', 'sowing', 'cover', 'summary'] as WizardStep[]).filter((x) => x !== 'variety' || s.hasTrials);

/** Choosing a crop resets everything that depends on it. */
export function pickCrop(s: WizardState, crop: string, hasTrials: boolean, perennial: boolean, years: number | null): WizardState {
  if (s.crop === crop) return s;
  return { ...s, crop, hasTrials, perennial, years: perennial ? years : null, variety: null, sowing: null,
    coverOn: false, coverSpecies: '', coverSowing: null, coverOther: false };
}

/** A new main sowing; a cover sowing that would no longer start before it falls back to the first option. */
export function setSowing(s: WizardState, sowing: Fortnight, coverOptions: Fortnight[]): WizardState {
  const keep = s.coverSowing && s.coverSowing.start < sowing.start;
  return { ...s, sowing, coverSowing: keep ? s.coverSowing : (coverOptions[0] ?? null) };
}

export function canAdvance(s: WizardState, coverListed: string[]): boolean {
  switch (s.step) {
    case 'crop': return s.crop !== null;
    case 'variety': return true; // optional (W4)
    case 'sowing':
      return s.sowing !== null && (!s.perennial || (s.years !== null && Number.isInteger(s.years)
        && s.years >= 1 && s.years <= MAX_PERENNIAL_YEARS));
    case 'cover':
      return !s.coverOn || (s.coverSowing !== null && s.coverSpecies.trim() !== ''
        && (coverListed.includes(s.coverSpecies) || RAW_SPECIES_NAME.test(s.coverSpecies.trim())));
    default: return true;
  }
}

export function next(s: WizardState): WizardState {
  const steps = stepsOf(s);
  const i = steps.indexOf(s.step);
  return i < steps.length - 1 ? { ...s, step: steps[i + 1] } : s;
}

export function back(s: WizardState): WizardState {
  const steps = stepsOf(s);
  const i = steps.indexOf(s.step);
  return i > 0 ? { ...s, step: steps[i - 1] } : s;
}

/** The plan body: fortnight windows; ends derived by the server (2026-10-12 rules). */
export function toPlanIn(s: WizardState, campaign: number, regime: Regime, management: Management): PlanIn {
  const main: PlanCycleIn = {
    role: 'main_crop', crop: s.crop ?? '', variety: s.variety, sowing_start: s.sowing?.start ?? '',
    sowing_end: s.sowing?.end ?? null, expected_termination: null, termination_method: null,
    ...(s.perennial ? { perennial_years: s.years } : {}),
  };
  const cycles: PlanCycleIn[] = s.coverOn && s.coverSowing
    ? [{ role: 'cover_crop', crop: s.coverSpecies.trim(), variety: null, sowing_start: s.coverSowing.start,
      sowing_end: s.coverSowing.end, expected_termination: null, termination_method: s.coverMethod }, main]
    : [main];
  return { campaign, regime, management, cycles };
}

/**
 * Save order (W10): the plan first; then, when the parcel had no regime, the one the farmer chose.
 * A failed plan throws and writes nothing else. A failed regime never rolls the plan back: it is
 * reported, and the regime is simply asked again next time.
 */
export async function savePlan(put: () => Promise<unknown>, saveRegime: (() => Promise<void>) | null): Promise<'saved' | 'regime_not_saved'> {
  await put();
  if (!saveRegime) return 'saved';
  try {
    await saveRegime();
    return 'saved';
  } catch {
    return 'regime_not_saved';
  }
}
