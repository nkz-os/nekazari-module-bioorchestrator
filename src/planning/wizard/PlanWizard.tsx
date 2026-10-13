// src/planning/wizard/PlanWizard.tsx
import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Inline, Stack } from '@nekazari/ui-kit';
import { fetchCalendar, fetchEstimates, fetchVarieties, PlanningApiError, putPlan, saveParcelRegime } from '../api';
import DirectSowingOption from '../DirectSowingOption';
import { loadErrorKey } from '../errors';
import { aquacropPending, mergeAquacrop, yieldSummary } from '../estimatesModel';
import EstimatesTable from '../EstimatesTable';
import { cropName } from '../format';
import { campaignFortnights, formatRange, middleDay, type Fortnight } from '../fortnight';
import LoadingLine from '../LoadingLine';
import { DEFAULT_PERENNIAL_YEARS } from '../planModel';
import Step from '../Step';
import {
  CONTRACT_VERSION, type CalendarResponse, type FitResponse, type Management, type ParcelRow, type Regime, type SpeciesFit,
} from '../types';
import { useAquacropEstimate } from '../useAquacropEstimate';
import { useLoad, type LoadView } from '../useLoad';
import {
  applyCalendar, back, calendarMainParam, canAdvance, initialState, next, pickCrop, prefillFromCells, regimeToSave, savePlan, setSowing,
  stepsOf, toPlanIn, type WizardState,
} from '../wizardModel';
import StepCover from './StepCover';
import StepCrop from './StepCrop';
import StepSowing from './StepSowing';
import StepVariety from './StepVariety';
import WizardSummary from './WizardSummary';

interface Props {
  row: ParcelRow; campaign: number; regime: Regime; management: Management; onSaved: () => void;
  /** The parcel's fit, loaded once by the detail (it also shows the parcel facts). */
  fit: LoadView<FitResponse> & { retry: () => void };
}

/** The calendar sends no species for a cover: its rules do not depend on it (and free text would reload per keystroke). */
const ANY_COVER = 'cover';

/** Every fortnight of the campaign, nothing chosen: what the strips offer when the calendar cannot be read (spec §5). */
function fallbackCalendar(parcelId: string, campaign: number): CalendarResponse {
  const fortnights = campaignFortnights(campaign);
  return {
    contract_version: CONTRACT_VERSION, parcel_id: parcelId, campaign, crop: null, cover_crop: null, fortnights,
    main: { window: null, window_source: null, default: null, selected: null, options: fortnights, perennial_years_default: null },
    previous: { crop: null, end: null, end_source: 'campaign_start' }, cover: null,
  };
}

/** The guided plan of one parcel and campaign (spec W5): one question per screen, then a summary with Guardar. */
export default function PlanWizard({ row, campaign, regime, management, onSaved, fit }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const cells = row.cells[String(campaign)] ?? [];
  const replacing = cells.some((c) => c.provenance === 'planned');
  const [s, setS] = useState<WizardState>(() => initialState(prefillFromCells(cells, campaign)));
  const [directSowing, setDirectSowing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [regimeNotice, setRegimeNotice] = useState(false);
  // The main fortnight the calendar proposed: asking again with it would return the same answer, so it is not sent.
  const [proposedMain, setProposedMain] = useState<Fortnight | null>(null);

  const chosen = fit.data?.species.find((x) => x.species === s.crop) ?? null;
  // Covers are annual: a perennial is never offered as one.
  const covers = (fit.data?.species ?? []).filter((x) => x.verdict !== 'no_fit' && x.lifecycle !== 'perennial');
  const listedCovers = covers.map((x) => x.species);

  const varieties = useLoad((sig) => fetchVarieties(row.parcel_id, s.crop ?? '', regime, management, sig),
    [row.parcel_id, s.crop, regime, management], Boolean(s.crop && s.hasTrials));
  const mainParam = calendarMainParam(s, proposedMain);
  const cal = useLoad((sig) => fetchCalendar(row.parcel_id, { campaign, crop: s.crop ?? '', cover: ANY_COVER, mainSowing: mainParam }, sig),
    [row.parcel_id, campaign, s.crop, mainParam], Boolean(s.crop));
  const calendar = cal.data ?? (cal.error != null ? fallbackCalendar(row.parcel_id, campaign) : null);

  const atSummary = s.step === 'summary';
  const sowingDate = s.sowing ? middleDay(s.sowing) : null;
  const estimates = useLoad(
    (sig) => fetchEstimates(row.parcel_id, { crop: s.crop ?? '', variety: s.variety, regime, management, sowingDate }, sig),
    [row.parcel_id, s.crop, s.variety, regime, management, sowingDate], Boolean(atSummary && s.crop && sowingDate));
  const aquacrop = useAquacropEstimate(row.parcel_id,
    { crop: s.crop ?? '', regime, sowingDate, sowingMethod: directSowing ? 'direct' : null },
    Boolean(atSummary && s.crop && estimates.data && aquacropPending(estimates.data)));
  const merged = estimates.data ? mergeAquacrop(estimates.data, aquacrop.response, aquacrop.failure ?? undefined) : null;

  // A pre-filled crop learns from the fit whether it has variety trials.
  const trials = chosen?.has_variety_trials;
  useEffect(() => {
    if (trials !== undefined) setS((x) => (x.hasTrials === trials ? x : { ...x, hasTrials: trials }));
  }, [trials]);
  // A variety belongs to one regime and management: a loaded list without it drops it.
  const loadedVarieties = varieties.data?.varieties;
  useEffect(() => {
    if (!loadedVarieties) return;
    setS((x) => (x.variety !== null && !loadedVarieties.some((v) => v.variety === x.variety) ? { ...x, variety: null } : x));
  }, [loadedVarieties]);
  // The calendar's answer proposes the dates: the main default when none is chosen, and a cover sowing that fits.
  const calData = cal.data;
  useEffect(() => {
    if (!calData) return;
    setProposedMain(calData.main.default);
    setS((x) => applyCalendar(x, calData));
  }, [calData]);

  const steps = stepsOf(s);
  const n = steps.indexOf(s.step) + 1;
  const cropLabel = (slug: string | null) => {
    const sp = slug ? fit.data?.species.find((x) => x.species === slug) : undefined;
    return sp ? cropName(sp.names, sp.species, lang) : (slug ?? '');
  };

  const pick = (sp: SpeciesFit) => {
    if (sp.species === s.crop) return; // choosing the crop already chosen keeps everything
    setS((x) => pickCrop(x, sp.species, sp.has_variety_trials, sp.lifecycle === 'perennial', DEFAULT_PERENNIAL_YEARS));
    setProposedMain(null);
    setDirectSowing(false);
  };

  const save = async () => {
    setSaving(true);
    setError(null);
    try {
      // W10: the regime of the plan is remembered on a parcel that has none of its own.
      const toParcel = regimeToSave(row, regime);
      const outcome = await savePlan(() => putPlan(row.parcel_id, toPlanIn(s, campaign, regime, management)),
        toParcel ? () => saveParcelRegime(row.parcel_id, toParcel) : null);
      if (outcome === 'regime_not_saved') {
        console.warn('planning: plan saved, parcel regime not saved', { parcel: row.parcel_id, campaign, regime });
        setRegimeNotice(true);
        return;
      }
      onSaved();
    } catch (e) {
      const code = e instanceof PlanningApiError && e.code ? e.code : 'generic';
      setError(t(`planning.plan.error.${code}`, { campaign, defaultValue: t('planning.plan.error.generic') }));
    } finally {
      setSaving(false);
    }
  };

  const summaryLines = (): string[] => {
    const lines = [`${cropLabel(s.crop)} · ${s.variety ?? t('planning.wizard.variety.none')}`];
    if (s.sowing) lines.push(t('planning.wizard.sowing.chosen', { crop: cropLabel(s.crop), range: formatRange(s.sowing, lang) }));
    if (s.perennial && s.years !== null) lines.push(`${t('planning.plan.years')}: ${s.years}`);
    const ends = cal.data?.cover?.ends ?? s.sowing?.start;
    if (s.coverOn && s.coverSowing && ends) {
      lines.push(`${t('planning.role.cover_crop')}: ${cropLabel(s.coverSpecies.trim())} · ${formatRange({ start: s.coverSowing.start, end: ends }, lang)}`);
    }
    return lines;
  };

  const calendarNotice = cal.error != null && (
    <Inline gap="inline" align="center" wrap>
      <p className="text-nkz-sm text-nkz-danger">{t(loadErrorKey(cal.error))}</p>
      <Button size="sm" variant="secondary" onClick={cal.retry}>{t('planning.retry')}</Button>
    </Inline>
  );

  const body = (() => {
    switch (s.step) {
      case 'crop':
        return (
          <Step title={t('planning.wizard.step', { n, total: steps.length })} loading={fit.loading} error={fit.error}
            onRetry={fit.retry} loadingText={t('planning.loading.fit')}>
            {fit.data && <StepCrop fit={fit.data} selected={s.crop} onPick={pick} />}
          </Step>
        );
      case 'variety':
        return (
          <Step title={t('planning.wizard.step', { n, total: steps.length })} loading={varieties.loading} error={varieties.error}
            onRetry={varieties.retry} loadingText={t('planning.loading.varieties')}>
            {varieties.data && <StepVariety data={varieties.data} selected={s.variety}
              onSelect={(v) => setS((x) => ({ ...x, variety: v }))} />}
          </Step>
        );
      case 'sowing':
      case 'cover':
        return (
          <Step title={t('planning.wizard.step', { n, total: steps.length })}>
            {cal.loading && <LoadingLine text={t('planning.loading.generic')} />}
            {calendarNotice}
            {calendar && !cal.loading && (s.step === 'sowing'
              ? <StepSowing cal={calendar} cropName={cropLabel(s.crop)} selected={s.sowing} years={s.years} perennial={s.perennial}
                  onSowing={(f) => setS((x) => setSowing(x, f, cal.data?.cover?.options ?? []))}
                  onYears={(y) => setS((x) => ({ ...x, years: y }))} />
              : <StepCover cal={calendar} on={s.coverOn} species={s.coverSpecies} sowing={s.coverSowing} method={s.coverMethod}
                  coverOptions={covers}
                  onOn={(on) => setS((x) => ({ ...x, coverOn: on, coverSowing: x.coverSowing ?? cal.data?.cover?.default ?? null }))}
                  other={s.coverOther} onOther={(o) => setS((x) => ({ ...x, coverOther: o }))}
                  onSpecies={(v) => setS((x) => ({ ...x, coverSpecies: v }))}
                  onSowing={(f) => setS((x) => ({ ...x, coverSowing: f }))}
                  onMethod={(m) => setS((x) => ({ ...x, coverMethod: m }))} />)}
          </Step>
        );
      default:
        return (
          <Step title={t('planning.wizard.step', { n, total: steps.length })}>
            <WizardSummary lines={summaryLines()} yieldLine={estimates.data ? yieldSummary(merged?.data.estimates ?? estimates.data.estimates) : null}
              detail={(
                <Stack gap="stack">
                  {estimates.loading && <LoadingLine text={t('planning.loading.estimates')} />}
                  {estimates.error != null && (
                    <Inline gap="inline" align="center" wrap>
                      <p className="text-nkz-sm text-nkz-danger">{t(loadErrorKey(estimates.error))}</p>
                      <Button size="sm" variant="secondary" onClick={estimates.retry}>{t('planning.retry')}</Button>
                    </Inline>
                  )}
                  {estimates.data && aquacropPending(estimates.data) && (
                    <DirectSowingOption checked={directSowing} onChange={setDirectSowing}
                      fromOperations={aquacrop.response?.sowing_method === 'direct'
                        && aquacrop.response.sowing_method_source === 'field_operations'}
                      notApplied={aquacrop.response?.state === 'done' && aquacrop.response.sowing_method !== 'direct'} />
                  )}
                  {merged && <EstimatesTable data={merged.data} progress={merged.progress} onRetry={aquacrop.retry} />}
                </Stack>
              )}
              replacing={replacing} saving={saving} error={error} regimeNotice={regimeNotice} onSave={save} onContinue={onSaved} />
          </Step>
        );
    }
  })();

  return (
    <Stack gap="stack">
      {body}
      {!atSummary && (
        <Inline gap="inline" align="center">
          {n > 1 && <Button variant="secondary" onClick={() => setS(back)}>{t('planning.wizard.back')}</Button>}
          <Button variant="primary" disabled={!canAdvance(s, listedCovers)} onClick={() => setS(next)}>{t('planning.wizard.next')}</Button>
        </Inline>
      )}
      {atSummary && <div><Button variant="secondary" onClick={() => setS(back)}>{t('planning.wizard.back')}</Button></div>}
    </Stack>
  );
}
