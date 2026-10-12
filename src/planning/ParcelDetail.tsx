// src/planning/ParcelDetail.tsx
import React, { useEffect, useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, FormField, Inline, Select, Stack } from '@nekazari/ui-kit';
import { ArrowLeft } from 'lucide-react';
import { fetchEstimates, fetchFit, fetchVarieties } from './api';
import DirectSowingOption from './DirectSowingOption';
import { aquacropPending, mergeAquacrop } from './estimatesModel';
import EstimatesTable from './EstimatesTable';
import FitList from './FitList';
import { cropName, safeLang, shortId } from './format';
import { lockMessage, lockReason, previousMain, simulationAvailable } from './matrixModel';
import PlanForm from './PlanForm';
import { CycleLine } from './PlanMatrix';
import { draftFor, draftFromCells, type PlanDraft } from './planModel';
import Step from './Step';
import type { Management, ParcelRow, Regime } from './types';
import { useAquacropEstimate } from './useAquacropEstimate';
import { useLoad } from './useLoad';
import VarietyTable from './VarietyTable';

interface Props {
  row: ParcelRow; campaign: number; onClose: () => void; onSaved: () => void; onSimulate?: () => void;
  /** Reloads the farm data: offered next to a lock that a failed history read causes. */
  onReload?: () => void;
}
const ESTIMATE_DEBOUNCE_MS = 600;
const EMPTY_DRAFT: PlanDraft = { main: { species: '', variety: null, sowing: '', years: null, method: null }, cover: null };

export default function ParcelDetail({ row, campaign, onClose, onSaved, onSimulate, onReload }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const [regime, setRegime] = useState<Regime | null>(row.regime);
  const [management, setManagement] = useState<Management>('conventional');
  const [directSowing, setDirectSowing] = useState(false);
  // "Change" starts from the plan the campaign already has, so saving never replaces it unseen.
  const targetCells = row.cells[String(campaign)] ?? [];
  const [prefill] = useState(() => draftFromCells(targetCells, campaign));
  const [species, setSpecies] = useState<string | null>(prefill?.main.species || null);
  const [draft, setDraft] = useState<PlanDraft>(prefill ?? EMPTY_DRAFT);
  // The date that feeds the estimates: it follows the sowing field after a pause, not on every keystroke.
  const [estimateSowing, setEstimateSowing] = useState(prefill?.main.sowing ?? '');
  useEffect(() => {
    const id = setTimeout(() => setEstimateSowing(draft.main.sowing), ESTIMATE_DEBOUNCE_MS);
    return () => clearTimeout(id);
  }, [draft.main.sowing]);

  // A variety belongs to one regime and management: changing either drops it, and so does a loaded
  // list that does not contain it, so the form never holds a variety the farmer cannot see.
  const clearVariety = () => setDraft((d) => (d.main.variety === null ? d : { ...d, main: { ...d.main, variety: null } }));
  const changeRegime = (r: Regime) => { setRegime(r); clearVariety(); };
  const changeManagement = (m: Management) => { setManagement(m); clearVariety(); };

  const fit = useLoad((s) => fetchFit(row.parcel_id, campaign, regime, s), [row.parcel_id, campaign, regime]);
  const chosen = fit.data?.species.find((x) => x.species === species) ?? null;
  const varieties = useLoad(
    (s) => fetchVarieties(row.parcel_id, chosen!.species, regime!, management, s),
    [row.parcel_id, chosen?.species, regime, management], Boolean(chosen?.has_variety_trials && regime));
  const estimates = useLoad(
    (s) => fetchEstimates(row.parcel_id, { crop: chosen!.species, variety: draft.main.variety, regime: regime!, management,
      sowingDate: estimateSowing || null }, s),
    [row.parcel_id, chosen?.species, draft.main.variety, regime, management, estimateSowing], Boolean(chosen && regime));
  // AquaCrop is asynchronous: /estimates only says it will answer ("async"); this polls its own endpoint.
  const aquacrop = useAquacropEstimate(
    row.parcel_id,
    { crop: chosen?.species ?? '', regime: regime ?? 'secano', sowingDate: estimateSowing || null,
      sowingMethod: directSowing ? 'direct' : null },
    Boolean(chosen && regime && estimates.data && aquacropPending(estimates.data)));
  const merged = estimates.data ? mergeAquacrop(estimates.data, aquacrop.response, aquacrop.failure ?? undefined) : null;
  const loadedVarieties = varieties.data?.varieties;
  useEffect(() => {
    if (!loadedVarieties) return;
    setDraft((d) => (d.main.variety !== null && !loadedVarieties.some((v) => v.variety === d.main.variety)
      ? { ...d, main: { ...d.main, variety: null } } : d));
  }, [loadedVarieties]);

  const pick = (slug: string) => {
    if (slug === species) return; // choosing the crop already chosen keeps the draft
    const sp = fit.data?.species.find((x) => x.species === slug);
    setSpecies(slug);
    if (sp) {
      const next = draftFor(sp, campaign);
      setDraft(next);
      setEstimateSowing(next.main.sowing); // a new crop estimates at once, without waiting for the pause
    }
  };
  const prev = previousMain(row, campaign);
  // A deep link can open a parcel the matrix would not let you plan; the evidence stays, the form does not.
  const locked = lockReason(row, campaign);
  const blocked = locked ? lockMessage(locked, campaign, t)
    : campaign < new Date().getFullYear() ? t('planning.plan.error.campaign_past', { campaign }) : null;
  const facts = fit.data?.facts;
  const soilTexture = facts?.soil_texture
    ? t(`planning.texture.${facts.soil_texture}`, { defaultValue: facts.soil_texture })
    : t('planning.detail.noData');
  const regimeOptions = (['secano', 'regadío'] as const).map((r) => ({ value: r, label: t(`planning.regime.${r}`) }));
  const managementOptions = (['conventional', 'organic'] as const).map((m) => ({ value: m, label: t(`planning.management.${m}`) }));
  // Cover crops are annual: a perennial is never offered as a cover.
  const candidates = (fit.data?.species ?? []).filter((s) => s.verdict !== 'no_fit' && s.lifecycle !== 'perennial');

  return (
    <Stack gap="section">
      <Inline gap="inline" align="center" wrap>
        <Button variant="ghost" onClick={onClose} leadingIcon={<ArrowLeft className="w-4 h-4" />}>{t('planning.detail.back')}</Button>
        <h2 className="text-nkz-lg font-semibold text-nkz-text-primary">{`${row.name ?? shortId(row.parcel_id)} · ${campaign}`}</h2>
        {prev && <span className="text-nkz-sm text-nkz-text-muted">{t('planning.detail.previous', { crop: cropName(prev.names, prev.species_raw, lang), campaign: campaign - 1 })}</span>}
        {onSimulate && simulationAvailable(row, campaign) && (
          <Button size="sm" variant="secondary" onClick={onSimulate}>{t('planning.simulation.open')}</Button>
        )}
      </Inline>

      {facts && (
        <Inline gap="inline" wrap align="center">
          <span className="text-nkz-sm text-nkz-text-muted">{t('planning.detail.known')}:</span>
          <Badge intent="default">{`${t('planning.detail.climate')}: ${facts.climate_class ?? t('planning.detail.noData')}`}</Badge>
          <Badge intent="default">{`${t('planning.detail.soil')}: ${soilTexture}${facts.soil_ph != null ? `, pH ${facts.soil_ph.toLocaleString(safeLang(lang))}` : ''}`}</Badge>
          {!facts.history_available && <Badge intent="warning">{t('planning.detail.historyDown')}</Badge>}
        </Inline>
      )}

      {targetCells.length > 0 && (
        <Stack gap="tight">
          <span className="text-nkz-sm text-nkz-text-muted">{`${t('planning.detail.currentPlan')}:`}</span>
          <div className="text-nkz-sm text-nkz-text-primary">
            {targetCells.map((cell, i) => <CycleLine key={cell.crop_id ?? `${campaign}-${i}`} cell={cell} />)}
          </div>
        </Stack>
      )}

      <Inline gap="inline" wrap align="end">
        <FormField label={t('planning.detail.regimeQuestion')} description={regime ? undefined : t('planning.detail.regimeHint')}>
          <Select value={regime ?? ''} placeholder={t('planning.detail.regimeQuestion')} options={regimeOptions}
            onValueChange={(v) => changeRegime(v as Regime)} error={!regime} />
        </FormField>
        <FormField label={t('planning.detail.managementLabel')}>
          <Select value={management} options={managementOptions} onValueChange={(v) => changeManagement(v as Management)} />
        </FormField>
      </Inline>

      <Step title={t('planning.step.crop')} loading={fit.loading} error={fit.error} onRetry={fit.retry} loadingText={t('planning.loading.fit')}>
        {fit.data && <FitList fit={fit.data} selected={species} onSelect={pick} />}
      </Step>

      {!chosen && fit.data && <p className="text-nkz-sm text-nkz-text-muted">{t('planning.fit.pick')}</p>}

      {chosen && regime && (
        <>
          <Step title={t('planning.step.variety')} loading={varieties.loading} error={varieties.error} onRetry={varieties.retry}
            loadingText={t('planning.loading.varieties')}>
            {chosen.has_variety_trials
              ? varieties.data && <VarietyTable data={varieties.data} selected={draft.main.variety}
                  onSelect={(v) => setDraft({ ...draft, main: { ...draft.main, variety: v } })} />
              : <p className="text-nkz-sm text-nkz-text-muted">{t('planning.variety.noTrials')}</p>}
          </Step>
          <Step title={t('planning.step.estimate')} loading={estimates.loading} error={estimates.error} onRetry={estimates.retry}
            loadingText={t('planning.loading.estimates')}>
            {estimates.data && aquacropPending(estimates.data) && (
              <DirectSowingOption checked={directSowing} onChange={setDirectSowing}
                fromOperations={aquacrop.response?.sowing_method === 'direct'
                  && aquacrop.response.sowing_method_source === 'field_operations'}
                notApplied={aquacrop.response?.state === 'done' && aquacrop.response.sowing_method !== 'direct'} />
            )}
            {merged && <EstimatesTable data={merged.data} progress={merged.progress} onRetry={aquacrop.retry} />}
          </Step>
          <Step title={t('planning.step.plan')}>
            {blocked ? (
              <Inline gap="inline" align="center" wrap>
                <p className="text-nkz-sm text-nkz-text-muted">{blocked}</p>
                {locked === 'cycles_unavailable' && onReload && (
                  <Button size="sm" variant="secondary" onClick={onReload}>{t('planning.retry')}</Button>
                )}
              </Inline>
            ) : (
              <PlanForm key={chosen.species} parcelId={row.parcel_id} campaign={campaign} regime={regime} management={management} main={chosen}
                coverOptions={candidates} draft={draft} onDraft={setDraft} onSaved={onSaved}
                replacing={targetCells.some((c) => c.provenance === 'planned')} />
            )}
          </Step>
        </>
      )}
      {chosen && !regime && <p className="text-nkz-sm text-nkz-warning">{t('planning.plan.needsRegime')}</p>}
    </Stack>
  );
}
