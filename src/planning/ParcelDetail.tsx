// src/planning/ParcelDetail.tsx
import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, FormField, Inline, Select, Stack } from '@nekazari/ui-kit';
import { ArrowLeft } from 'lucide-react';
import { fetchEstimates, fetchFit, fetchVarieties } from './api';
import EstimatesTable from './EstimatesTable';
import FitList from './FitList';
import { cropName, shortId } from './format';
import { lockReason, previousMain } from './matrixModel';
import PlanForm from './PlanForm';
import { draftFor, type PlanDraft } from './planModel';
import Step from './Step';
import type { Management, ParcelRow, Regime } from './types';
import { useLoad } from './useLoad';
import VarietyTable from './VarietyTable';

interface Props { row: ParcelRow; campaign: number; onClose: () => void; onSaved: () => void }
const EMPTY_DRAFT: PlanDraft = { main: { species: '', variety: null, sowing: '', end: '', method: null }, cover: null };

export default function ParcelDetail({ row, campaign, onClose, onSaved }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const [regime, setRegime] = useState<Regime | null>(row.regime);
  const [management, setManagement] = useState<Management>('conventional');
  const [species, setSpecies] = useState<string | null>(null);
  const [draft, setDraft] = useState<PlanDraft>(EMPTY_DRAFT);

  const fit = useLoad((s) => fetchFit(row.parcel_id, campaign, regime, s), [row.parcel_id, campaign, regime]);
  const chosen = fit.data?.species.find((x) => x.species === species) ?? null;
  const varieties = useLoad(
    (s) => fetchVarieties(row.parcel_id, chosen!.species, regime!, management, s),
    [row.parcel_id, chosen?.species, regime, management], Boolean(chosen?.has_variety_trials && regime));
  const estimates = useLoad(
    (s) => fetchEstimates(row.parcel_id, { crop: chosen!.species, variety: draft.main.variety, regime: regime!, management,
      sowingDate: draft.main.sowing || null }, s),
    [row.parcel_id, chosen?.species, draft.main.variety, regime, management, draft.main.sowing], Boolean(chosen && regime));

  const pick = (slug: string) => {
    const sp = fit.data?.species.find((x) => x.species === slug);
    setSpecies(slug);
    if (sp) setDraft(draftFor(sp, campaign));
  };
  const prev = previousMain(row, campaign);
  // A deep link can open a parcel the matrix would not let you plan; the evidence stays, the form does not.
  const locked = lockReason(row, campaign);
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
      </Inline>

      <Inline gap="inline" wrap align="center">
        <span className="text-nkz-sm text-nkz-text-muted">{t('planning.detail.known')}:</span>
        <Badge intent="default">{`${t('planning.detail.climate')}: ${facts?.climate_class ?? t('planning.detail.noData')}`}</Badge>
        <Badge intent="default">{`${t('planning.detail.soil')}: ${soilTexture}${facts?.soil_ph != null ? `, pH ${facts.soil_ph}` : ''}`}</Badge>
        {facts && !facts.history_available && <Badge intent="warning">{t('planning.detail.historyDown')}</Badge>}
      </Inline>

      <Inline gap="inline" wrap align="end">
        <FormField label={t('planning.detail.regimeQuestion')} description={regime ? undefined : t('planning.detail.regimeHint')}>
          <Select value={regime ?? ''} placeholder={t('planning.detail.regimeQuestion')} options={regimeOptions}
            onValueChange={(v) => setRegime(v as Regime)} error={!regime} />
        </FormField>
        <FormField label={t('planning.detail.managementLabel')}>
          <Select value={management} options={managementOptions} onValueChange={(v) => setManagement(v as Management)} />
        </FormField>
      </Inline>

      <Step title={t('planning.step.crop')} loading={fit.loading} error={fit.error} onRetry={fit.retry}>
        {fit.data && <FitList fit={fit.data} selected={species} onSelect={pick} />}
      </Step>

      {!chosen && fit.data && <p className="text-nkz-sm text-nkz-text-muted">{t('planning.fit.pick')}</p>}

      {chosen && regime && (
        <>
          <Step title={t('planning.step.variety')} loading={varieties.loading} error={varieties.error} onRetry={varieties.retry}>
            {chosen.has_variety_trials
              ? varieties.data && <VarietyTable data={varieties.data} selected={draft.main.variety}
                  onSelect={(v) => setDraft({ ...draft, main: { ...draft.main, variety: v } })} />
              : <p className="text-nkz-sm text-nkz-text-muted">{t('planning.variety.noTrials')}</p>}
          </Step>
          <Step title={t('planning.step.estimate')} loading={estimates.loading} error={estimates.error} onRetry={estimates.retry}>
            {estimates.data && <EstimatesTable data={estimates.data} />}
          </Step>
          <Step title={t('planning.step.plan')}>
            {locked ? (
              <p className="text-nkz-sm text-nkz-text-muted">
                {locked === 'cycles_unavailable' ? t('planning.warning.cycles_unavailable') : t(`planning.plan.error.${locked}`, { campaign })}
              </p>
            ) : (
              <PlanForm parcelId={row.parcel_id} campaign={campaign} regime={regime} management={management} main={chosen}
                coverOptions={candidates} varieties={varieties.data?.varieties ?? []} draft={draft} onDraft={setDraft} onSaved={onSaved} />
            )}
          </Step>
        </>
      )}
      {chosen && !regime && <p className="text-nkz-sm text-nkz-warning">{t('planning.plan.needsRegime')}</p>}
    </Stack>
  );
}
