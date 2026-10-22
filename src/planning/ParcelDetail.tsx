// src/planning/ParcelDetail.tsx
import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Badge, Button, FormField, Inline, Select, Stack } from '@nekazari/ui-kit';
import { ArrowLeft } from 'lucide-react';
import { fetchFit } from './api';
import { cropName, safeLang, shortId } from './format';
import { lockMessage, lockReason, previousMain, simulationAvailable } from './matrixModel';
import { CycleLine } from './PlanMatrix';
import type { Management, ParcelRow, Regime } from './types';
import { useLoad } from './useLoad';
import PlanWizard from './wizard/PlanWizard';

interface Props {
  row: ParcelRow; campaign: number; onClose: () => void; onSaved: () => void; onSimulate?: () => void; onForecast?: () => void;
  /** Reloads the farm data: offered next to a lock that a failed history read causes. */
  onReload?: () => void;
}

export default function ParcelDetail({ row, campaign, onClose, onSaved, onSimulate, onForecast, onReload }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const [regime, setRegime] = useState<Regime | null>(row.regime);
  const [management, setManagement] = useState<Management>('conventional');
  // The two settings show as one line; "Cambiar" opens them, and a parcel without a regime opens them at once.
  const [editing, setEditing] = useState(row.regime === null);
  const targetCells = row.cells[String(campaign)] ?? [];

  const fit = useLoad((s) => fetchFit(row.parcel_id, campaign, regime, s), [row.parcel_id, campaign, regime]);
  const prev = previousMain(row, campaign);
  // A deep link can open a parcel the matrix would not let you plan; the evidence stays, the wizard does not.
  const locked = lockReason(row, campaign);
  const blocked = locked ? lockMessage(locked, campaign, t)
    : campaign < new Date().getFullYear() ? t('planning.plan.error.campaign_past', { campaign }) : null;
  const facts = fit.data?.facts;
  const soilTexture = facts?.soil_texture
    ? t(`planning.texture.${facts.soil_texture}`, { defaultValue: facts.soil_texture })
    : t('planning.detail.noData');
  const regimeOptions = (['rainfed', 'irrigated'] as const).map((r) => ({ value: r, label: t(`planning.regime.${r}`) }));
  const managementOptions = (['conventional', 'organic'] as const).map((m) => ({ value: m, label: t(`planning.management.${m}`) }));

  return (
    <Stack gap="section">
      <Inline gap="inline" align="center" wrap>
        <Button variant="ghost" onClick={onClose} leadingIcon={<ArrowLeft className="w-4 h-4" />}>{t('planning.detail.back')}</Button>
        <h2 className="text-nkz-lg font-semibold text-nkz-text-primary">{`${row.name ?? shortId(row.parcel_id)} · ${campaign}`}</h2>
        {prev && <span className="text-nkz-sm text-nkz-text-muted">{t('planning.detail.previous', { crop: cropName(prev.names, prev.species_raw, lang), campaign: campaign - 1 })}</span>}
        {onSimulate && simulationAvailable(row, campaign) && (
          <Button size="sm" variant="secondary" onClick={onSimulate}>{t('planning.simulation.open')}</Button>
        )}
        {onForecast && simulationAvailable(row, campaign) && (
          <Button size="sm" variant="ghost" onClick={onForecast}>{t('planning.simulation.forecast')}</Button>
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

      {editing ? (
        <Inline gap="inline" wrap align="end">
          <FormField label={t('planning.detail.regimeQuestion')} description={regime ? undefined : t('planning.detail.regimeHint')}>
            <Select value={regime ?? ''} placeholder={t('planning.detail.regimeQuestion')} options={regimeOptions}
              onValueChange={(v) => setRegime(v as Regime)} error={!regime} />
          </FormField>
          <FormField label={t('planning.detail.managementLabel')}>
            <Select value={management} options={managementOptions} onValueChange={(v) => setManagement(v as Management)} />
          </FormField>
        </Inline>
      ) : (
        <Inline gap="inline" align="center" wrap>
          <span className="text-nkz-sm text-nkz-text-primary">
            {`${t(`planning.regime.${regime ?? 'unknown'}`)} · ${t(`planning.management.${management}`)}`}
          </span>
          <Button size="sm" variant="ghost" onClick={() => setEditing(true)}>{t('planning.wizard.change')}</Button>
        </Inline>
      )}

      {blocked ? (
        <Inline gap="inline" align="center" wrap>
          <p className="text-nkz-sm text-nkz-text-muted">{blocked}</p>
          {locked === 'cycles_unavailable' && onReload && (
            <Button size="sm" variant="secondary" onClick={onReload}>{t('planning.retry')}</Button>
          )}
        </Inline>
      ) : regime && (
        // A new regime or management keeps the choices; a variety the new evidence lacks is dropped by the wizard.
        <PlanWizard row={row} campaign={campaign} regime={regime} management={management}
          onSaved={onSaved} fit={fit} />
      )}
    </Stack>
  );
}
