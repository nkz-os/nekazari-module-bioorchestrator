// src/planning/PlanForm.tsx
import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, FormField, Input, Select, Stack, Switch } from '@nekazari/ui-kit';
import { PlanningApiError, putPlan } from './api';
import { cropName } from './format';
import { COVER_METHODS, NONE, emptyCover, toPlanIn, validateDraft, type CycleDraft, type PlanDraft } from './planModel';
import type { Management, Regime, SpeciesFit, TerminationMethod, VarietyStat } from './types';

/** Select sentinel for "a species that is not in the list": its name is typed in (the server validates it). */
const OTHER = '__other__';

interface Props {
  parcelId: string; campaign: number; regime: Regime | null; management: Management; main: SpeciesFit;
  coverOptions: SpeciesFit[]; varieties: VarietyStat[]; draft: PlanDraft; onDraft: (d: PlanDraft) => void; onSaved: () => void;
}

export default function PlanForm({ parcelId, campaign, regime, management, main, coverOptions, varieties, draft, onDraft, onSaved }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [otherCover, setOtherCover] = useState(false);
  const setMain = (patch: Partial<CycleDraft>) => onDraft({ ...draft, main: { ...draft.main, ...patch } });
  const setCover = (patch: Partial<CycleDraft>) => draft.cover && onDraft({ ...draft, cover: { ...draft.cover, ...patch } });

  async function submit() {
    if (!regime) { setError(t('planning.plan.needsRegime')); return; }
    const local = validateDraft(draft, campaign, main.lifecycle === 'perennial');
    if (local) { setError(t(`planning.plan.error.${local}`, { campaign })); return; }
    setSaving(true);
    setError(null);
    try {
      await putPlan(parcelId, toPlanIn(draft, campaign, regime, management));
      onSaved();
    } catch (e) {
      const code = e instanceof PlanningApiError && e.code ? e.code : 'generic';
      setError(t(`planning.plan.error.${code}`, { campaign, defaultValue: t('planning.plan.error.generic') }));
    } finally {
      setSaving(false);
    }
  }

  const varietyOptions = [{ value: NONE, label: t('planning.variety.none') }, ...varieties.map((v) => ({ value: v.variety, label: v.variety }))];
  const coverSpecies = coverOptions.map((s) => ({ value: s.species, label: cropName(s.names, s.species, lang) }));
  const methods = COVER_METHODS.map((m) => ({ value: m, label: t(`planning.plan.methods.${m}`) }));

  return (
    <Stack gap="stack">
      <p className="text-nkz-sm font-medium text-nkz-text-primary">{`${t('planning.plan.crop')}: ${cropName(main.names, main.species, lang)}`}</p>
      {varieties.length > 0 && (
        <FormField label={t('planning.plan.variety')}>
          <Select value={draft.main.variety ?? NONE} options={varietyOptions}
            onValueChange={(v) => setMain({ variety: v === NONE ? null : v })} />
        </FormField>
      )}
      <FormField label={t('planning.plan.sowing')}>
        <Input type="date" value={draft.main.sowing} onChange={(e) => setMain({ sowing: e.target.value })} />
      </FormField>
      <FormField label={t('planning.plan.harvest')}>
        <Input type="date" value={draft.main.end} onChange={(e) => setMain({ end: e.target.value })} />
      </FormField>
      <Switch checked={draft.cover !== null} label={t('planning.plan.addCover')} labelPosition="right"
        onChange={(on: boolean) => { setOtherCover(false); onDraft({ ...draft, cover: on ? emptyCover() : null }); }} />
      {draft.cover && (
        <Stack gap="tight">
          <FormField label={t('planning.plan.cover')}>
            <Select value={otherCover ? OTHER : (draft.cover.species || NONE)} placeholder={t('planning.plan.cover')}
              options={[{ value: NONE, label: '—' }, ...coverSpecies, { value: OTHER, label: t('planning.plan.otherSpecies') }]}
              onValueChange={(v) => {
                setOtherCover(v === OTHER);
                setCover({ species: v === NONE || v === OTHER ? '' : v });
              }} />
          </FormField>
          {otherCover && (
            <FormField label={t('planning.plan.speciesName')}>
              <Input value={draft.cover.species} maxLength={64} onChange={(e) => setCover({ species: e.target.value })} />
            </FormField>
          )}
          <FormField label={t('planning.plan.coverSowing')}>
            <Input type="date" value={draft.cover.sowing} onChange={(e) => setCover({ sowing: e.target.value })} />
          </FormField>
          <FormField label={t('planning.plan.coverEnd')}>
            <Input type="date" value={draft.cover.end} onChange={(e) => setCover({ end: e.target.value })} />
          </FormField>
          <FormField label={t('planning.plan.method')}>
            <Select value={draft.cover.method ?? 'roller_crimper'} options={methods}
              onValueChange={(v) => setCover({ method: v as TerminationMethod })} />
          </FormField>
        </Stack>
      )}
      {error && <p role="alert" className="text-nkz-sm text-nkz-danger">{error}</p>}
      <div>
        <Button variant="primary" loading={saving} disabled={saving} onClick={submit}>{t('planning.plan.submit')}</Button>
      </div>
    </Stack>
  );
}
