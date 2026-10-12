// src/planning/PlanForm.tsx
import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, FormField, Input, Select, Stack, Switch } from '@nekazari/ui-kit';
import { PlanningApiError, putPlan } from './api';
import { cropName } from './format';
import { COVER_METHODS, NONE, emptyCover, isOtherCover, toPlanIn, validateDraft, type CycleDraft, type PlanDraft } from './planModel';
import type { Management, Regime, SpeciesFit, TerminationMethod } from './types';

/** Select sentinel for "a species that is not in the list": its name is typed in (the server validates it). */
const OTHER = '__other__';

interface Props {
  parcelId: string; campaign: number; regime: Regime | null; management: Management; main: SpeciesFit;
  coverOptions: SpeciesFit[]; draft: PlanDraft; onDraft: (d: PlanDraft) => void; onSaved: () => void;
  /** The campaign already has a planned cycle: saving replaces it. */
  replacing: boolean;
}

export default function PlanForm({ parcelId, campaign, regime, management, main, coverOptions, draft, onDraft, onSaved, replacing }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [otherCover, setOtherCover] = useState(false);
  const isPerennial = main.lifecycle === 'perennial';
  // An edit makes the last submit error stale: clear it.
  const setMain = (patch: Partial<CycleDraft>) => { setError(null); onDraft({ ...draft, main: { ...draft.main, ...patch } }); };
  const setCover = (patch: Partial<CycleDraft>) => {
    if (!draft.cover) return;
    setError(null);
    onDraft({ ...draft, cover: { ...draft.cover, ...patch } });
  };

  async function submit() {
    if (!regime) { setError(t('planning.plan.needsRegime')); return; }
    const local = validateDraft(draft, campaign, isPerennial, coverOptions.map((s) => s.species));
    if (local) { setError(t(`planning.plan.error.${local}`, { campaign })); return; }
    setSaving(true);
    setError(null);
    try {
      await putPlan(parcelId, toPlanIn(draft, campaign, regime, management, isPerennial));
      onSaved();
    } catch (e) {
      const code = e instanceof PlanningApiError && e.code ? e.code : 'generic';
      setError(t(`planning.plan.error.${code}`, { campaign, defaultValue: t('planning.plan.error.generic') }));
    } finally {
      setSaving(false);
    }
  }

  const coverSpecies = coverOptions.map((s) => ({ value: s.species, label: cropName(s.names, s.species, lang) }));
  // Free-text mode follows the data: a name the options do not list stays visible after a remount or a reload.
  const coverOther = draft.cover ? isOtherCover(draft.cover.species, coverSpecies.map((o) => o.value), otherCover) : false;
  const methods = COVER_METHODS.map((m) => ({ value: m, label: t(`planning.plan.methods.${m}`) }));

  return (
    <Stack gap="stack">
      <p className="text-nkz-sm font-medium text-nkz-text-primary">{`${t('planning.plan.crop')}: ${cropName(main.names, main.species, lang)}`}</p>
      {/* The varieties table is the one place a variety is chosen; the form only shows the choice. */}
      {(main.has_variety_trials || draft.main.variety) && (
        <p className="text-nkz-sm text-nkz-text-primary">{`${t('planning.plan.variety')}: ${draft.main.variety ?? t('planning.variety.none')}`}</p>
      )}
      <FormField label={t('planning.plan.sowing')}>
        <Input type="date" value={draft.main.sowing} onChange={(e) => setMain({ sowing: e.target.value })} />
      </FormField>
      {/* Owner decision: no harvest date is asked; a perennial is planned for a number of years. */}
      {isPerennial && (
        <FormField label={t('planning.plan.years')}>
          <Input type="number" min={1} max={10} step={1} value={draft.main.years ?? ''}
            onChange={(e) => setMain({ years: e.target.value === '' ? null : Number(e.target.value) })} />
        </FormField>
      )}
      <Switch checked={draft.cover !== null} label={t('planning.plan.addCover')} labelPosition="right"
        onChange={(on: boolean) => { setError(null); setOtherCover(false); onDraft({ ...draft, cover: on ? emptyCover() : null }); }} />
      {draft.cover && (
        <Stack gap="tight">
          <FormField label={t('planning.plan.cover')}>
            <Select value={coverOther ? OTHER : (draft.cover.species || NONE)} placeholder={t('planning.plan.cover')}
              options={[{ value: NONE, label: '—' }, ...coverSpecies, { value: OTHER, label: t('planning.plan.otherSpecies') }]}
              onValueChange={(v) => {
                setOtherCover(v === OTHER);
                setCover({ species: v === NONE || v === OTHER ? '' : v });
              }} />
          </FormField>
          {coverOther && (
            <FormField label={t('planning.plan.speciesName')}>
              <Input value={draft.cover.species} maxLength={64} onChange={(e) => setCover({ species: e.target.value })} />
            </FormField>
          )}
          <FormField label={t('planning.plan.coverSowing')}>
            <Input type="date" value={draft.cover.sowing} onChange={(e) => setCover({ sowing: e.target.value })} />
          </FormField>
          <p className="text-nkz-xs text-nkz-text-muted">{t('planning.plan.coverEndsAtSowing')}</p>
          <FormField label={t('planning.plan.method')}>
            <Select value={draft.cover.method ?? 'roller_crimper'} options={methods}
              onValueChange={(v) => setCover({ method: v as TerminationMethod })} />
          </FormField>
        </Stack>
      )}
      {error && <p role="alert" className="text-nkz-sm text-nkz-danger">{error}</p>}
      {replacing && <p className="text-nkz-xs text-nkz-warning">{t('planning.plan.replaces')}</p>}
      <div>
        <Button variant="primary" loading={saving} disabled={saving} onClick={submit}>{t('planning.plan.submit')}</Button>
      </div>
    </Stack>
  );
}
