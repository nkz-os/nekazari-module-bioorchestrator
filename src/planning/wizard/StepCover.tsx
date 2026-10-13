import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, FormField, Inline, Input, Select, Stack } from '@nekazari/ui-kit';
import FortnightStrip from '../FortnightStrip';
import { formatRange, type Fortnight } from '../fortnight';
import { cropName } from '../format';
import { COVER_METHODS, NONE, isOtherCover } from '../planModel';
import type { CalendarResponse, SpeciesFit, TerminationMethod } from '../types';

const OTHER = '__other__';

interface Props {
  cal: CalendarResponse; on: boolean; species: string; sowing: Fortnight | null; method: TerminationMethod;
  coverOptions: SpeciesFit[]; onOn: (on: boolean) => void; onSpecies: (s: string) => void;
  onSowing: (f: Fortnight) => void; onMethod: (m: TerminationMethod) => void;
}

export default function StepCover({ cal, on, species, sowing, method, coverOptions, onOn, onSpecies, onSowing, onMethod }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const cover = cal.cover;
  const listed = coverOptions.map((s) => s.species);
  const other = isOtherCover(species, listed, false);
  const methodLabel = t(`planning.plan.methods.${method}`);
  return (
    <Stack gap="stack">
      <h3 className="text-nkz-lg font-semibold text-nkz-text-primary">{t('planning.wizard.cover.title')}</h3>
      {cover && !cover.fits ? <p className="text-nkz-sm text-nkz-text-muted">{t('planning.wizard.cover.noFit')}</p> : (
        <Inline gap="inline">
          <Button variant={!on ? 'primary' : 'secondary'} aria-pressed={!on} onClick={() => onOn(false)}>{t('planning.wizard.cover.no')}</Button>
          <Button variant={on ? 'primary' : 'secondary'} aria-pressed={on} onClick={() => onOn(true)}>{t('planning.wizard.cover.yes')}</Button>
        </Inline>
      )}
      {on && cover && cover.fits && (
        <Stack gap="tight">
          <FormField label={t('planning.plan.cover')}>
            <Select value={other ? OTHER : (species || NONE)}
              options={[{ value: NONE, label: '—' }, ...coverOptions.map((s) => ({ value: s.species, label: cropName(s.names, s.species, lang) })),
                { value: OTHER, label: t('planning.plan.otherSpecies') }]}
              onValueChange={(v) => onSpecies(v === NONE || v === OTHER ? '' : v)} />
          </FormField>
          {other && <FormField label={t('planning.plan.speciesName')}>
            <Input value={species} maxLength={64} onChange={(e) => onSpecies(e.target.value)} /></FormField>}
          <FormField label={t('planning.plan.method')}>
            <Select value={method} options={COVER_METHODS.map((m) => ({ value: m, label: t(`planning.plan.methods.${m}`) }))}
              onValueChange={(v) => onMethod(v as TerminationMethod)} />
          </FormField>
          {/* Only fortnights that fit before the main sowing: any other would be a cover ending before it starts. */}
          <FortnightStrip fortnights={cover.options} selected={sowing} onSelect={onSowing} label={t('planning.wizard.cover.title')}
            bar={sowing && cover.ends ? { start: sowing.start, end: cover.ends } : null}
            recommended={cover.options} />
          {sowing && cover.ends && (
            <p className="text-nkz-sm text-nkz-text-primary">{t('planning.wizard.cover.grows', {
              range: formatRange({ start: sowing.start, end: cover.ends }, lang), weeks: cover.weeks ?? 0 })}</p>
          )}
          {cover.short && <p className="text-nkz-sm text-nkz-warning">{t('planning.wizard.cover.short', { weeks: cover.weeks ?? 0 })}</p>}
          <p className="text-nkz-xs text-nkz-text-muted">{t('planning.wizard.cover.ends', { method: methodLabel })}</p>
        </Stack>
      )}
    </Stack>
  );
}
