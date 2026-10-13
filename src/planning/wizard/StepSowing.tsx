import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { FormField, Input, Stack } from '@nekazari/ui-kit';
import FortnightStrip from '../FortnightStrip';
import { formatRange, withFortnight, type Fortnight } from '../fortnight';
import { MAX_PERENNIAL_YEARS } from '../planModel';
import type { CalendarResponse } from '../types';

interface Props {
  cal: CalendarResponse; cropName: string; selected: Fortnight | null; years: number | null; perennial: boolean;
  onSowing: (f: Fortnight) => void; onYears: (y: number | null) => void;
}

export default function StepSowing({ cal, cropName, selected, years, perennial, onSowing, onYears }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const recommended = cal.main.window ? cal.main.options.filter((f) => f.start >= cal.main.window!.start && f.end <= cal.main.window!.end) : [];
  return (
    <Stack gap="stack">
      <h3 className="text-nkz-lg font-semibold text-nkz-text-primary">{t('planning.wizard.sowing.title')}</h3>
      <FortnightStrip fortnights={withFortnight(cal.main.options, selected)} recommended={recommended} selected={selected} onSelect={onSowing}
        label={t('planning.wizard.sowing.title')} />
      {selected && <p className="text-nkz-sm text-nkz-text-primary">{t('planning.wizard.sowing.chosen', { crop: cropName, range: formatRange(selected, lang) })}</p>}
      {cal.main.window
        ? <p className="text-nkz-sm text-nkz-text-muted">{t('planning.wizard.sowing.recommended', { range: formatRange(cal.main.window, lang) })}</p>
        : <p className="text-nkz-sm text-nkz-warning">{t('planning.wizard.sowing.noWindow')}</p>}
      {perennial && (
        <FormField label={t('planning.wizard.sowing.years')}>
          <Input type="number" min={1} max={MAX_PERENNIAL_YEARS} step={1} value={years ?? ''}
            onChange={(e) => onYears(e.target.value === '' ? null : Number(e.target.value))} />
        </FormField>
      )}
    </Stack>
  );
}
