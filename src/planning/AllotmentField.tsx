import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, FormField, Input, Inline, Select, Stack } from '@nekazari/ui-kit';
import { num } from './regionalYield';
import { MAX_ALLOTMENT_MM, mmToUnit, parseAllotment, type AllotmentUnit } from './irrigationNeed';

interface Props {
  /** Stored allotment in mm, or null when none is declared. */
  stored: number | null;
  /** Persists the value (mm, or null to clear); rejects when the save fails. */
  onSave: (mm: number | null) => Promise<void>;
}

/** «Dotación de la campaña»: entered in m³/ha or mm, kept in mm; what is stored is always shown. */
export default function AllotmentField({ stored, onSave }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const base = 'planning.detail.allotment';
  const [unit, setUnit] = useState<AllotmentUnit>('m3ha');
  const [text, setText] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const run = async (mm: number | null) => {
    setSaving(true);
    setError(null);
    try {
      await onSave(mm);
      setText('');
    } catch {
      // The typed value stays in the field so it is not lost.
      setError(t(`${base}.saveError`));
    } finally {
      setSaving(false);
    }
  };
  const submit = () => {
    const parsed = parseAllotment(text, unit);
    if (!parsed.ok) {
      setError(parsed.error === 'tooLarge' ? t(`${base}.tooLarge`, { max: num(mmToUnit(MAX_ALLOTMENT_MM, unit), lang, 0, false), unit: t(`${base}.unit.${unit}`) }) : t(`${base}.invalid`));
      return;
    }
    void run(parsed.mm);
  };
  const unitOptions = (['m3ha', 'mm'] as const).map((u) => ({ value: u, label: t(`${base}.unit.${u}`) }));

  return (
    <Stack gap="tight">
      <Inline gap="inline" wrap align="end">
        <FormField label={t(`${base}.label`)} description={t(`${base}.hint`)} error={error ?? undefined}>
          <Input value={text} inputMode="decimal" aria-label={t(`${base}.label`)} placeholder={t(`${base}.placeholder`)}
            onChange={(e) => { setText(e.target.value); setError(null); }} />
        </FormField>
        <Select value={unit} options={unitOptions} aria-label={t(`${base}.unitLabel`)}
          onValueChange={(v) => { setUnit(v as AllotmentUnit); setError(null); }} />
        <Button size="sm" variant="secondary" disabled={saving || text.trim() === ''} onClick={submit}>{t(`${base}.save`)}</Button>
      </Inline>
      <Inline gap="inline" wrap align="center">
        <span className="text-nkz-sm text-nkz-text-primary">
          {stored == null ? t(`${base}.none`)
            : t(`${base}.stored`, { mm: num(stored, lang, 0, false), m3: num(mmToUnit(stored, 'm3ha'), lang, 0, false) })}
        </span>
        {stored != null && (
          <Button size="sm" variant="ghost" disabled={saving} onClick={() => void run(null)}>{t(`${base}.clear`)}</Button>
        )}
      </Inline>
    </Stack>
  );
}
