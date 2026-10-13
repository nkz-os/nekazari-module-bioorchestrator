import React, { useState } from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Stack } from '@nekazari/ui-kit';
import { formatT } from '../format';

interface Props {
  lines: string[]; yieldLine: { low: number; high: number } | null; detail: React.ReactNode;
  replacing: boolean; saving: boolean; error: string | null; regimeNotice: boolean; onSave: () => void;
  /** After a regime notice the plan is already saved: this leaves the wizard. */
  onContinue?: () => void;
}

export default function WizardSummary({ lines, yieldLine, detail, replacing, saving, error, regimeNotice, onSave, onContinue }: Props) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const [open, setOpen] = useState(false);
  return (
    <Stack gap="stack">
      <h3 className="text-nkz-lg font-semibold text-nkz-text-primary">{t('planning.wizard.summary.title')}</h3>
      {lines.map((l) => <p key={l} className="text-nkz-sm text-nkz-text-primary">{l}</p>)}
      {yieldLine && <p className="text-nkz-sm text-nkz-text-primary">{t('planning.wizard.summary.yield', {
        low: formatT(yieldLine.low, i18n.language), high: formatT(yieldLine.high, i18n.language) })}</p>}
      {detail && <Button variant="ghost" onClick={() => setOpen((o) => !o)}>{t('planning.wizard.summary.detail')}</Button>}
      {open && detail}
      {replacing && <p className="text-nkz-xs text-nkz-warning">{t('planning.plan.replaces')}</p>}
      {error && <p role="alert" className="text-nkz-sm text-nkz-danger">{error}</p>}
      {regimeNotice && <p className="text-nkz-sm text-nkz-warning">{t('planning.wizard.summary.regimeNotSaved')}</p>}
      <div>{regimeNotice && onContinue
        ? <Button variant="primary" onClick={onContinue}>{t('planning.wizard.summary.continue')}</Button>
        : <Button variant="primary" loading={saving} disabled={saving} onClick={onSave}>{t('planning.wizard.save')}</Button>}</div>
    </Stack>
  );
}
