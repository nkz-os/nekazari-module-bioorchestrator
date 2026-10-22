// src/planning/DirectSowingOption.tsx
import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Checkbox, Stack } from '@nekazari/ui-kit';

interface Props {
  checked: boolean; onChange: (checked: boolean) => void; fromOperations: boolean;
  /** Ticked, but the finished simulation does not say it ran with direct sowing. */
  notApplied: boolean;
}

/** AquaCrop's direct-sowing switch: the stubble simulated as a mulch (fixed, validated values). */
export default function DirectSowingOption({ checked, onChange, fromOperations, notApplied }: Props) {
  const { t } = useTranslation('bioorchestrator');
  return (
    <Stack gap="tight">
      <Checkbox id="planning-direct-sowing" checked={checked} onChange={onChange} label={t('planning.estimate.directSowing')} />
      <p className="text-nkz-sm text-nkz-text-muted">{t('planning.estimate.directSowingHint')}</p>
      {!checked && fromOperations && (
        <p className="text-nkz-sm text-nkz-text-muted">{t('planning.estimate.directSowingFromOperations')}</p>
      )}
      {checked && notApplied && (
        <p className="text-nkz-sm text-nkz-warning">{t('planning.estimate.directSowingNotApplied')}</p>
      )}
    </Stack>
  );
}
