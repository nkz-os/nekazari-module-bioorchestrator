import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Select } from '@nekazari/ui-kit';
import { KOPPEN_CODES } from './pageModel';

interface KoppenPickerProps {
  value: string | null;
  onChange: (code: string) => void;
}

/** Köppen class selector with plain-language labels. Has no empty option: a class can be changed, never cleared. */
export default function KoppenPicker({ value, onChange }: KoppenPickerProps) {
  const { t } = useTranslation('bioorchestrator');
  const options = KOPPEN_CODES.map((code) => ({
    value: code,
    label: `${code} · ${t(`whatToSow.koppen.${code}`, { defaultValue: code })}`,
  }));
  return (
    <Select
      value={value ?? ''}
      onValueChange={(code: string) => {
        if (code) onChange(code);
      }}
      options={options}
      placeholder={t('whatToSow.koppenPicker.placeholder')}
    />
  );
}
