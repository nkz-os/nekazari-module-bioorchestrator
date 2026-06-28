import React from 'react';
import { useTranslation } from '@nekazari/sdk';

export interface EconomicInputs {
  seedPrice: number;
  harvestPrice: number;
  priceUnit: 'eur_per_kg' | 'eur_per_t';
  operationCost: number;
}

interface Props {
  value: EconomicInputs;
  onChange: (v: EconomicInputs) => void;
}

const EconomicInputsPanel: React.FC<Props> = ({ value, onChange }) => {
  const { t } = useTranslation('bioorchestrator');

  return (
    <div className="flex flex-wrap gap-3 items-end">
      <div className="w-[130px]">
        <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
          {t('comparator.seedPrice', { defaultValue: 'Seed cost €/ha' })}
        </label>
        <input
          type="number"
          min="0"
          step="1"
          value={value.seedPrice}
          onChange={e => onChange({ ...value, seedPrice: Number(e.target.value) || 0 })}
          className="w-full rounded-md border border-nkz-border bg-nkz-surface px-2 py-1.5 text-nkz-sm"
        />
      </div>

      <div className="w-[130px]">
        <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
          {t('planning.pricePerKg', { defaultValue: '€/kg' })} / {t('planning.pricePerT', { defaultValue: '€/t' })}
        </label>
        <div className="flex gap-1">
          <input
            type="number"
            min="0"
            step="0.01"
            value={value.harvestPrice}
            onChange={e => onChange({ ...value, harvestPrice: Number(e.target.value) || 0 })}
            className="flex-1 rounded-md border border-nkz-border bg-nkz-surface px-2 py-1.5 text-nkz-sm"
          />
          <select
            value={value.priceUnit}
            onChange={e => onChange({ ...value, priceUnit: e.target.value as 'eur_per_kg' | 'eur_per_t' })}
            className="rounded-md border border-nkz-border bg-nkz-surface px-1.5 py-1.5 text-nkz-xs"
          >
            <option value="eur_per_t">€/t</option>
            <option value="eur_per_kg">€/kg</option>
          </select>
        </div>
      </div>

      <div className="w-[130px]">
        <label className="block text-nkz-xs font-medium text-nkz-text-secondary mb-1">
          {t('comparator.operationCost', { defaultValue: 'Cost/op €' })}
        </label>
        <input
          type="number"
          min="0"
          step="1"
          value={value.operationCost}
          onChange={e => onChange({ ...value, operationCost: Number(e.target.value) || 0 })}
          className="w-full rounded-md border border-nkz-border bg-nkz-surface px-2 py-1.5 text-nkz-sm"
        />
      </div>
    </div>
  );
};

export default EconomicInputsPanel;
