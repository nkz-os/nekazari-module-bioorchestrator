// src/planning/CampaignSummary.tsx
import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Stack } from '@nekazari/ui-kit';
import { cropName, formatHa } from './format';
import type { FarmResponse } from './types';

/** One bar per crop (widest = 100%), undecided parcels in the danger colour, rotation warnings as text. */
export default function CampaignSummary({ data }: { data: FarmResponse }) {
  const { t, i18n } = useTranslation('bioorchestrator');
  const lang = i18n.language;
  const { area_by_crop: crops, undecided, rotation_warnings: rotation } = data.summary;
  const widest = Math.max(0, ...crops.map((c) => c.area_ha));
  return (
    <Stack gap="inline">
      <p className="text-nkz-sm text-nkz-text-primary">{t('planning.summary.title', { campaign: data.campaign })}</p>
      {crops.map((c) => (
        <div key={c.species} className="text-nkz-xs text-nkz-text-muted">
          <div>{`${cropName(c.names, c.species, lang)} · ${t('planning.summary.ha', { ha: formatHa(c.area_ha, lang) })}`}</div>
          <div className="bg-nkz-accent-base h-2 rounded" style={{ width: `${widest > 0 ? (c.area_ha / widest) * 100 : 0}%` }} />
        </div>
      ))}
      {undecided > 0 && <p className="text-nkz-sm text-nkz-danger">{t('planning.summary.undecided', { count: undecided })}</p>}
      {rotation > 0 && <p className="text-nkz-sm text-nkz-text-muted">{t('planning.summary.rotation', { count: rotation })}</p>}
    </Stack>
  );
}
