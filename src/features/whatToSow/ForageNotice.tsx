import React from 'react';
import { useTranslation } from '@nekazari/sdk';
import { Button, Stack } from '@nekazari/ui-kit';
import type { Recommendation } from '../../types/recommend';
import { forageNoticeCount } from './viewModel';

interface ForageNoticeProps {
  rec: Recommendation;
  /** Switches the page to forage mode. */
  onViewForage: () => void;
  className?: string;
}

/** Harvest mode: the crop also has forage trials nearby that are not in the numbers above. */
export default function ForageNotice({ rec, onViewForage, className }: ForageNoticeProps) {
  const { t } = useTranslation('bioorchestrator');
  const count = forageNoticeCount(rec);
  if (count == null) return null;
  return (
    <Stack gap="tight" className={className}>
      <p className="text-nkz-sm text-nkz-info">{t('whatToSow.card.forageNotice', { count })}</p>
      <div>
        <Button variant="ghost" size="sm" onClick={onViewForage}>{t('whatToSow.card.viewAsForage')}</Button>
      </div>
    </Stack>
  );
}
